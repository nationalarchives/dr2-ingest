package uk.gov.nationalarchives.ingestflowcontrol

import cats.effect.*
import cats.syntax.all.*
import io.circe.{Decoder, HCursor}
import io.circe.generic.auto.*
import pureconfig.ConfigReader
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbWriteItemRequest
import uk.gov.nationalarchives.DASFNClient.Status.Running
import uk.gov.nationalarchives.ingestflowcontrol.Lambda.*
import uk.gov.nationalarchives.utils.{Generators, LambdaRunner}
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient, DASSMClient}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{*, given}

import java.time.Instant
import scala.annotation.tailrec
import uk.gov.nationalarchives.DADynamoDBClient.given
import org.scanamo.syntax.*
import software.amazon.awssdk.services.sfn.model.TaskTimedOutException

class Lambda extends LambdaRunner[Option[Input], TaskOutput, Config, Dependencies] {

  override def handler: (Option[Input], Config, Dependencies) => IO[TaskOutput] = (potentialInput, config, dependencies) => {

    val executionStarter = potentialInput.map(_.executionName).getOrElse("NO_EXECUTION_NAME")

    /** A recursive method to send task success to one and only one task per invocation. It takes a list of source systems, iterates over the list (recursively) to start a task on
      * first system which has a reserved channel available
      * @param sourceSystems
      *   List of source systems to iterate over
      * @param executionsBySystem
      *   Map of SystemName -> count of running executions
      * @param flowControlConfig
      *   Flow control configuration
      * @param taskExecutorName
      *   Name of the task executor which sent the success
      * @return
      *   IO[String]: name of the task executor which sent success, a hard coded string "CONTINUE_TO_NEXT_SYSTEM" to indicate that no task was sent for the tried system or an empty
      *   string to indicate that there is nothing more to do
      */
    def startTaskOnReservedChannel(
        sourceSystems: List[SourceSystem],
        executionsBySystem: Map[String, Int],
        flowControlConfig: FlowControlConfig,
        taskExecutorName: String
    ): IO[String] = {
      if sourceSystems.isEmpty then
        logInfo("Reserved channel: No more source systems to iterate over", taskExecutorName) >>
          IO.pure(taskExecutorName)
      else
        val currentSystemName = sourceSystems.head.systemName
        val reservedChannels: Int = flowControlConfig.sourceSystems.find(_.systemName == currentSystemName).map(_.reservedChannels).getOrElse(0)
        val currentExecutionCount = executionsBySystem.getOrElse(currentSystemName, 0)
        if currentExecutionCount >= reservedChannels then
          logInfo(
            s"Reserved channel: Execution count of $currentExecutionCount exceeds reserved channel limit of $reservedChannels, continuing with remaining systems",
            executionStarter,
            currentSystemName,
            remainingSystems = sourceSystems.tail.map(_.systemName).mkString(",")
          ) >>
            startTaskOnReservedChannel(sourceSystems.tail, executionsBySystem, flowControlConfig, taskExecutorName)
        else
          logInfo("Attempting progress on reserved channel for current system", executionStarter, currentSystemName) >>
            dependencies.dynamoClient
              .queryItems[IngestQueueTableItem](config.flowControlQueueTableName, "sourceSystem" === currentSystemName)
              .flatMap { queueTableTasks =>
                if queueTableTasks.nonEmpty then
                  logInfo("Initiate processing for current system", executionStarter, currentSystemName) >>
                    sendTaskSuccessThenDelete(currentSystemName, queueTableTasks).flatMap { taskOutput =>
                      if taskOutput == continueProcessingNextSystem then startTaskOnReservedChannel(sourceSystems.tail, executionsBySystem, flowControlConfig, taskOutput)
                      else
                        logInfo("Reserved channel: Task success sent by an execution, terminating further computation", taskOutput) >>
                          IO.pure(taskOutput)
                    }
                else
                  logInfo(
                    "Reserved channel: No tasks in queue for current system, continuing with remaining systems",
                    executionStarter,
                    currentSystemName,
                    sourceSystems.tail.map(_.systemName).mkString(", ")
                  ) >>
                    startTaskOnReservedChannel(sourceSystems.tail, executionsBySystem, flowControlConfig, taskExecutorName)
              }
    }

    /** A recursive method to send success to one of the systems, based on probability as configured in the Flow control configuration. It builds a probability ranges map based on
      * the config (e.g. TDR -> (1, 25), FCL -> (25, 40) ... ). It then generates a random number between the minimum and maximum value over all probabilities and tries to schedule
      * a task for that system. If there is no task waiting for the system, it recreates the probability map excluding that system from the config and generates a random number for
      * the remaining systems only, thus making sure that the probabilities are honoured over all iterations.
      *
      * The probability of each system is kept intact in relation to each other, even if one of the systems does not have a waiting task. e.g. if there are 3 systems, one, two, and
      * three with probabilities of 25, 55, 20, the ranges being 1-26, 26-81, 81-101. For this example, let's assume that system "two" does not have a waiting task. Iteration 1 -
      * random number generated is 30 (which corresponds to system "two"), since system "two" does not have a waiting task, Iteration 2 - a new ranges map is constructed by
      * excluding system "two" (1-26, 26-47) and a new number is generated between 1 and 47 this ensures that the probability in relation to each other is kept intact for the
      * remaining systems
      * @param sourceSystems
      *   List of source systems to iterate for starting a task
      * @return
      *   IO[String]: name of the task executor which sent success, a hard coded string "CONTINUE_TO_NEXT_SYSTEM" to indicate that no task was sent for the tried system
      */
    def startTaskBasedOnProbability(sourceSystems: List[SourceSystem]): IO[String] = {
      sourceSystems match
        case Nil =>
          logInfo("Iterated over all source systems for probability, none of them have a waiting task, terminating lambda", executionStarter) >>
            IO.pure(executionStarter)
        case _ =>
          val sourceSystemProbabilities = buildProbabilityRangesMap(sourceSystems, 1, Map.empty[String, Range])
          if sourceSystemProbabilities.isEmpty then
            logInfo(
              "All remaining source systems have zero probability, terminating lambda",
              executionStarter,
              remainingSystems = sourceSystems.map(_.systemName).mkString(", ")
            ) >>
              IO.pure(executionStarter)
          else
            val maxRandomValue: Int = sourceSystemProbabilities.values.map(_.endExclusive).max
            val luckyDip = dependencies.randomInt(1, maxRandomValue)
            val sourceSystemEntry = sourceSystemProbabilities.find((_, probRange) => luckyDip >= probRange.startInclusive && luckyDip < probRange.endExclusive).get
            val systemToStartTaskOn = sourceSystemEntry._1
            val systemProbabilitiesStr = sourceSystemProbabilities.map { case (system, range) => s"$system -> (${range.startInclusive}, ${range.endExclusive})" }.mkString(", ")
            logInfo(
              s"Selected source system based on probability using lucky dip: $luckyDip in ranges $systemProbabilitiesStr, querying items from queue table",
              executionStarter,
              systemToStartTaskOn
            ) >>
              dependencies.dynamoClient
                .queryItems[IngestQueueTableItem](config.flowControlQueueTableName, "sourceSystem" === systemToStartTaskOn)
                .flatMap { queueTableTasks =>
                  lazy val remainingSystems = sourceSystems.filter(_.systemName != systemToStartTaskOn)
                  if queueTableTasks.nonEmpty then
                    logInfo("Initiate processing using probability approach", executionStarter, sourceSystems.head.systemName) >>
                      sendTaskSuccessThenDelete(systemToStartTaskOn, queueTableTasks).flatMap { successExecutorName =>
                        if successExecutorName == continueProcessingNextSystem then startTaskBasedOnProbability(remainingSystems)
                        else IO.pure(successExecutorName)
                      }
                  else
                    logInfo(
                      "Probability: No tasks in queue for current system, continuing with remaining systems",
                      executionStarter,
                      systemToStartTaskOn,
                      remainingSystems.map(_.systemName).mkString(", ")
                    ) >>
                      startTaskBasedOnProbability(remainingSystems)
                }
    }

    def writeTaskToQueueTable(flowControlConfig: FlowControlConfig): IO[Unit] = IO.whenA(potentialInput.nonEmpty) {
      val input = potentialInput.get
      input.taskToken match
        case null | "" =>
          logInfo("Task token is empty, skipping writing task to ingest queue table", input.executionName)
        case _ =>
          val inputSystemName = input.executionName.split("_").head
          val supportedSystemName = flowControlConfig.sourceSystems.find(_.systemName == inputSystemName).map(_.systemName).getOrElse(default)
          val queuedTimeAndExecutionName = Instant.now.toString + "_" + input.executionName
          logInfo("Writing task to ingest queue table", input.executionName, supportedSystemName, queuedTimeAndExecution = queuedTimeAndExecutionName) >>
            dependencies.dynamoClient
              .writeItem(
                DADynamoDbWriteItemRequest(
                  config.flowControlQueueTableName,
                  Map(
                    sourceSystem -> AttributeValue.builder.s(supportedSystemName).build(),
                    queuedAt -> AttributeValue.builder.s(queuedTimeAndExecutionName).build(),
                    taskToken -> AttributeValue.builder.s(input.taskToken).build(),
                    executionName -> AttributeValue.builder.s(input.executionName).build()
                  )
                )
              )
              .void
    }

    def deleteTask(systemName: String, task: IngestQueueTableItem) = {
      logInfo("Deleting task from ingest queue table", executionStarter, systemName, queuedTimeAndExecution = task.queuedTimeAndExecutionName) >>
        dependencies.dynamoClient
          .deleteItems(
            config.flowControlQueueTableName,
            List(IngestQueuePrimaryKey(IngestQueuePartitionKey(systemName), IngestQueueSortKey(task.queuedTimeAndExecutionName)))
          )
    }

    def sendTaskSuccessThenDelete(systemName: String, tasks: List[IngestQueueTableItem]): IO[String] =
      tasks match
        case Nil =>
          logInfo("sendTaskSuccessThenDelete: No more tasks to process for current system", executionStarter, systemName) >>
            IO.pure(continueProcessingNextSystem)
        case _ =>
          val task = tasks.head
          logInfo(
            "Sending success for the task",
            task.executionName,
            systemName,
            queuedTimeAndExecution = task.queuedTimeAndExecutionName,
            resumedExecution = executionStarter
          ) >>
            dependencies.stepFunctionClient
              .sendTaskSuccess(task.taskToken, Option(TaskOutput(executionStarter, task.executionName)))
              .flatMap { _ =>
                logInfo(
                  "Task sent successfully, deleting task from table",
                  task.executionName,
                  systemName,
                  queuedTimeAndExecution = task.queuedTimeAndExecutionName,
                  resumedExecution = executionStarter
                ) >>
                  deleteTask(systemName, task) >> IO.pure(executionStarter)
              }
              .handleErrorWith {
                case timeoutException: TaskTimedOutException =>
                  logInfo(
                    "Encountered TaskTimedOutException, deleting task from table",
                    executionStarter,
                    systemName,
                    queuedTimeAndExecution = task.queuedTimeAndExecutionName
                  ) >>
                    deleteTask(systemName, task).void >>
                    sendTaskSuccessThenDelete(systemName, tasks.tail)
                case exception: Throwable =>
                  IO.raiseError(exception)
              }

    for {
      _ <- logInfo(s"Starting flow control lambda with input: $potentialInput", executionStarter)
      flowControlConfig <- dependencies.ssmClient.getParameter[FlowControlConfig](config.configParamName)
      _ <- writeTaskToQueueTable(flowControlConfig)
      runningExecutions <- dependencies.stepFunctionClient.listStepFunctions(config.stepFunctionArn, Running)
      taskSuccessExecutor <-
        if runningExecutions.size < flowControlConfig.maxConcurrency && flowControlConfig.enabled then
          if flowControlConfig.hasReservedChannels then
            val executionsMap = runningExecutions.map(_.split("_").head).groupBy(identity).view.mapValues(_.size).toMap
            startTaskOnReservedChannel(flowControlConfig.sourceSystems, executionsMap, flowControlConfig, "").flatMap { taskExecutorName =>
              if taskExecutorName.nonEmpty then
                logInfo("Task started successfully on reserved channel. Terminating lambda", executionStarter, resumedExecution = taskExecutorName) >>
                  IO.pure(taskExecutorName)
              else if (flowControlConfig.hasSpareChannels)
                logInfo("Attempting to start task based on probability", executionStarter) >>
                  startTaskBasedOnProbability(flowControlConfig.sourceSystems)
              else
                logInfo("No task sent, no spare channels available, terminating lambda", executionStarter) >>
                  IO.pure(executionStarter)
            }
          else startTaskBasedOnProbability(flowControlConfig.sourceSystems)
        else if !flowControlConfig.enabled then
          logInfo("Flow control is disabled, terminating lambda", executionStarter) >>
            IO.pure("FLOW_CONTROL_DISABLED")
        else
          logInfo("Max concurrency reached, terminating lambda", executionStarter) >>
            IO.pure(executionStarter)
    } yield TaskOutput(taskSuccessExecutor, executionStarter)
  }

  private def logInfo(
      message: String,
      executionName: String,
      currentSystem: String = "",
      remainingSystems: String = "",
      queuedTimeAndExecution: String = "",
      resumedExecution: String = ""
  ): IO[Unit] = {
    logger.info(
      Map(
        "executionName" -> executionName,
        "currentSystem" -> currentSystem,
        "remainingSystems" -> remainingSystems,
        "queuedAt" -> queuedTimeAndExecution,
        "resumedExecution" -> resumedExecution
      ).filter(_._2.nonEmpty)
    )(message)
  }

  /** Builds a map of system name to a range. Range is a case class representing a starting point (inclusive) and ending point (exclusive) of the range. e.g. A config where
    * "System1" has 30% probability and "System2" has 15% probability will look like: "System1" -> Range(1, 31) "System2" -> Range(31, 46)
    *
    * If the probability for any system is zero, such system is excluded from the generated map
    *
    * @param systems
    *   list of @SourceSystem
    * @param rangeStart
    *   starting point for the next range iteration
    * @param sourceSystemProbabilityRanges
    *   accumulator to build the map of all ranges
    * @return
    *   map of system name to probability ranges
    */
  @tailrec
  final def buildProbabilityRangesMap(
      systems: List[Lambda.SourceSystem],
      rangeStart: Int,
      sourceSystemProbabilityRanges: Map[String, Range]
  ): Map[String, Range] = {
    systems match
      case Nil => sourceSystemProbabilityRanges
      case _ =>
        if systems.head.probability == 0 then buildProbabilityRangesMap(systems.tail, rangeStart, sourceSystemProbabilityRanges)
        else
          val rangeEnd = systems.head.probability + rangeStart
          val newMap = sourceSystemProbabilityRanges ++ Map(systems.head.systemName -> Range(rangeStart, rangeEnd))
          buildProbabilityRangesMap(systems.tail, rangeEnd, newMap)
  }

  override def dependencies(config: Config): IO[Dependencies] = IO(
    Dependencies(DADynamoDBClient[IO](), DASFNClient[IO](), DASSMClient[IO](), (min: Int, max: Int) => Generators().generateRandomInt(min, max))
  )
}

object Lambda {

  given Decoder[Option[Input]] = (c: HCursor) =>
    for {
      potentialExecutionName <- c.downField("executionName").as[Option[String]]
      potentialTaskToken <- c.downField("taskToken").as[Option[String]]
    } yield (potentialExecutionName, potentialTaskToken).mapN(Input.apply)

  private val default = "DEFAULT"
  private val continueProcessingNextSystem = "CONTINUE_TO_NEXT_SYSTEM"

  case class Dependencies(dynamoClient: DADynamoDBClient[IO], stepFunctionClient: DASFNClient[IO], ssmClient: DASSMClient[IO], randomInt: (Int, Int) => Int)
  case class Config(flowControlQueueTableName: String, configParamName: String, stepFunctionArn: String) derives ConfigReader
  case class Input(executionName: String, taskToken: String)
  case class TaskOutput(successSender: String, executionStarter: String)

  case class SourceSystem(systemName: String, reservedChannels: Int = 0, probability: Int = 0) {
    require(systemName.nonEmpty, "System name should not be empty")
    require(reservedChannels >= 0, "Reserved channels should not be fewer than zero")
    require(probability >= 0 && probability <= 100, "Probability must be between 0 and 100")
  }

  case class FlowControlConfig(maxConcurrency: Int, sourceSystems: List[SourceSystem], enabled: Boolean) {
    lazy private val reservedChannelsCount: Int = sourceSystems.map(_.reservedChannels).sum
    lazy private val probabilityTotal: Int = sourceSystems.map(_.probability).sum
    require(maxConcurrency >= 0, s"The max concurrency must be greater than or equal to 0, currently it is $maxConcurrency")
    require(sourceSystems.nonEmpty, "Source systems list cannot be empty")
    require(probabilityTotal == 100, s"The probability of all systems together should equate to 100%; the probability currently equates to $probabilityTotal%")
    require(reservedChannelsCount <= maxConcurrency, s"Total of reserved channels of $reservedChannelsCount exceeds maximum concurrency of $maxConcurrency")
    require(sourceSystems.map(_.systemName).toSet.size == sourceSystems.map(_.systemName).size, "System name must be unique")
    require(sourceSystems.map(_.systemName).contains(default), "Missing 'DEFAULT' system in the configuration")

    val hasReservedChannels: Boolean = reservedChannelsCount > 0
    val hasSpareChannels: Boolean = reservedChannelsCount < maxConcurrency
  }

  case class Range(startInclusive: Int, endExclusive: Int)
}
