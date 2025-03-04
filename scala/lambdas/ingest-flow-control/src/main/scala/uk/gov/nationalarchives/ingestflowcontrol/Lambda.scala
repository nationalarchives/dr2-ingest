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

class Lambda extends LambdaRunner[Option[Input], StateOutput, Config, Dependencies] {

  override def handler: (Option[Input], Config, Dependencies) => IO[StateOutput] = (potentialInput, config, dependencies) => {

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
      else if !(taskExecutorName.isBlank) && taskExecutorName != continueProcessingNextSystem then
        logInfo("Reserved channel: Task success sent by an execution, terminating further computation", taskExecutorName) >>
          IO.pure(taskExecutorName)
      else
        val currentSystem = sourceSystems.head
        val reservedChannels: Int = flowControlConfig.sourceSystems.find(_.systemName == currentSystem.systemName).map(_.reservedChannels).getOrElse(0)
        val currentExecutionCount = executionsBySystem.getOrElse(currentSystem.systemName, 0)
        val executionNameForLogging = potentialInput.map(_.executionName).getOrElse("NO_EXECUTION_NAME")
        if currentExecutionCount >= reservedChannels then
          logInfo(
            s"Reserved channel: Execution count of ${currentExecutionCount} exceeds reserved channels",
            executionNameForLogging,
            currentSystem.systemName,
            remainingSystems = sourceSystems.tail.map(_.systemName).mkString(",")
          ) >>
            startTaskOnReservedChannel(sourceSystems.tail, executionsBySystem, flowControlConfig, "")
        else
          logInfo("attempting progress on reserved channel for current system", executionNameForLogging, currentSystem.systemName) >>
            dependencies.dynamoClient
              .queryItems[IngestQueueTableItem](config.flowControlQueueTableName, "sourceSystem" === currentSystem.systemName)
              .flatMap { queueTableItems =>
                if queueTableItems.nonEmpty then
                  logInfo("Initiate processing for current system", executionNameForLogging, currentSystem.systemName) >>
                    processItems(currentSystem.systemName, queueTableItems).flatMap { itemOutput =>
                      if itemOutput == continueProcessingNextSystem then startTaskOnReservedChannel(sourceSystems.tail, executionsBySystem, flowControlConfig, "")
                      else IO(itemOutput)
                    }
                else
                  logInfo(
                    "Reserved channel: No tasks in queue for current system, continuing with remaining systems",
                    executionNameForLogging,
                    currentSystem.systemName,
                    sourceSystems.tail.map(_.systemName).mkString(", ")
                  ) >>
                    startTaskOnReservedChannel(sourceSystems.tail, executionsBySystem, flowControlConfig, "")
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
      val executionNameForLogging = potentialInput.map(_.executionName).getOrElse("NO_EXECUTION_NAME")
      sourceSystems match
        case Nil =>
          logInfo("Iterated over all source systems for probability, none of them have a waiting task, terminating lambda", executionNameForLogging, "") >>
            IO.pure(executionNameForLogging)
        case _ =>
          val sourceSystemProbabilities = buildProbabilityRangesMap(sourceSystems, 1, Map.empty[String, Range])
          if sourceSystemProbabilities.isEmpty then
            logInfo(
              "All remaining source systems have zero probability, terminating lambda",
              executionNameForLogging,
              remainingSystems = sourceSystems.map(_.systemName).mkString(", ")
            ) >>
              IO.pure(executionNameForLogging)
          else
            val maxRandomValue: Int = sourceSystemProbabilities.values.map(_.endExclusive).max
            val luckyDip = dependencies.randomInt(1, maxRandomValue)
            val sourceSystemEntry = sourceSystemProbabilities.find((_, probRange) => probRange.startInclusive <= luckyDip && probRange.endExclusive > luckyDip).get
            val systemToStartTaskOn = sourceSystemEntry._1
            val systemProbabilitiesStr = sourceSystemProbabilities.map { case (system, range) => s"$system -> (${range.startInclusive}, ${range.endExclusive})" }.mkString(", ")
            logInfo(
              s"Selected source system based on probability using lucky dip: $luckyDip in ranges $systemProbabilitiesStr",
              executionNameForLogging,
              systemToStartTaskOn
            ) >>
              dependencies.dynamoClient
                .queryItems[IngestQueueTableItem](config.flowControlQueueTableName, "sourceSystem" === systemToStartTaskOn)
                .flatMap { queueTableItems =>
                  if queueTableItems.nonEmpty then
                    logInfo("Initiate processing using probability approach", executionNameForLogging, sourceSystems.head.systemName) >>
                      processItems(systemToStartTaskOn, queueTableItems).flatMap { successExecutorName =>
                        if successExecutorName == continueProcessingNextSystem then startTaskBasedOnProbability(sourceSystems.filter(_.systemName != systemToStartTaskOn))
                        else IO.pure(successExecutorName)
                      }
                  else
                    val remainingSystems = sourceSystems.filter(_.systemName != systemToStartTaskOn)
                    logInfo(
                      "Probability: No tasks in queue for current system, continuing with remaining systems",
                      executionNameForLogging,
                      systemToStartTaskOn,
                      remainingSystems.map(_.systemName).mkString(", ")
                    ) >>
                      startTaskBasedOnProbability(remainingSystems)
                }
    }

    def writeItemToQueueTable(flowControlConfig: FlowControlConfig): IO[Unit] = IO.whenA(potentialInput.nonEmpty) {
      val input = potentialInput.get
      input.taskToken match
        case null | "" =>
          logInfo("Task token is empty, skipping writing item to flow control queue table", input.executionName)
        case _ =>
          val sourceSystemName = input.executionName.split("_").head
          val systemName = flowControlConfig.sourceSystems.find(_.systemName == sourceSystemName).map(_.systemName).getOrElse(default)
          val queuedTimeAndExecution = Instant.now.toString + "_" + input.executionName
          logInfo("Writing item to ingest queue table", input.executionName, systemName, queuedTimeAndExecution = queuedTimeAndExecution) >>
            dependencies.dynamoClient
              .writeItem(
                DADynamoDbWriteItemRequest(
                  config.flowControlQueueTableName,
                  Map(
                    sourceSystem -> AttributeValue.builder.s(systemName).build(),
                    queuedAt -> AttributeValue.builder.s(queuedTimeAndExecution).build(),
                    taskToken -> AttributeValue.builder.s(input.taskToken).build(),
                    executionName -> AttributeValue.builder.s(input.executionName).build()
                  )
                )
              )
              .void
    }

    def deleteItem(systemName: String, firstItem: IngestQueueTableItem) = {
      val executionNameForLogging = potentialInput.map(_.executionName).getOrElse("NO_EXECUTION_NAME")
      logInfo("Deleting item from ingest queue table", executionNameForLogging, systemName, queuedTimeAndExecution = s"${firstItem.queuedAtAndExecution}") >>
        dependencies.dynamoClient
          .deleteItems(
            config.flowControlQueueTableName,
            List(IngestQueuePrimaryKey(IngestQueuePartitionKey(systemName), IngestQueueSortKey(firstItem.queuedAtAndExecution)))
          )
    }

    def processItems(systemName: String, items: List[IngestQueueTableItem]): IO[String] =
      val executionNameForLogging = potentialInput.map(_.executionName).getOrElse("NO_EXECUTION_NAME")
      items match
        case Nil =>
          logInfo("Process Items: No more items to process for current system", executionNameForLogging, systemName) >>
            IO.pure(continueProcessingNextSystem)
        case _ =>
          val item = items.head
          logInfo(
            "sending success for the task",
            item.executionName,
            systemName,
            queuedTimeAndExecution = s"${item.queuedAtAndExecution}",
            resumedExecution = executionNameForLogging
          ) >>
            dependencies.stepFunctionClient
              .sendTaskSuccess(item.taskToken)
              .flatMap { _ =>
                logInfo(
                  "Task sent successfully, deleting item from table",
                  item.executionName,
                  systemName,
                  queuedTimeAndExecution = s"${item.queuedAtAndExecution}",
                  resumedExecution = executionNameForLogging
                ) >>
                  deleteItem(systemName, item) >> IO.pure(executionNameForLogging)
              }
              .handleErrorWith {
                case timeoutException: TaskTimedOutException =>
                  logInfo(
                    "Encountered TaskTimedOutException, deleting item from table",
                    executionNameForLogging,
                    systemName,
                    queuedTimeAndExecution = s"${item.queuedAtAndExecution}"
                  ) >>
                    deleteItem(systemName, item).void >>
                    processItems(systemName, items.tail)
                case exception: Throwable =>
                  IO.raiseError(exception)
              }

    for {
      executionNameForLogging <- IO.pure(potentialInput.map(_.executionName).getOrElse("NO_EXECUTION_NAME"))
      _ <- logInfo(s"Starting flow control lambda with input: $potentialInput", executionNameForLogging)
      flowControlConfig <- dependencies.ssmClient.getParameter[FlowControlConfig](config.configParamName)
      _ <- writeItemToQueueTable(flowControlConfig)
      runningExecutions <- dependencies.stepFunctionClient.listStepFunctions(config.stepFunctionArn, Running)
      taskSuccessExecutor <-
        if (runningExecutions.size < flowControlConfig.maxConcurrency) {
          if flowControlConfig.hasReservedChannels then
            val executionsMap = runningExecutions.map(_.split("_").head).groupBy(identity).view.mapValues(_.size).toMap
            startTaskOnReservedChannel(flowControlConfig.sourceSystems, executionsMap, flowControlConfig, "").flatMap { taskExecutorName =>
              if taskExecutorName.nonEmpty then
                logInfo("Task started successfully on reserved channel. Terminating lambda", executionNameForLogging, resumedExecution = taskExecutorName) >>
                  IO.pure(taskExecutorName)
              else if (flowControlConfig.hasSpareChannels)
                logInfo("Attempting to start task based on probability", executionNameForLogging) >>
                  startTaskBasedOnProbability(flowControlConfig.sourceSystems)
              else
                logInfo("No task sent, no spare channels available, terminating lambda", executionNameForLogging) >>
                  IO.pure(executionNameForLogging)
            }
          else startTaskBasedOnProbability(flowControlConfig.sourceSystems)
        } else {
          logInfo("Max concurrency reached, terminating lambda", executionNameForLogging) >>
            IO.pure(executionNameForLogging)
        }
    } yield StateOutput(taskSuccessExecutor)
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
        if systems.head.probability == 0 then {
          buildProbabilityRangesMap(systems.tail, rangeStart, sourceSystemProbabilityRanges)
        } else {
          val rangeEnd = systems.head.probability + rangeStart
          val newMap = sourceSystemProbabilityRanges ++ Map(systems.head.systemName -> Range(rangeStart, rangeEnd))
          buildProbabilityRangesMap(systems.tail, rangeEnd, newMap)
        }
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
    } yield {
      (potentialExecutionName, potentialTaskToken).mapN(Input.apply)
    }

  private val default = "DEFAULT"
  private val continueProcessingNextSystem = "CONTINUE_TO_NEXT_SYSTEM"

  case class Dependencies(dynamoClient: DADynamoDBClient[IO], stepFunctionClient: DASFNClient[IO], ssmClient: DASSMClient[IO], randomInt: (Int, Int) => Int)
  case class Config(flowControlQueueTableName: String, configParamName: String, stepFunctionArn: String) derives ConfigReader
  case class Input(executionName: String, taskToken: String)
  case class StateOutput(executionName: String)

  case class SourceSystem(systemName: String, reservedChannels: Int = 0, probability: Int = 0) {
    require(systemName.nonEmpty, "System name should not be empty")
    require(reservedChannels >= 0, "Reserved channels should not be fewer than zero")
    require(probability >= 0 && probability <= 100, "Probability must be between 0 and 100")
  }

  case class FlowControlConfig(maxConcurrency: Int, sourceSystems: List[SourceSystem]) {
    private val reservedChannelsCount: Int = sourceSystems.map(_.reservedChannels).sum
    private val probabilityTotal: Int = sourceSystems.map(_.probability).sum
    require(maxConcurrency > 0, s"The max concurrency must be greater than 0, currently it is $maxConcurrency")
    require(sourceSystems.nonEmpty, "Source systems list cannot be empty")
    require(probabilityTotal == 100, s"The probability of all systems together should equate to 100%; the probability currently equates to $probabilityTotal%")
    require(reservedChannelsCount <= maxConcurrency, s"Total of reserved channels of $reservedChannelsCount exceeds maximum concurrency of $maxConcurrency")
    require(sourceSystems.map(_.systemName).toSet.size == sourceSystems.map(_.systemName).size, "System name must be unique")
    require(sourceSystems.map(_.systemName).contains(default), "Missing 'DEFAULT' system in the configuration")

    def hasReservedChannels: Boolean = reservedChannelsCount > 0
    def hasSpareChannels: Boolean = reservedChannelsCount < maxConcurrency
  }

  case class Range(startInclusive: Int, endExclusive: Int)
}
