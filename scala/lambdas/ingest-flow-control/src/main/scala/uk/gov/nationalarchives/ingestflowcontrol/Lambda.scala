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
      *   IO[String]: name of the task executor which sent the success
      */
    def startTaskOnReservedChannel(
        sourceSystems: List[SourceSystem],
        executionsBySystem: Map[String, Int],
        flowControlConfig: FlowControlConfig,
        taskExecutorName: String
    ): IO[String] = {
      if sourceSystems.isEmpty then
        logger.info(Map("taskExecutor" -> s"$taskExecutorName"))("list of remaining sourceSystems is empty, nothing more to do on reserved channel") >> IO.pure(taskExecutorName)
      else if (!taskExecutorName.isBlank) then
        logger.info(Map("successSentBy" -> taskExecutorName))("Sent task success on a reserved channel, exiting reserved channel computation") >> IO.pure(taskExecutorName)
      else
        val currentSystem = sourceSystems.head
        val reservedChannels: Int = flowControlConfig.sourceSystems.find(_.systemName == currentSystem.systemName).map(_.reservedChannels).getOrElse(0)
        val currentExecutionCount = executionsBySystem.getOrElse(currentSystem.systemName, 0)
        val executionNameForLogging = potentialInput.map(_.executionName).getOrElse("NO_EXECUTION_NAME")
        if currentExecutionCount >= reservedChannels then
          logger.info(
            Map(
              "executionName" -> executionNameForLogging,
              "currentSystem" -> currentSystem.systemName,
              "executionCountOnSystem" -> s"$currentExecutionCount",
              "remainingSourceSystems" -> sourceSystems.tail.map(_.systemName).mkString(",")
            )
          )(
            s"Execution count on current system exceeds reserved channels, proceeding to check remaining source systems"
          ) >>
            startTaskOnReservedChannel(sourceSystems.tail, executionsBySystem, flowControlConfig, taskExecutorName)
        else
          logger.info(
            Map(
              "executionName" -> executionNameForLogging,
              "currentSystem" -> currentSystem.systemName
            )
          )(s"attempting progress on reserved channel for current system") >>
            dependencies.dynamoClient
              .queryItems[IngestQueueTableItem](config.flowControlQueueTableName, "sourceSystem" === currentSystem.systemName)
              .flatMap { queueTableItems =>
                if queueTableItems.nonEmpty then
                  logger.info(
                    Map(
                      "executionName" -> executionNameForLogging,
                      "taskTokens" -> queueTableItems.map(_.taskToken).mkString(", "),
                      "currentSystem" -> s"${currentSystem.systemName}"
                    )
                  )(s"Initiate processing for current system") >>
                    processItems(currentSystem.systemName, queueTableItems) map (_ => executionNameForLogging)
                else
                  logger.info(
                    Map(
                      "executionName" -> executionNameForLogging,
                      "remainingSystems" -> sourceSystems.tail.map(_.systemName).mkString(", ")
                    )
                  )("Continuing on reserved channel for remaining systems") >>
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
      *   IO[Unit]
      */
    def startTaskBasedOnProbability(sourceSystems: List[SourceSystem]): IO[Unit] = {
      val executionNameForLogging = potentialInput.map(_.executionName).getOrElse("NO_EXECUTION_NAME")
      sourceSystems match
        case Nil =>
          logger.info(
            Map(
              "executionName" -> executionNameForLogging
            )
          )("Iterated over all source systems for probability, none of them have a waiting task, terminating lambda")
        case _ =>
          val sourceSystemProbabilities = buildProbabilityRangesMap(sourceSystems, 1, Map.empty[String, Range])
          val maxRandomValue: Int = sourceSystemProbabilities.values.map(_.endExclusive).max
          val luckyDip = dependencies.randomInt(1, maxRandomValue)
          val sourceSystemEntry = sourceSystemProbabilities.find((_, probRange) => probRange.startInclusive <= luckyDip && probRange.endExclusive > luckyDip).get
          val systemToStartTaskOn = sourceSystemEntry._1
          val systemProbabilitiesStr = sourceSystemProbabilities.map { case (system, range) => s"$system -> (${range.startInclusive}, ${range.endExclusive})" }.mkString(", ")
          logger.info(
            Map(
              "executionName" -> executionNameForLogging,
              "luckyDip" -> s"$luckyDip",
              "systemRanges" -> systemProbabilitiesStr,
              "selection" -> systemToStartTaskOn
            )
          )(
            s"Selected source system based on probability. Attempting to send task success"
          ) >>
            dependencies.dynamoClient
              .queryItems[IngestQueueTableItem](config.flowControlQueueTableName, "sourceSystem" === systemToStartTaskOn)
              .flatMap { queueTableItems =>
                if queueTableItems.nonEmpty then
                  logger.info(
                    Map(
                      "executionName" -> executionNameForLogging,
                      "taskTokens" -> queueTableItems.map(_.taskToken).mkString(", "),
                      "currentSystem" -> sourceSystems.head.systemName
                    )
                  )("Initiate processing using probability approach") >>
                    processItems(systemToStartTaskOn, queueTableItems)
                else
                  val remainingSystems = sourceSystems.filter(_.systemName != systemToStartTaskOn)
                  logger.info(
                    Map(
                      "executionName" -> executionNameForLogging,
                      "remainingSystems" -> remainingSystems.map(_.systemName).mkString(", "),
                      "attemptedSystem" -> systemToStartTaskOn
                    )
                  )(
                    s"Attempted system does not have a waiting task, continuing to attempt on remaining systems"
                  ) >>
                    startTaskBasedOnProbability(remainingSystems)
              }
    }

    def writeItemToQueueTable(flowControlConfig: FlowControlConfig): IO[Unit] = IO.whenA(potentialInput.nonEmpty) {
      val input = potentialInput.get
      input.taskToken match
        case null | "" =>
          logger.info("Task token is empty, skipping writing item to flow control queue table")
        case _ =>
          val sourceSystemName = input.executionName.split("_").head
          val systemName = flowControlConfig.sourceSystems.find(_.systemName == sourceSystemName).map(_.systemName).getOrElse(default)
          val queuedAtTime = Instant.now.toString
          logger.info(
            Map(
              "executionName" -> input.executionName,
              "sourceSystem" -> systemName,
              "taskToken" -> input.taskToken,
              "queuedAt" -> queuedAtTime
            )
          )(s"Writing item to flow control queue table") >>
            dependencies.dynamoClient
              .writeItem(
                DADynamoDbWriteItemRequest(
                  config.flowControlQueueTableName,
                  Map(
                    sourceSystem -> AttributeValue.builder.s(systemName).build(),
                    queuedAt -> AttributeValue.builder.s(queuedAtTime).build(),
                    taskToken -> AttributeValue.builder.s(input.taskToken).build(),
                    executionName -> AttributeValue.builder.s(input.executionName).build()
                  )
                )
              )
              .void
    }

    def deleteItem(systemName: String, firstItem: IngestQueueTableItem) = {
      val executionNameForLogging = potentialInput.map(_.executionName).getOrElse("NO_EXECUTION_NAME")
      logger.info(
        Map(
          "taskToken" -> firstItem.taskToken,
          "system" -> systemName,
          "queuedAt" -> s"${firstItem.queuedAt}",
          "executionName" -> executionNameForLogging
        )
      )("Deleting item from flow control queue table") >>
        dependencies.dynamoClient
          .deleteItems(
            config.flowControlQueueTableName,
            List(IngestQueuePrimaryKey(IngestQueuePartitionKey(systemName), IngestQueueSortKey(firstItem.queuedAt)))
          )
    }

    def processItems(systemName: String, items: List[IngestQueueTableItem]): IO[Unit] =
      val executionNameForLogging = potentialInput.map(_.executionName).getOrElse("NO_EXECUTION_NAME")
      items match
        case Nil =>
          logger.info(
            Map(
              "system" -> systemName
            )
          )("No items to process") >>
            IO.unit
        case _ =>
          val item = items.head
          logger.info(
            Map(
              "taskToken" -> item.taskToken,
              "system" -> systemName,
              "queuedAt" -> s"${item.queuedAt}",
              "startedBy" -> s"${item.executionName}",
              "resumedExecution" -> executionNameForLogging
            )
          )("sending success for the task") >>
            dependencies.stepFunctionClient
              .sendTaskSuccess(item.taskToken)
              .flatMap { _ =>
                logger.info(
                  Map(
                    "taskToken" -> item.taskToken,
                    "system" -> systemName,
                    "queuedAt" -> s"${item.queuedAt}",
                    "resumedExecution" -> executionNameForLogging,
                    "executionName" -> s"${item.executionName}"
                  )
                )("Task sent successfully, deleting item from table") >>
                  deleteItem(systemName, item).void
              }
              .handleErrorWith {
                case timeoutException: TaskTimedOutException => {
                  logger.info(
                    Map(
                      "taskToken" -> item.taskToken,
                      "system" -> systemName,
                      "queuedAt" -> s"${item.queuedAt}",
                      "executionName" -> executionNameForLogging,
                      "exceptionMessage" -> timeoutException.getMessage
                    )
                  )("Encountered TaskTimedOutException, deleting item from table") >>
                    deleteItem(systemName, item).void >>
                    processItems(systemName, items.tail)
                }
                case exception: Throwable => {
                  IO.raiseError(exception)
                }
              }

    for {
      executionNameForLogging <- IO.pure(potentialInput.map(_.executionName).getOrElse("NO_EXECUTION_NAME"))
      _ <- logger.info(
        Map(
          "executionName" -> executionNameForLogging
        )
      )(s"Starting flow control lambda with input: $potentialInput")
      flowControlConfig <- dependencies.ssmClient.getParameter[FlowControlConfig](config.configParamName)
      _ <- writeItemToQueueTable(flowControlConfig)
      runningExecutions <- dependencies.stepFunctionClient.listStepFunctions(config.stepFunctionArn, Running)
      _ <- logger.info(
        Map(
          "executionName" -> executionNameForLogging,
          "running executions" -> runningExecutions.mkString(",")
        )
      )(
        s"Found ${runningExecutions.size} running executions against max concurrency of ${flowControlConfig.maxConcurrency}"
      )
      _ <- IO.whenA(runningExecutions.size < flowControlConfig.maxConcurrency) {
        if flowControlConfig.hasReservedChannels then
          val executionsMap = runningExecutions.map(_.split("_").head).groupBy(identity).view.mapValues(_.size).toMap
          startTaskOnReservedChannel(flowControlConfig.sourceSystems, executionsMap, flowControlConfig, "").flatMap { taskExecutorName =>
            if !taskExecutorName.isEmpty then logger.info(Map("executionName" -> executionNameForLogging))("Task started successfully on reserved channel. Terminating lambda")
            else if (flowControlConfig.hasSpareChannels)
              logger.info(Map("executionName" -> executionNameForLogging))("Attempting to start task based on probability") >>
                startTaskBasedOnProbability(flowControlConfig.sourceSystems)
            else
              logger.info(Map("executionName" -> executionNameForLogging))("No task sent, no spare channels available, terminating lambda")
          }
        else startTaskBasedOnProbability(flowControlConfig.sourceSystems)
      }
    } yield (StateOutput(executionNameForLogging))
  }

  /** Builds a map of system name to a range. Range is a case class representing a starting point (inclusive) and ending point (exclusive) of the range. e.g. A config where
    * "System1" has 30% probability and "System2" has 15% probability will look like: "System1" -> Range(1, 31) "System2" -> Range(31, 46)
    *
    * A zero-length range is represented by the starting and ending number to be same e.g. "default" -> Range(71, 71)
    *
    * @param systems
    *   list of @SourceSystem
    * @param rangeStart
    *   starting point for the next range iteration
    * @param sourceSystemProbabilityRanges
    *   accumulator to build the map of all ranges
    * @return
    *   accumulated map of system name to probability ranges
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
    } yield {
      (potentialExecutionName, potentialTaskToken).mapN(Input.apply)
    }

  private val default = "DEFAULT"

  case class Dependencies(dynamoClient: DADynamoDBClient[IO], stepFunctionClient: DASFNClient[IO], ssmClient: DASSMClient[IO], randomInt: (Int, Int) => Int)
  case class Config(flowControlQueueTableName: String, configParamName: String, stepFunctionArn: String) derives ConfigReader
  case class Input(executionName: String, taskToken: String)
  case class StateOutput(executionId: String)

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
