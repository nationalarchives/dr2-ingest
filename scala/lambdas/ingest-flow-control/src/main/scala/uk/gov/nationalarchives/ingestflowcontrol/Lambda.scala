package uk.gov.nationalarchives.ingestflowcontrol

import cats.effect.*
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

class Lambda extends LambdaRunner[Input, Unit, Config, Dependencies] {

  override def handler: (Input, Config, Dependencies) => IO[Unit] = (input, config, dependencies) => {

    /** A recursive method to send task success to one and only one task per invocation. It takes a list of source systems, iterates over the list (recursively) to start a task on
      * first system which has a reserved channel available
      * @param sourceSystems
      *   List of source systems to iterate over
      * @param executionsBySystem
      *   Map of SystemName -> count of running executions
      * @param flowControlConfig
      *   Flow control configuration
      * @param taskStarted
      *   Boolean indicating if a task was started in the recursive call
      * @return
      *   IO[Boolean]: true if it called "sendTaskSuccess" on one of the tasks, otherwise false
      */
    def startTaskOnReservedChannel(
        sourceSystems: List[SourceSystem],
        executionsBySystem: Map[String, Int],
        flowControlConfig: FlowControlConfig,
        taskStarted: Boolean
    ): IO[Boolean] = {
      if sourceSystems.isEmpty then IO.pure(taskStarted)
      else if taskStarted then IO.pure(true)
      else
        val currentSystem = sourceSystems.head
        val reservedChannels: Int = flowControlConfig.sourceSystems.find(_.systemName == currentSystem.systemName).map(_.reservedChannels).getOrElse(0)
        val currentExecutionCount = executionsBySystem.getOrElse(currentSystem.systemName, 0)
        if currentExecutionCount >= reservedChannels then startTaskOnReservedChannel(sourceSystems.tail, executionsBySystem, flowControlConfig, taskStarted)
        else
          val partKeys = List(IngestQueuePartitionKey(currentSystem.systemName))
          dependencies.dynamoClient
            .getItems[IngestQueueTableItem, IngestQueuePartitionKey](partKeys, config.flowControlQueueTableName)
            .flatMap { queueTableItems =>
              queueTableItems.headOption match
                case Some(firstItem) =>
                  (dependencies.stepFunctionClient.sendTaskSuccess(firstItem.taskToken) >>
                    dependencies.dynamoClient.deleteItems(
                      config.flowControlQueueTableName,
                      List(IngestQueuePrimaryKey(IngestQueuePartitionKey(currentSystem.systemName), IngestQueueSortKey(firstItem.queuedAt)))
                    )) >>
                    IO.pure(true)
                case None => startTaskOnReservedChannel(sourceSystems.tail, executionsBySystem, flowControlConfig, taskStarted)
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
      sourceSystems match
        case Nil => IO.unit // nothing more to do, no one is waiting
        case _ =>
          val sourceSystemProbabilities = buildProbabilityRangesMap(sourceSystems, 1, Map.empty[String, Range])
          val maxRandomValue: Int = sourceSystemProbabilities.values.map(_.endExclusive).max
          val luckyDip = dependencies.randomInt(1, maxRandomValue)
          val sourceSystemEntry = sourceSystemProbabilities.find((_, probRange) => probRange.startInclusive <= luckyDip && probRange.endExclusive > luckyDip).get
          val systemToStartTaskOn = sourceSystemEntry._1

          dependencies.dynamoClient
            .getItems[IngestQueueTableItem, IngestQueuePartitionKey](List(IngestQueuePartitionKey(systemToStartTaskOn)), config.flowControlQueueTableName)
            .flatMap { queueTableItem =>
              queueTableItem.headOption match
                case Some(firstItem) =>
                  dependencies.stepFunctionClient.sendTaskSuccess(firstItem.taskToken) >>
                    dependencies.dynamoClient
                      .deleteItems(
                        config.flowControlQueueTableName,
                        List(IngestQueuePrimaryKey(IngestQueuePartitionKey(systemToStartTaskOn), IngestQueueSortKey(firstItem.queuedAt)))
                      )
                      .void
                case None =>
                  val remainingSystems = sourceSystems.filter(_.systemName != systemToStartTaskOn)
                  startTaskBasedOnProbability(remainingSystems)
            }
    }

    def writeItemToQueueTable(flowControlConfig: FlowControlConfig): IO[Unit] = IO.whenA(input.taskToken.nonEmpty) {
      val sourceSystemName = input.executionName.split("_").head
      val systemName = flowControlConfig.sourceSystems.find(_.systemName == sourceSystemName).map(_.systemName).getOrElse(default)
      dependencies.dynamoClient
        .writeItem(
          DADynamoDbWriteItemRequest(
            config.flowControlQueueTableName,
            Map(
              sourceSystem -> AttributeValue.builder.s(systemName).build(),
              queuedAt -> AttributeValue.builder.s(Instant.now.toString).build(),
              taskToken -> AttributeValue.builder.s(input.taskToken).build()
            )
          )
        )
        .void
    }

    for {
      flowControlConfig <- dependencies.ssmClient.getParameter[FlowControlConfig](config.configParamName)
      _ <- writeItemToQueueTable(flowControlConfig)
      runningExecutions <- dependencies.stepFunctionClient.listStepFunctions(config.stepFunctionArn, Running)
      _ <- IO.whenA(runningExecutions.size < flowControlConfig.maxConcurrency) {
        if flowControlConfig.hasReservedChannels then
          val executionsMap = runningExecutions.map(_.split("_").head).groupBy(identity).view.mapValues(_.size).toMap
          startTaskOnReservedChannel(flowControlConfig.sourceSystems, executionsMap, flowControlConfig, false).flatMap { taskStarted =>
            if taskStarted then IO.unit
            else if (flowControlConfig.hasSpareChannels) startTaskBasedOnProbability(flowControlConfig.sourceSystems)
            else IO.unit
          }
        else startTaskBasedOnProbability(flowControlConfig.sourceSystems)
      }
    } yield ()
  }

  /** Builds a map of system name to a range. Range is a case class representing a starting point (inclusive) and ending point (exclusive) of the range. e.g. A config where
    * "System1" has 30% probability and "System2" has 15% probability will look like: "System1" -> Range(1, 31) "System2" -> Range(31, 46)
    *
    * A zero-length range is represented by the starting and ending number to be same e.g. "default" -> Range(71, 71)
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

  private val default = "DEFAULT"

  case class Dependencies(dynamoClient: DADynamoDBClient[IO], stepFunctionClient: DASFNClient[IO], ssmClient: DASSMClient[IO], randomInt: (Int, Int) => Int)
  case class Config(flowControlQueueTableName: String, configParamName: String, stepFunctionArn: String) derives ConfigReader
  case class Input(executionName: String, taskToken: String)

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
