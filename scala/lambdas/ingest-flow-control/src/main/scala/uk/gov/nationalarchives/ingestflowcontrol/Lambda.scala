package uk.gov.nationalarchives.ingestflowcontrol

import cats.effect.*
//import cats.syntax.all.*
import io.circe.generic.auto.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbWriteItemRequest
import uk.gov.nationalarchives.DASFNClient.Status.Running
import uk.gov.nationalarchives.ingestflowcontrol.Lambda.*
import uk.gov.nationalarchives.utils.{Generators, LambdaRunner}
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient, DASSMClient}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{*, given}

import java.time.Instant
import scala.annotation.tailrec
import cats.effect.unsafe.implicits.global

class Lambda extends LambdaRunner[Input, Unit, Config, Dependencies] {

  override def handler: (Input, Config, Dependencies) => IO[Unit] = { (input, config, dependencies) =>
    {
      /**
       * A recursive method to send task success to one and only one task per invocation. It takes a list of source systems,
       * iterates over the list (recursively) to start a task on first system which has a dedicated channel available
       * @param sourceSystems List of source systems to iterate over
       * @param executionsMap Map of SystemName -> current running executions
       * @param flowControlConfig Flow control configuration
       * @param taskStarted Boolean indicating if a task was started in the recursive call
       * @return IO[Boolean]: true if it called "sendTaskSuccess" on one of the tasks, otherwise false
       */
      def startTaskOnDedicatedChannel(sourceSystems: List[SourceSystem], executionsMap: Map[String, Int], flowControlConfig: FlowControlConfig, taskStarted: Boolean): IO[Boolean] = {
        if sourceSystems.isEmpty then
          IO(taskStarted)
        else if taskStarted then
          IO(true)
        else
          val currentSystem = sourceSystems.head
          val dedicatedChannels: Int = flowControlConfig.sourceSystems.find(_.systemName == currentSystem.systemName).get.dedicatedChannels
          val currentExecutionCount = executionsMap.getOrElse(currentSystem.systemName, 0)
          if currentExecutionCount >= dedicatedChannels then
            IO(false)
          else
            val partKeys = List(IngestQueuePartitionKey(currentSystem.systemName))
            dependencies.dynamoClient
              .getItems[IngestQueueTableItem, IngestQueuePartitionKey](partKeys, config.flowControlQueueTableName)
              .flatMap { queueTableItems =>
              val taskToken = queueTableItems.headOption.map(_.taskToken)
              val queuedAt = queueTableItems.headOption.map(_.queuedAt)
              if (taskToken.nonEmpty) {
                //FIXME: use of unsafeRunSync, need to do it elegantly
                (dependencies.stepFunctionClient.sendTaskSuccess(taskToken.get) >>
                  dependencies.dynamoClient.deleteItems(config.flowControlQueueTableName,
                    List(IngestQueuePrimaryKey(IngestQueuePartitionKey(currentSystem.systemName), IngestQueueSortKey(queuedAt.get))))).unsafeRunSync()
                IO(true)
              } else {
                startTaskOnDedicatedChannel(sourceSystems.tail, executionsMap, flowControlConfig, taskStarted)
              }
            }
      }

      /**
       * A recursive method to send success to one of the systems based on probability as configured in the Flow control configuration.
       * It builds a probability ranges map based on the config (e.g. TDR -> (1, 25), FCL -> (25, 40) ... ). It then generates a random
       * number between the minimum and maximum value over all probabilities and tries to schedule a task for that system, If there is
       * no task waiting for the system, it recreates the probability map excluding that system from the config and generates a random
       * number for the remaining systems only, thus making sure that the probabilities are honoured over all iterations.
       * @param sourceSystems List of source systems to be used
       * @param skippedSystems List of systems that do not have a waiting task at that point in time
       * @return IO[Unit]
       */
      def startTaskBasedOnProbability(sourceSystems: List[SourceSystem], skippedSystems: List[String]): IO[Unit] = {
        if (sourceSystems.size == skippedSystems.size) {
          IO.unit // nothing more to do, no one is waiting
        } else {
          val sourceSystemProbabilityMap = buildProbabilityRangesMap(sourceSystems, skippedSystems, 1, Map.empty[String, (Int, Int)])
          val maxRandomValue: Int = sourceSystemProbabilityMap.values.maxBy(_._2)._2
          val luckyDip = dependencies.randomInt(1, maxRandomValue)
          val sourceSystemEntry = sourceSystemProbabilityMap.find(eachEntry => eachEntry._2._1 <= luckyDip && eachEntry._2._2 >= luckyDip).get
          val systemToStartTaskOn = sourceSystemEntry._1

          dependencies.dynamoClient
            .getItems[IngestQueueTableItem, IngestQueuePartitionKey](List(IngestQueuePartitionKey(systemToStartTaskOn)), config.flowControlQueueTableName)
            .flatMap { queueTableItem =>
              val taskToken = queueTableItem.headOption.map(_.taskToken)
              val queuedAt = queueTableItem.headOption.map(_.queuedAt)
              if (taskToken.nonEmpty) {
                dependencies.stepFunctionClient.sendTaskSuccess(taskToken.get) >>
                dependencies.dynamoClient.deleteItems(config.flowControlQueueTableName,
                  List(IngestQueuePrimaryKey(IngestQueuePartitionKey(systemToStartTaskOn), IngestQueueSortKey(queuedAt.get)))).void
              } else {
                val newSkippedSystems = skippedSystems :+ systemToStartTaskOn
                startTaskBasedOnProbability(sourceSystems, newSkippedSystems)
              }
            }
        }
      }

      def writeItemToQueueTable(flowControlConfig: FlowControlConfig): IO[Unit] = IO.whenA(input.taskToken.nonEmpty) {
        val sourceSystemName = getSourceSystemName(input.executionName)
        val systemName = flowControlConfig.sourceSystems.find(_.systemName == sourceSystemName).map(_.systemName).getOrElse("DEFAULT")
        dependencies.dynamoClient.writeItem(
          DADynamoDbWriteItemRequest(
            config.flowControlQueueTableName,
            Map(
              sourceSystem -> AttributeValue.builder.s(systemName).build(),
              queuedAt -> AttributeValue.builder.s(Instant.now.toString).build(),
              taskToken -> AttributeValue.builder.s(input.taskToken).build()
            )
          )
        ).void
      }

      for {
        flowControlConfig <- dependencies.ssmClient.getParameter[FlowControlConfig](config.configParamName)
        _ <- writeItemToQueueTable(flowControlConfig)
        runningExecutions <- dependencies.stepFunctionClient.listStepFunctions(config.stepFunctionArn, Running)
        executionsMap = runningExecutions.map(_.split("_").head).groupBy(identity).view.mapValues(_.size).toMap
        _ <- IO.whenA(runningExecutions.size < flowControlConfig.maxConcurrency) {
          if (flowControlConfig.hasDedicatedChannels) {
            startTaskOnDedicatedChannel(flowControlConfig.sourceSystems, executionsMap, flowControlConfig, false).flatMap { answer =>
                if (answer == true) {
                  IO.unit
                } else {
                  if (flowControlConfig.hasSpareChannels) {
                    startTaskBasedOnProbability(flowControlConfig.sourceSystems, List.empty)
                  } else {
                    IO.unit
                  }
                }
              }
          } else {
            if (flowControlConfig.hasSpareChannels) {
              startTaskBasedOnProbability(flowControlConfig.sourceSystems, List.empty)
            } else {
              IO.unit
            }
          }
        }
      } yield ()
    }
  }


  def getSourceSystemName(executionName: String): String = {
    executionName.split("_").head
  }

  /** Builds a map of system name to a range. Range is a tuple of starting point (inclusive) and ending point (exclusive) of the range. e.g. a config where "System1" has 30%
    * probability and "System2" has 15% probability will look like: "System1" -> (1, 31) "System2" -> (31, 45)
    *
    * A zero length range is represented by the starting and ending number to be same e.g. "default" -> (71, 71)
    * @param systems
    *   list of @SourceSystem
    * @param skippedSystems
    *   list of systems that do not have any waiting task
    * @param rangeStart
    *   starting point for the next range iteration
    * @param accumulatedMap
    *   accumulator to build the map of all ranges
    * @return
    *   accumulated map of system name to probability ranges
    */
  @tailrec
  final def buildProbabilityRangesMap(
      systems: List[Lambda.SourceSystem],
      skippedSystems: List[String],
      rangeStart: Int,
      accumulatedMap: Map[String, (Int, Int)]
  ): Map[String, (Int, Int)] = {
    systems.size match
      case 0 => accumulatedMap
      case _ =>
        if (skippedSystems.contains(systems.head.systemName)) {
          buildProbabilityRangesMap(systems.tail, skippedSystems, rangeStart, accumulatedMap)
        } else {
          val rangeEnd = systems.head.probability + rangeStart
          val newMap = accumulatedMap ++ Map(systems.head.systemName -> (rangeStart, rangeEnd))
          buildProbabilityRangesMap(systems.tail, skippedSystems, rangeEnd, newMap)
        }
  }

  override def dependencies(config: Config): IO[Dependencies] = IO(Dependencies(DADynamoDBClient[IO](), DASFNClient[IO](), DASSMClient[IO](), (min: Int, max: Int) => Generators().generateRandomInt(min, max)))
}

object Lambda {
  case class Dependencies(dynamoClient: DADynamoDBClient[IO], stepFunctionClient: DASFNClient[IO], ssmClient: DASSMClient[IO], randomInt: (Int, Int) => Int)
  case class Config(flowControlQueueTableName: String, configParamName: String, stepFunctionArn: String) derives ConfigReader
  case class Input(executionName: String, taskToken: String)

  case class SourceSystem(systemName: String, dedicatedChannels: Int = 0, probability: Int = 0) {
    require(systemName.nonEmpty, "System name should not be empty")
    require(dedicatedChannels >= 0, "Dedicated channels should not be fewer than zero")
    require(probability >= 0 && probability <= 100, "Probability must be between 0 and 100")
  }

  case class FlowControlConfig(maxConcurrency: Int, sourceSystems: List[SourceSystem]) {
    require(sourceSystems.nonEmpty, "Source systems list cannot be empty")
    require(sourceSystems.map(_.probability).sum == 100, "The probability of all systems together should equate to 100%")
    require(sourceSystems.map(_.dedicatedChannels).sum <= maxConcurrency, "Total of dedicated channels exceed maximum concurrency")
    require(sourceSystems.map(_.systemName).toSet.size == sourceSystems.map(_.systemName).size, "System name must be unique")
    require(sourceSystems.map(_.systemName).contains("default"), "Missing 'default' system in the configuration")

    def hasDedicatedChannels: Boolean = sourceSystems.map(_.dedicatedChannels).sum > 0
    def hasSpareChannels: Boolean = sourceSystems.map(_.dedicatedChannels).sum < maxConcurrency
  }
}
