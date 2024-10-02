package uk.gov.nationalarchives.preingesttdraggregator

import cats.Parallel
import cats.effect.std.AtomicCell
import cats.effect.syntax.all.*
import cats.effect.{Async, Outcome}
import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.*
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jFactory
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbWriteItemRequest
import uk.gov.nationalarchives.preingesttdraggregator.Aggregator.NewGroupReason.*
import uk.gov.nationalarchives.preingesttdraggregator.Aggregator.{Input, OutputArguments}
import uk.gov.nationalarchives.preingesttdraggregator.Duration.*
import uk.gov.nationalarchives.preingesttdraggregator.Ids.*
import uk.gov.nationalarchives.preingesttdraggregator.Lambda.{Config, Group}
import uk.gov.nationalarchives.utils.Generators
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient, DASQSClient}

import java.net.URI
import java.time.Instant
import java.util.UUID

trait Aggregator[F[_]]:

  def aggregate(config: Config, atomicCell: AtomicCell[F, Map[String, Group]], messages: List[SQSMessage], remainingTimeInMillis: Int)(using
                                                                                                                                       Encoder[OutputArguments],
                                                                                                                                       Decoder[Input]
  ): F[List[Outcome[F, Throwable, Unit]]]

object Aggregator:

  given Encoder[OutputArguments] = (a: OutputArguments) =>
    Json.fromJsonObject(
      JsonObject(
        ("groupId", Json.fromString(a.groupId.groupValue)),
        ("batchId", Json.fromString(a.batchId.batchValue)),
        ("waitFor", a.waitFor.toJson),
        ("retryCount", Json.fromInt(a.retryCount))
      )
    )

  sealed trait Input:
    def id: String | UUID
  private case class TdrInput(id: UUID, location: URI) extends Input
  private case class CustodialCopyInput(id: String, deleted: Boolean) extends Input

  given Decoder[Input] = (c: HCursor) =>
    if c.keys.map(_.toList).getOrElse(Nil).contains("location") then
      for {
        id <- c.downField("id").as[String]
        location <- c.downField("location").as[String]
      } yield TdrInput(UUID.fromString(id), URI.create(location))
    else
      for {
        id <- c.downField("id").as[String]
        deleted <- c.downField("deleted").as[Boolean]
      } yield CustodialCopyInput(id, deleted)

  case class OutputArguments(groupId: GroupId, batchId: BatchId, waitFor: Seconds, retryCount: Int = 0)

  enum NewGroupReason:
    case NoExistingGroup, ExpiryBeforeLambdaTimeout, MaxGroupSizeExceeded

  def apply[F[_]: Async](using ev: Aggregator[F]): Aggregator[F] = ev

  given aggregator[F[_]: Async: DASFNClient: DADynamoDBClient: Parallel: DASQSClient](using Generators): Aggregator[F] = new Aggregator[F]:
    private val logger: SelfAwareStructuredLogger[F] = Slf4jFactory.create[F].getLogger

    private def logWithReason(sourceId: String)(newGroupReason: NewGroupReason): F[Unit] =
      logger.info(Map("action" -> "Start new group", "reason" -> newGroupReason.toString, "sourceId" -> sourceId))("Starting a new group")

    private def toDynamoString(value: String): AttributeValue = AttributeValue.builder.s(value).build

    def writeToLockTable(input: Input, config: Config, groupId: GroupId)(using dynamoClient: DADynamoDBClient[F]): F[Int] = {
      dynamoClient.writeItem(
        DADynamoDbWriteItemRequest(
          config.lockTable,
          Map(
            "assetId" -> toDynamoString(input.id.toString),
            "groupId" -> toDynamoString(groupId.groupValue),
            "message" -> toDynamoString(input.asJson.printWith(Printer.noSpaces))
          ),
          Some(s"attribute_not_exists(assetId)")
        )
      )
    }

    private def startNewGroup(sourceId: String, config: Config, groupExpiryTime: Milliseconds)(using enc: Encoder[OutputArguments],
                                                                                               sfnClient: DASFNClient[F],
                                                                                               sqsClient: DASQSClient[F]
    ): F[Group] = {
      val waitFor: Seconds = Math.ceil((groupExpiryTime - Generators().generateInstant.toEpochMilli.milliSeconds).toDouble / 1000).toInt.seconds
      val groupId: GroupId = GroupId(config.sourceSystem)
      val batchId = BatchId(groupId)
      val group = Group(groupId, Instant.ofEpochMilli(groupExpiryTime.length), 1)
      val outputArguments = OutputArguments(groupId, batchId, waitFor)
      if config.sfnArn.isDefined then
        sfnClient.startExecution(config.sfnArn.get, outputArguments, batchId.batchValue.some).map(_ => group)
      else if config.outputQueue.isDefined then
        sqsClient.sendMessage(config.outputQueue.get)(outputArguments, delaySeconds = waitFor.length).map(_ => group)
      else
        Async[F].raiseError(new Exception("Either the SFN Arn or SQS queue url must be set"))
    }

    private def getNewOrExistingGroupId(atomicCell: AtomicCell[F, Map[String, Group]], config: Config, sourceId: String, lambdaTimeoutTime: Milliseconds)(using
        Encoder[OutputArguments]
    ): F[GroupId] = {
      def log = logWithReason(sourceId)
      val groupExpiryTime: Milliseconds = lambdaTimeoutTime + config.maxSecondaryBatchingWindow.toMilliSeconds
      atomicCell
        .evalUpdateAndGet { groupCache =>
          val groupF = groupCache.get(sourceId) match
            case None => log(NoExistingGroup) >> startNewGroup(sourceId, config, groupExpiryTime)
            case Some(currentGroupForSource) =>
              if currentGroupForSource.expires.toEpochMilli <= lambdaTimeoutTime.length then log(ExpiryBeforeLambdaTimeout) >> startNewGroup(sourceId, config, groupExpiryTime)
              else if currentGroupForSource.itemCount > config.maxBatchSize then log(MaxGroupSizeExceeded) >> startNewGroup(sourceId, config, groupExpiryTime)
              else Async[F].pure(currentGroupForSource.copy(itemCount = currentGroupForSource.itemCount + 1))
          groupF.map(group => groupCache + (sourceId -> group))
        }
        .map(_(sourceId).groupId)
    }

    override def aggregate(config: Config, atomicCell: AtomicCell[F, Map[String, Group]], messages: List[SQSMessage], remainingTimeInMillis: Int)(using
                                                                                                                                                  Encoder[OutputArguments],
                                                                                                                                                  Decoder[Input]
    ): F[List[Outcome[F, Throwable, Unit]]] = {
      val lambdaTimeoutTime = (Generators().generateInstant.toEpochMilli + remainingTimeInMillis).milliSeconds
      for {
        fibers <- messages.parTraverse { record =>
          val process = for {
            groupId <- getNewOrExistingGroupId(atomicCell, config, record.getEventSourceArn, lambdaTimeoutTime)
            input <- Async[F].fromEither(decode[Input](record.getBody))
            _ <- writeToLockTable(input, config, groupId)
          } yield ()
          process.start
        }
        results <- fibers.traverse(_.join)
        _ <- logger.info(Map("successes" -> results.count(_.isSuccess).toString, "failures" -> results.count(_.isError).toString))("Aggregation complete")
      } yield results
    }
