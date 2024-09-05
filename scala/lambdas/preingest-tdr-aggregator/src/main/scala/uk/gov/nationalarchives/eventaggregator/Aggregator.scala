package uk.gov.nationalarchives.eventaggregator

import cats.Parallel
import cats.effect.syntax.all.*
import cats.effect.{Async, Outcome, Ref}
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
import uk.gov.nationalarchives.eventaggregator.Aggregator.NewGroupReason.*
import uk.gov.nationalarchives.eventaggregator.Aggregator.{Input, SFNArguments}
import uk.gov.nationalarchives.eventaggregator.Duration.{MilliSeconds, Seconds}
import uk.gov.nationalarchives.eventaggregator.Ids.*
import uk.gov.nationalarchives.eventaggregator.Lambda.{Config, Group}
import uk.gov.nationalarchives.utils.Generators
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient}

import java.net.URI
import java.time.Instant
import java.util.UUID

trait Aggregator[F[_]]:

  def aggregate(config: Config, ref: Ref[F, Map[String, Group]], messages: List[SQSMessage], remainingTimeInMillis: Int)(using
      Encoder[SFNArguments],
      Decoder[Input]
  ): F[List[Outcome[F, Throwable, Unit]]]

object Aggregator:

  given Encoder[SFNArguments] = (a: SFNArguments) =>
    Json.fromJsonObject(
      JsonObject(
        ("groupId", Json.fromString(a.groupId.groupValue)),
        ("batchId", Json.fromString(a.batchId.batchValue)),
        ("waitFor", a.waitFor.toJson),
        ("retryCount", Json.fromInt(a.retryCount))
      )
    )

  extension (i: Int) def seconds: Seconds = Seconds(i)

  extension (l: Long) private def milliSeconds: MilliSeconds = MilliSeconds(l)

  case class Input(id: UUID, location: URI)

  case class SFNArguments(groupId: GroupId, batchId: BatchId, waitFor: Seconds, retryCount: Int = 0)

  enum NewGroupReason:
    case NoExistingGroup, ExpiryBeforeLambdaTimeout, MaxGroupSizeExceeded

  def apply[F[_]: Async](using ev: Aggregator[F]): Aggregator[F] = ev

  given aggregator[F[_]: Async: DASFNClient: DADynamoDBClient: Parallel](using Generators): Aggregator[F] = new Aggregator[F]:
    private val logger: SelfAwareStructuredLogger[F] = Slf4jFactory.create[F].getLogger

    private def logWithReason(sourceId: String)(newGroupReason: NewGroupReason): F[Unit] =
      logger.info(Map("action" -> "Start new group", "reason" -> newGroupReason.toString, "sourceId" -> "sourceId"))("Starting a new group")

    private def toDynamoString(value: String): AttributeValue = AttributeValue.builder.s(value).build

    def writeToLockTable(input: Input, config: Config, groupId: GroupId)(using dynamoClient: DADynamoDBClient[F]): F[Int] = {
      dynamoClient.writeItem(
        DADynamoDbWriteItemRequest(
          config.lockTable,
          Map(
            "messageId" -> toDynamoString(input.id.toString),
            "groupId" -> toDynamoString(groupId.groupValue),
            "message" -> toDynamoString(input.asJson.printWith(Printer.noSpaces))
          )
        )
      )
    }

    private def startNewGroup(groupCacheRef: Ref[F, Map[String, Group]], sourceId: String, config: Config, lambdaTimeoutTime: MilliSeconds)(using
        sfnClient: DASFNClient[F],
        enc: Encoder[SFNArguments]
    ): F[GroupId] = {
      val groupExpiryTime = lambdaTimeoutTime + (config.maxSecondaryBatchingWindow * 1000)
      val waitFor: Seconds = Math.ceil((groupExpiryTime - Generators().generateInstant.toEpochMilli).toDouble / 1000).toInt.seconds
      val groupId: GroupId = GroupId(config.sourceSystem)
      val batchId = BatchId(groupId)
      for {
        _ <- sfnClient.startExecution(config.sfnArn, SFNArguments(groupId, batchId, waitFor), batchId.batchValue.some)
        _ <- groupCacheRef.update(groupCache => groupCache + (sourceId -> Group(groupId, Instant.ofEpochMilli(groupExpiryTime), 1)))
      } yield groupId
    }

    private def getNewOrExistingGroupId(groupCacheRef: Ref[F, Map[String, Group]], config: Config, sourceId: String, lambdaTimeoutTime: MilliSeconds)(using
        Encoder[SFNArguments]
    ): F[GroupId] = {
      def log = logWithReason(sourceId)
      groupCacheRef.get.flatMap { groupCache =>
        val potentialCurrentGroupForSource = groupCache.get(sourceId)
        potentialCurrentGroupForSource match
          case None => log(NoExistingGroup) >> startNewGroup(groupCacheRef, sourceId, config, lambdaTimeoutTime)
          case Some(currentGroupForSource) =>
            if currentGroupForSource.expires.toEpochMilli <= lambdaTimeoutTime then
              log(ExpiryBeforeLambdaTimeout) >> startNewGroup(groupCacheRef, sourceId, config, lambdaTimeoutTime)
            else if currentGroupForSource.itemCount > config.maxBatchSize then log(MaxGroupSizeExceeded) >> startNewGroup(groupCacheRef, sourceId, config, lambdaTimeoutTime)
            else groupCacheRef.update(_ + (sourceId -> currentGroupForSource.copy(itemCount = currentGroupForSource.itemCount + 1))) >> Async[F].pure(currentGroupForSource.groupId)
      }
    }

    override def aggregate(config: Config, ref: Ref[F, Map[String, Group]], messages: List[SQSMessage], remainingTimeInMillis: Int)(using
        Encoder[SFNArguments],
        Decoder[Input]
    ): F[List[Outcome[F, Throwable, Unit]]] = {
      val lambdaTimeoutTime = (Generators().generateInstant.toEpochMilli + remainingTimeInMillis).milliSeconds
      for {
        fibers <- messages.parTraverse { record =>
          val process = for {
            groupId <- getNewOrExistingGroupId(ref, config, record.getEventSourceArn, lambdaTimeoutTime)
            input <- Async[F].fromEither(decode[Input](record.getBody))
            _ <- writeToLockTable(input, config, groupId)
          } yield ()
          process.start
        }
        results <- fibers.traverse(_.join)
        _ <- logger.info(Map("successes" -> results.count(_.isSuccess).toString, "failures" -> results.count(_.isError).toString))("Aggregation complete")
      } yield results
    }
