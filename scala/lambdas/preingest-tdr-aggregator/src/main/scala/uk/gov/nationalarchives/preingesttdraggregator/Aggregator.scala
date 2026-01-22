package uk.gov.nationalarchives.preingesttdraggregator

import cats.Parallel
import cats.effect.kernel.Outcome
import cats.effect.std.AtomicCell
import cats.effect.syntax.all.*
import cats.effect.Async
import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.decode
import org.scanamo.DynamoValue
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jFactory
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbWriteItemRequest
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.IngestLockTableItem
import uk.gov.nationalarchives.dynamoformatters.DynamoWriteUtils
import uk.gov.nationalarchives.preingesttdraggregator.Aggregator.NewGroupReason.*
import uk.gov.nationalarchives.preingesttdraggregator.Aggregator.{Input, SFNArguments}
import uk.gov.nationalarchives.preingesttdraggregator.Duration.*
import uk.gov.nationalarchives.preingesttdraggregator.Ids.*
import uk.gov.nationalarchives.preingesttdraggregator.Lambda.{Config, Group}
import uk.gov.nationalarchives.utils.ExternalUtils.NotificationMessage
import uk.gov.nationalarchives.utils.ExternalUtils.given
import uk.gov.nationalarchives.utils.Generators
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient}

import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*

trait Aggregator[F[_]]:

  def aggregate(config: Config, atomicCell: AtomicCell[F, Map[String, Group]], messages: List[SQSMessage], remainingTimeInMillis: Int)(using
      Encoder[SFNArguments],
      Decoder[Input]
  ): F[List[BatchItemFailure]]

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

  type Input = NotificationMessage

  case class SFNArguments(groupId: GroupId, batchId: BatchId, waitFor: Seconds, retryCount: Int = 0)

  private case class AggregatorError(messageId: String) extends Throwable

  enum NewGroupReason:
    case NoExistingGroup, ExpiryBeforeLambdaTimeout, MaxGroupSizeExceeded

  def apply[F[_]: {Async, Parallel}](sfnClient: DASFNClient[F], dynamoClient: DADynamoDBClient[F])(using Generators): Aggregator[F] =
    new Aggregator[F]:
      private val logger: SelfAwareStructuredLogger[F] = Slf4jFactory.create[F].getLogger

      private def logWithReason(sourceId: String)(newGroupReason: NewGroupReason): F[Unit] =
        logger.info(Map("action" -> "Start new group", "reason" -> newGroupReason.toString, "sourceId" -> sourceId))("Starting a new group")

      private def toDynamoString(value: String): AttributeValue = AttributeValue.builder.s(value).build

      def writeToLockTable(id: UUID, msgBody: String, config: Config, groupId: GroupId): F[Int] = {
        val dynamoLockTableItem: DynamoValue = DynamoWriteUtils.writeLockTableItem(
          IngestLockTableItem(id, groupId.groupValue, msgBody, Generators().generateInstant.toString)
        )

        dynamoClient.writeItem(
          DADynamoDbWriteItemRequest(
            config.lockTable,
            dynamoLockTableItem.toAttributeValue.m().asScala.toMap,
            Some(s"attribute_not_exists(assetId)")
          )
        )
      }

      private def startNewGroup(config: Config, groupExpiryTime: Milliseconds, id: UUID, msgBody: String)(using enc: Encoder[SFNArguments]): F[Group] = {
        val groupId: GroupId = GroupId(config.sourceSystem)
        val batchId = BatchId(groupId)
        writeToLockTable(id, msgBody, config, groupId) >> {
          val waitFor: Seconds = Math.ceil((groupExpiryTime - Generators().generateInstant.toEpochMilli.milliSeconds).toDouble / 1000).toInt.seconds
          sfnClient.startExecution(config.sfnArn, SFNArguments(groupId, batchId, waitFor), batchId.batchValue.some).map { _ =>
            Group(groupId, Instant.ofEpochMilli(groupExpiryTime.length), 1)
          }
        }
      }

      private def updateOrCreateNewGroup(atomicCell: AtomicCell[F, Map[String, Group]], config: Config, sourceId: String, lambdaTimeoutTime: Milliseconds, msgBody: String)(using
          Encoder[SFNArguments]
      ): F[Unit] = {
        def log = logWithReason(sourceId)

        lazy val groupExpiryTime: Milliseconds = lambdaTimeoutTime + config.maxSecondaryBatchingWindow.toMilliSeconds
        Async[F]
          .fromEither(decode[Input](msgBody))
          .flatMap { msgInput =>
            atomicCell.evalUpdateAndGet { groupCache =>
              val groupF = groupCache.get(sourceId) match
                case None                        => log(NoExistingGroup) >> startNewGroup(config, groupExpiryTime, msgInput.id, msgBody)
                case Some(currentGroupForSource) =>
                  if currentGroupForSource.expires.toEpochMilli <= lambdaTimeoutTime.length then
                    log(ExpiryBeforeLambdaTimeout) >> startNewGroup(config, groupExpiryTime, msgInput.id, msgBody)
                  else if currentGroupForSource.itemCount > config.maxBatchSize then log(MaxGroupSizeExceeded) >> startNewGroup(config, groupExpiryTime, msgInput.id, msgBody)
                  else
                    writeToLockTable(msgInput.id, msgBody, config, currentGroupForSource.groupId) >>
                      Async[F].pure(currentGroupForSource.copy(itemCount = currentGroupForSource.itemCount + 1))
              groupF.map(group => groupCache + (sourceId -> group))
            }
          }
          .void
      }

      override def aggregate(config: Config, atomicCell: AtomicCell[F, Map[String, Group]], messages: List[SQSMessage], remainingTimeInMillis: Int)(using
          Encoder[SFNArguments],
          Decoder[Input]
      ): F[List[BatchItemFailure]] = {
        val lambdaTimeoutTime = (Generators().generateInstant.toEpochMilli + remainingTimeInMillis).milliSeconds
        for {
          fibers <- messages.parTraverse { message =>
            val process = for {
              _ <- updateOrCreateNewGroup(atomicCell, config, message.getEventSourceArn, lambdaTimeoutTime, message.getBody)
            } yield message.getMessageId
            process.handleErrorWith { err =>
              logger.error(Map(), err)(err.getMessage) >>
                Async[F].raiseError(AggregatorError(message.getMessageId))
            }.start
          }
          results <- fibers.traverse(_.join)
          _ <- logger.info(Map("successes" -> results.count(_.isSuccess).toString, "failures" -> results.count(_.isError).toString))("Aggregation complete")
          response <- handleErrors(results)
        } yield response
      }

      private def handleErrors(results: List[Outcome[F, Throwable, String]]): F[List[BatchItemFailure]] = {
        results
          .traverse {
            case Outcome.Errored(e) =>
              e match
                case AggregatorError(messageId) => BatchItemFailure.builder().withItemIdentifier(messageId).build.some.pure[F]
                case _ => Async[F].raiseError(new RuntimeException("Unexpected error", e)) // Should never get here but the compiler complains if I don't check.
            case _ => none[BatchItemFailure].pure[F]
          }
          .map(_.flatten)
      }
