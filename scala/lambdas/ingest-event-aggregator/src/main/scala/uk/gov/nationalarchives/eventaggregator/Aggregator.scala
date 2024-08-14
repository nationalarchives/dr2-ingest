package uk.gov.nationalarchives.eventaggregator

import cats.Parallel
import cats.effect.syntax.all.*
import cats.effect.{Async, Outcome, Ref}
import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.{Decoder, Encoder, Printer}
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jFactory
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbWriteItemRequest
import uk.gov.nationalarchives.eventaggregator.Aggregator.NewBatchReason.*
import uk.gov.nationalarchives.eventaggregator.Aggregator.{Input, SFNArguments}
import uk.gov.nationalarchives.eventaggregator.Lambda.{Batch, Config}
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient}

import java.net.URI
import java.time.Instant
import java.util.UUID

trait Aggregator[F[_]]:

  def aggregate(config: Config, ref: Ref[F, Map[String, Batch]], messages: List[SQSMessage], remainingTimeInMillis: Int)(using
      Encoder[SFNArguments],
      Decoder[Input]
  ): F[List[Outcome[F, Throwable, Unit]]]

object Aggregator:

  case class Input(id: UUID, location: URI)

  case class SFNArguments(groupId: String, batchId: String, waitFor: Long, retryCount: Int = 0)

  enum NewBatchReason:
    case NoExistingBatch, ExpiryBeforeLambdaTimeout, MaxBatchSizeExceeded

  def apply[F[_]: Async](using ev: Aggregator[F]): Aggregator[F] = ev

  given aggregator[F[_]: Async: Generators: DASFNClient: DADynamoDBClient: Parallel]: Aggregator[F] = new Aggregator[F]:
    private val logger: SelfAwareStructuredLogger[F] = Slf4jFactory.create[F].getLogger

    private def logWithReason(sourceId: String)(newBatchReason: NewBatchReason): F[Unit] =
      logger.info(Map("action" -> "Start new batch", "reason" -> newBatchReason.toString, "sourceId" -> "sourceId"))("Starting a new batch")

    private def toDynamoString(value: String): AttributeValue = AttributeValue.builder.s(value).build

    def writeToLockTable(input: Input, config: Config, batchId: String)(using dynamoClient: DADynamoDBClient[F]): F[Int] = {
      dynamoClient.writeItem(
        DADynamoDbWriteItemRequest(
          config.lockTable,
          Map("messageId" -> toDynamoString(input.id.toString), "batchId" -> toDynamoString(batchId), "message" -> toDynamoString(input.asJson.printWith(Printer.noSpaces)))
        )
      )
    }

    private def startNewBatch(ref: Ref[F, Map[String, Batch]], sourceId: String, config: Config, lambdaTimeoutTime: Long)(using
        sfnClient: DASFNClient[F],
        enc: Encoder[SFNArguments]
    ): F[String] = {
      val expires = lambdaTimeoutTime + (config.maxSecondaryBatchingWindow * 1000)
      val delay = Math.ceil((expires - Generators[F].generateInstant.toEpochMilli).toDouble / 1000).toInt
      val groupId = s"${config.sourceSystem}_${Generators[F].generateUuid}"
      val batchId = s"${groupId}_0"
      for {
        batchCache <- ref.update(batchCache => batchCache + (sourceId -> Batch(batchId, Instant.ofEpochMilli(expires), 1)))
        _ <- sfnClient.startExecution(config.sfnArn, SFNArguments(groupId, batchId, delay), batchId.some)
      } yield batchId
    }

    private def getBatchId(ref: Ref[F, Map[String, Batch]], config: Config, sourceId: String, lambdaTimeoutTime: Long)(using Encoder[SFNArguments]): F[String] = {
      def log = logWithReason(sourceId)
      ref.get.flatMap { batchCache =>
        val potentialCurrentBatchForSource = batchCache.get(sourceId)
        potentialCurrentBatchForSource match
          case None => log(NoExistingBatch) >> startNewBatch(ref, sourceId, config, lambdaTimeoutTime)
          case Some(currentBatchForSource) =>
            if lambdaTimeoutTime >= currentBatchForSource.expires.toEpochMilli then log(ExpiryBeforeLambdaTimeout) >> startNewBatch(ref, sourceId, config, lambdaTimeoutTime)
            else if currentBatchForSource.items > config.maxBatchSize then log(MaxBatchSizeExceeded) >> startNewBatch(ref, sourceId, config, lambdaTimeoutTime)
            else ref.update(_ + (sourceId -> currentBatchForSource.copy(items = currentBatchForSource.items + 1))) >> Async[F].pure(currentBatchForSource.batchId)
      }
    }

    override def aggregate(config: Config, ref: Ref[F, Map[String, Batch]], messages: List[SQSMessage], remainingTimeInMillis: Int)(using
        Encoder[SFNArguments],
        Decoder[Input]
    ): F[List[Outcome[F, Throwable, Unit]]] = {
      val lambdaTimeoutTime = Generators[F].generateInstant.toEpochMilli + remainingTimeInMillis
      for {
        fibers <- messages.parTraverse { record =>
          val process = for {
            batchId <- getBatchId(ref, config, record.getEventSourceArn, lambdaTimeoutTime)
            input <- Async[F].fromEither(decode[Input](record.getBody))
            _ <- writeToLockTable(input, config, batchId)
          } yield ()
          process.start
        }
        results <- fibers.traverse(_.join)
        _ <- logger.info(Map("success" -> results.count(_.isSuccess).toString, "failures" -> results.count(_.isError).toString))("Aggregation complete")
      } yield results
    }
