package uk.gov.nationalarchives.postingestresender

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.jawn.decode
import org.scanamo.query.AndCondition
import org.scanamo.syntax.*
import pureconfig.ConfigReader
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.sqs.model.QueueAttributeName
import uk.gov.nationalarchives.DADynamoDBClient.{DADynamoDbRequest, given}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{postIngestStatePkFormat, *}
import uk.gov.nationalarchives.postingestresender.Lambda.{Config, Dependencies, QueueMessage}
import uk.gov.nationalarchives.utils.EventCodecs.given
import uk.gov.nationalarchives.utils.PostingestUtils.Queue
import uk.gov.nationalarchives.utils.{Generators, LambdaRunner}
import uk.gov.nationalarchives.{DADynamoDBClient, DASQSClient}

import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*

class Lambda extends LambdaRunner[ScheduledEvent, Unit, Config, Dependencies] {
  given Encoder[QueueMessage] = deriveEncoder[QueueMessage]

  override def dependencies(config: Config): IO[Dependencies] = IO(
    Dependencies(DADynamoDBClient[IO](), DASQSClient[IO](), () => Generators().generateInstant)
  )

  override def handler: (ScheduledEvent, Config, Dependencies) => IO[Unit] = { (triggerEvent, config, dependencies) =>

    /** This function receives a queue as a parameter. For the particular queue, it retrieves the details from the post ingest table, it also retrieves the 'Message Retention
      * Period' for this queue. If any of the items in the post ingest table has a 'lastQueued' time before the message retention period, it resends the message to the queue and
      * updates the 'lastQueued' value for this item in post ingest table with the current datetime.
      */

    def resendExpiredMessages(queue: Queue): IO[Unit] = for {
      queueAttributes <- dependencies.sqsClient.getQueueAttributes(queue.queueUrl, List(QueueAttributeName.MESSAGE_RETENTION_PERIOD))
      messageRetentionPeriod: Long = queueAttributes.attributes().get(QueueAttributeName.MESSAGE_RETENTION_PERIOD).toLong
      dateTimeNow: Instant = dependencies.instantGenerator()
      dateTimeCutOff: Instant = dateTimeNow.minusSeconds(messageRetentionPeriod)

      expiredItems <- dependencies.dynamoClient.queryItems[PostIngestStateTableItem](
        config.stateTableName,
        AndCondition(DynamoFormatters.queue === queue.queueAlias, lastQueued < dateTimeCutOff.toString),
        Some(config.stateGsiName)
      )
      semaphore <- Semaphore[IO](50)
      _ <- IO.whenA(expiredItems.nonEmpty) { logger.info(s"Resending ${expiredItems.size} items to '${queue.queueAlias}' queue") }
      _ <- expiredItems
        .parTraverse { item =>
          sendToQueue(item, queue, semaphore) >>
            updateLastQueued(item, queue, dateTimeNow, semaphore)
        }
    } yield ()

    def sendToQueue(item: PostIngestStateTableItem, queue: Queue, semaphore: Semaphore[IO]): IO[Unit] =
      semaphore.acquire >> dependencies.sqsClient
        .sendMessage(queue.queueUrl)(QueueMessage(item.assetId, item.batchId, queue.resultAttrName, item.input))
        .handleErrorWith { error =>
          logger.error(s"""Failed to send message for assetId '${item.assetId}' to '${queue.queueAlias}' queue:
                      |${error.getMessage}""".stripMargin) >>
            semaphore.release >> IO.raiseError(error)
        } >> semaphore.release

    def updateLastQueued(item: PostIngestStateTableItem, queue: Queue, newDateTime: Instant, semaphore: Semaphore[IO]): IO[Unit] = {
      val postIngestPk = PostIngestStatePrimaryKey(PostIngestStatePartitionKey(item.assetId), PostIngestStateSortKey(item.batchId))
      semaphore.acquire >> dependencies.dynamoClient
        .updateAttributeValues(
          DADynamoDbRequest(
            config.stateTableName,
            postIngestStatePkFormat.write(postIngestPk).toAttributeValue.m().asScala.toMap,
            Map(lastQueued -> AttributeValue.builder().s(newDateTime.toString).build())
          )
        )
        .handleErrorWith { error =>
          logger.error(s"""Failed to update 'lastQueued' timestamp for assetId '${item.assetId}' of '${queue.queueAlias}' queue:
                          |${error.getMessage}""".stripMargin) >>
            semaphore.release >> IO.raiseError(error)
        } >> semaphore.release

    }

    (for {
      orderedQueues <- IO.fromEither(decode[List[Queue]](config.queues)).map(_.sortBy(_.queueOrder))
      _ <- logger.info(s"Starting message resender for queues: ${orderedQueues.map(_.queueAlias).mkString(", ")}")
      _ <- orderedQueues.traverse(resendExpiredMessages)
    } yield ()).handleErrorWith { error =>
      logger.error(s"Error processing scheduled event: ${error.getMessage}") >>
        IO.raiseError(error)
    }
  }
}

object Lambda {
  case class Config(stateTableName: String, stateGsiName: String, queues: String) derives ConfigReader
  case class Dependencies(dynamoClient: DADynamoDBClient[IO], sqsClient: DASQSClient[IO], instantGenerator: () => Instant)
  case class QueueMessage(assetId: UUID, batchId: String, resultAttributeName: String, payload: String)
}
