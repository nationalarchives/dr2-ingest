package uk.gov.nationalarchives.postingestresender

import cats.effect.IO
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
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.postingestresender.Lambda.{Config, Dependencies, QueueMessage}
import uk.gov.nationalarchives.utils.EventCodecs.given
import uk.gov.nationalarchives.utils.PostingestUtils.Queue
import uk.gov.nationalarchives.utils.{Generators, LambdaRunner}
import uk.gov.nationalarchives.{DADynamoDBClient, DASQSClient}

import java.time.Instant
import java.util.UUID

class Lambda extends LambdaRunner[ScheduledEvent, Unit, Config, Dependencies] {
  given Encoder[QueueMessage] = deriveEncoder[QueueMessage]

  override def dependencies(config: Config): IO[Dependencies] = IO(
    Dependencies(DADynamoDBClient[IO](), DASQSClient[IO](), () => Generators().generateInstant)
  )

  override def handler: (ScheduledEvent, Config, Dependencies) => IO[Unit] = { (triggerEvent, config, dependencies) =>
    val dynamoClient = dependencies.dynamoClient
    val sqsClient = dependencies.sqsClient

    def processQueues(queues: List[Queue]): IO[Unit] = {
      queues match {
        case Nil => IO.unit
        case _ =>
          resendMessages(queues.head)  >>
          processQueues(queues.tail)
      }
    }

    def resendMessages(queue: Queue): IO[Unit] = {
      for {
        queueAttributes <- dependencies.sqsClient.getQueueAttributes(queue.queueUrl, List(QueueAttributeName.MESSAGE_RETENTION_PERIOD))
        messageRetentionPeriod: Long = queueAttributes.attributes().get(QueueAttributeName.MESSAGE_RETENTION_PERIOD).toLong
        dateTimeNow = dependencies.instantGenerator()
        dateTimeCutOff = dateTimeNow.minusSeconds(messageRetentionPeriod)

        items <- dependencies.dynamoClient.queryItems[PostIngestStateTableItem](
          config.dynamoTableName,
          AndCondition("queue" === queue.queueAlias, "lastQueued" < dateTimeCutOff.toString), //no compiler error, but mock implementation complained
          Some("QueueLastQueuedIdx")
        )

        _ <-
          if (items.isEmpty) IO.unit
          else
            {
              logger.info(s"Resending ${items.size} items to queue ${queue.queueAlias}")
              items.parTraverse { item =>
                dependencies.sqsClient.sendMessage(queue.queueUrl)(QueueMessage(item.assetId, item.batchId, queue.resultAttrName, item.input)).handleErrorWith { error =>
                  logger.error(s"Failed to send message for assetId ${item.assetId} to queue ${queue.queueAlias}: ${error.getMessage}")
                  IO.raiseError(error)
                } >>
                  dependencies.dynamoClient
                    .updateAttributeValues(
                      DADynamoDbRequest(
                        config.dynamoTableName,
                        Map(assetId -> AttributeValue.builder().s(item.assetId.toString).build()),
                        Map(lastQueued -> Some(AttributeValue.builder().s(dateTimeNow.toString).build()))
                      )
                    )
                    .handleErrorWith { error =>
                      logger.error(s"Failed to update lastQueued for assetId ${item.assetId} and queue ${queue.queueAlias}: ${error.getMessage}")
                      IO.raiseError(error)
                    }
              }
            }.void
      } yield ()
    }

    val lambdaResult = for {
      listOfQueues <- IO.fromEither(decode[List[Queue]](config.queues)).map(_.sortBy(_.queueOrder))
      _ <- logger.info(s"Starting message resend for queues: ${listOfQueues.map(_.queueAlias).mkString(", ")}") >>
      processQueues(listOfQueues)
    } yield ()

    lambdaResult.handleErrorWith { error =>
      logger.error(s"Error processing scheduled event: ${error.getMessage}") >>
      IO.raiseError(error)
    }
  }
}

object Lambda {
  case class Config(dynamoTableName: String, dynamoGsiName: String, queues: String) derives ConfigReader
  case class Dependencies(dynamoClient: DADynamoDBClient[IO], sqsClient: DASQSClient[IO], instantGenerator: () => Instant)
  case class QueueMessage(assetId: UUID, batchId: String, resultAttributeName: String, payload: String)
}
