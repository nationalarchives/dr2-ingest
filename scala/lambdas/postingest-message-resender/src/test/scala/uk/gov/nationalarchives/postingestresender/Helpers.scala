package uk.gov.nationalarchives.postingestresender

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.amazonaws.services.lambda.runtime.events.{SQSEvent, ScheduledEvent}
import io.circe.syntax.*
import io.circe.{Decoder, Encoder}
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import scala.jdk.CollectionConverters.*
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.sqs.model.{DeleteMessageResponse, GetQueueAttributesResponse, QueueAttributeName, SendMessageResponse}
import uk.gov.nationalarchives.DADynamoDBClient.{DADynamoDbRequest, DADynamoDbWriteItemRequest}
import uk.gov.nationalarchives.DASQSClient.FifoQueueConfiguration
import uk.gov.nationalarchives.{DADynamoDBClient, DASQSClient}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.PostIngestStateTableItem
import uk.gov.nationalarchives.postingestresender.Lambda.{Config, Dependencies}

import java.time.Instant

object Helpers {

  def notImplemented[T]: IO[Nothing] = IO.raiseError(new Exception("Not implemented"))
  def testQueueUrl = "https://sqs.eu-west-2.amazonaws.com/123456789012/queue1.fifo"

  def runLambda(
      initialItemsInTable: List[PostIngestStateTableItem],
      event: ScheduledEvent,
      config: Config
  ): LambdaRunResults = {
    for {
      itemsInTableRef <- Ref[IO].of(initialItemsInTable)
      updateTableReqsRef <- Ref[IO].of[List[DADynamoDbRequest]](Nil)
      sqsMessagesRef <- Ref[IO].of[Map[String, List[SQSMessage]]](Map(testQueueUrl -> Nil))
      _ <- new Lambda().handler(
        event,
        config,
        Dependencies(dynamoClient(itemsInTableRef), sqsClient(sqsMessagesRef), () => Instant.now())
      )
      itemsRemainingInTable <- itemsInTableRef.get
      updateTableReqs <- updateTableReqsRef.get
      sqsMessages <- sqsMessagesRef.get
    } yield LambdaRunResults(itemsRemainingInTable, updateTableReqs, sqsMessages)
  }.unsafeRunSync()

  case class LambdaRunResults(
      finalItemsInTable: List[PostIngestStateTableItem],
      updateTableReqs: List[DADynamoDbRequest],
      sentSqsMessages: Map[String, List[SQSMessage]]
  )

  def sqsClient(sqsMessageRef: Ref[IO, Map[String, List[SQSEvent.SQSMessage]]]): DASQSClient[IO] = new DASQSClient[IO]:
    override def receiveMessages[T](queueUrl: String, maxNumberOfMessages: Int)(using dec: Decoder[T]): IO[List[DASQSClient.MessageResponse[T]]] = notImplemented
    override def deleteMessage(queueUrl: String, receiptHandle: String): IO[DeleteMessageResponse] = notImplemented
    override def getQueueAttributes(queueUrl: String, attributeNames: List[QueueAttributeName]): IO[GetQueueAttributesResponse] = {
        if (queueUrl == testQueueUrl) {
          IO.pure(
            GetQueueAttributesResponse.builder()
              .attributes(Map(QueueAttributeName.MESSAGE_RETENTION_PERIOD -> "345600").asJava)
              .build()
          )
        } else {
          IO.raiseError(new RuntimeException("Invalid queue URL"))
        }
      }


    override def sendMessage[T <: Product](
        queueUrl: String
    )(message: T, potentialFifoQueueConfiguration: Option[FifoQueueConfiguration], delaySeconds: Int)(using enc: Encoder[T]): IO[SendMessageResponse] = {
      sqsMessageRef
        .update { messagesMap =>
          val newMessage = new SQSMessage()
          newMessage.setBody(message.asJson.noSpaces)
          messagesMap.map {
            case (queue, messages) if queue == queueUrl => queue -> (newMessage :: messages)
            case (queue, messages)                      => queue -> messages
          }
        }
        .map(_ => SendMessageResponse.builder.build)
    }

  def dynamoClient(ref: Ref[IO, List[PostIngestStateTableItem]]): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:

    override def writeItem(dynamoDbWriteRequest: DADynamoDbWriteItemRequest): IO[Int] = notImplemented
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented
    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented
    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = notImplemented
    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = notImplemented

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      ref.get.map { existingItems =>
        (for {
          // need to return values based on 2 conditions
          values <- Option(requestCondition.attributes.values)
          map <- values.toMap[String].toOption
        } yield existingItems
          .sortBy(_.potentialLastQueued)
          .map(_.asInstanceOf[U])).getOrElse(Nil)
      }
}
