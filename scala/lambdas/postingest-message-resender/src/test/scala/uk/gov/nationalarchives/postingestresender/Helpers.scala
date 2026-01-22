package uk.gov.nationalarchives.postingestresender

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.amazonaws.services.lambda.runtime.events.{SQSEvent, ScheduledEvent}
import io.circe.syntax.*
import io.circe.{Decoder, Encoder}
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.sqs.model.{ChangeMessageVisibilityResponse, DeleteMessageResponse, GetQueueAttributesResponse, QueueAttributeName, SendMessageResponse}
import uk.gov.nationalarchives.DADynamoDBClient.{DADynamoDbRequest, DADynamoDbWriteItemRequest}
import uk.gov.nationalarchives.DASQSClient.FifoQueueConfiguration
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{PostIngestStateTableItem, assetId, lastQueued}
import uk.gov.nationalarchives.postingestresender.Lambda.{Config, Dependencies}
import uk.gov.nationalarchives.{DADynamoDBClient, DASQSClient}

import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.UUID
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*

object Helpers {

  case class Errors(
      getQueueAttributes: Boolean = false,
      sendMessage: Boolean = false,
      queryItems: Boolean = false,
      updateAttributeValues: Boolean = false
  )

  extension (errors: Option[Errors]) def raise(fn: Errors => Boolean, errorMessage: String): IO[Unit] = IO.raiseWhen(errors.exists(fn))(new Exception(errorMessage))

  def notImplemented[T]: IO[Nothing] = IO.raiseError(new Exception("Not implemented"))
  def testQueueUrl = "https://sqs.eu-west-2.amazonaws.com/123456789012/queue1.fifo"
  def predictableStartOfTheDay: () => Instant = () => LocalDate.now(ZoneOffset.UTC).atStartOfDay(ZoneOffset.UTC).toInstant

  def runLambda(
      initialItemsInTable: List[PostIngestStateTableItem],
      event: ScheduledEvent,
      config: Config,
      instantGenerator: () => Instant = () => Instant.now(),
      runtimeErrors: Option[Errors] = Option(Errors())
  ): LambdaRunResults = {
    for {
      itemsInTableRef <- Ref[IO].of(initialItemsInTable)
      updateTableReqsRef <- Ref[IO].of[List[DADynamoDbRequest]](Nil)
      sqsMessagesRef <- Ref[IO].of[Map[String, List[SQSMessage]]](Map(testQueueUrl -> Nil))
      result <- new Lambda()
        .handler(
          event,
          config,
          Dependencies(dynamoClient(itemsInTableRef, runtimeErrors), sqsClient(sqsMessagesRef, runtimeErrors), instantGenerator)
        )
        .attempt
      itemsRemainingInTable <- itemsInTableRef.get
      updateTableReqs <- updateTableReqsRef.get
      sqsMessages <- sqsMessagesRef.get
    } yield LambdaRunResults(result, itemsRemainingInTable, updateTableReqs, sqsMessages)
  }.unsafeRunSync()

  case class LambdaRunResults(
      result: Either[Throwable, Unit],
      finalItemsInTable: List[PostIngestStateTableItem],
      updateTableReqs: List[DADynamoDbRequest],
      sentSqsMessages: Map[String, List[SQSMessage]]
  )

  def sqsClient(sqsMessageRef: Ref[IO, Map[String, List[SQSEvent.SQSMessage]]], errors: Option[Errors]): DASQSClient[IO] = new DASQSClient[IO]:
    override def receiveMessages[T](queueUrl: String, maxNumberOfMessages: Int)(using dec: Decoder[T]): IO[List[DASQSClient.MessageResponse[T]]] = notImplemented
    override def deleteMessage(queueUrl: String, receiptHandle: String): IO[DeleteMessageResponse] = notImplemented
    override def getQueueAttributes(queueUrl: String, attributeNames: List[QueueAttributeName]): IO[GetQueueAttributesResponse] =
      errors.raise(_.getQueueAttributes, "Unable to retrieve queue attributes") >> {
        val retentionSeconds = if queueUrl == testQueueUrl then "345600" else "200"
        IO.pure(
          GetQueueAttributesResponse
            .builder()
            .attributes(Map(QueueAttributeName.MESSAGE_RETENTION_PERIOD -> retentionSeconds).asJava)
            .build()
        )
      }

    override def sendMessage[T <: Product](
        queueUrl: String
    )(message: T, potentialFifoQueueConfiguration: Option[FifoQueueConfiguration], delaySeconds: Int)(using enc: Encoder[T]): IO[SendMessageResponse] =
      errors.raise(_.sendMessage, "Unable to send message to queue") >>
        sqsMessageRef
          .update: messagesMap =>
            val newMessage = new SQSMessage()
            newMessage.setBody(message.asJson.noSpaces)
            messagesMap.map:
              case (queue, messages) if queue == queueUrl => queue -> (newMessage :: messages)
              case (queue, messages)                      => queue -> messages
          .map(_ => SendMessageResponse.builder.build)

  def dynamoClient(ref: Ref[IO, List[PostIngestStateTableItem]], errors: Option[Errors]): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:

    override def writeItem(dynamoDbWriteRequest: DADynamoDbWriteItemRequest): IO[Int] = notImplemented
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented
    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented
    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = notImplemented
    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = {
      errors.raise(_.updateAttributeValues, "Unable to update attribute values in the table") >> {
        val newLastQueued = dynamoDbRequest.attributeNamesAndValuesToUpdate.get(lastQueued).map(_.s())
        val assetIdToUpdate = UUID.fromString(dynamoDbRequest.primaryKeyAndItsValue.get(assetId).map(_.s()).get)
        ref
          .update: existingItems =>
            existingItems.map: item =>
              if item.assetId.equals(assetIdToUpdate) then item.copy(potentialLastQueued = newLastQueued) else item
          .map(_ => 1)
      }
    }

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      errors.raise(_.queryItems, "Unable to query items from the table") >>
        ref.get.map: existingItems =>
          (for
            values <- Option(requestCondition.attributes.values)
            queryConditionsMap <- values.toMap[String].toOption
          yield existingItems
            .filter: i =>
              i.potentialLastQueued.exists(_ < queryConditionsMap("conditionAttributeValue1")) &&
                i.potentialQueue.contains(queryConditionsMap("conditionAttributeValue0"))
            .map(_.asInstanceOf[U]))
            .getOrElse(Nil)

}
