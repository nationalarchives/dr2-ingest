package uk.gov.nationalarchives.postingeststatechangehandler

import cats.effect.{IO, Ref}
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import io.circe.syntax.*
import io.circe.{Decoder, Encoder}
import org.scanamo.request.RequestCondition
import org.scanamo.{DynamoFormat, DynamoReadError, DynamoValue, MissingProperty}
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.sns.model.PublishBatchResponse
import software.amazon.awssdk.services.sqs.model.{ChangeMessageVisibilityResponse, DeleteMessageResponse, GetQueueAttributesResponse, QueueAttributeName, SendMessageResponse}
import uk.gov.nationalarchives.DADynamoDBClient.{DADynamoDbRequest, DADynamoDbWriteItemRequest}
import uk.gov.nationalarchives.DASQSClient.FifoQueueConfiguration
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{PostIngestStatePartitionKey, PostIngestStatePrimaryKey, PostIngestStateSortKey, PostIngestStateTableItem}
import uk.gov.nationalarchives.utils.ExternalUtils.OutputMessage
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient, DASQSClient}

import scala.concurrent.duration.Duration

object Utils {

  def getPrimaryKey(item: PostIngestStateTableItem): PostIngestStatePrimaryKey =
    PostIngestStatePrimaryKey(PostIngestStatePartitionKey(item.assetId), PostIngestStateSortKey(item.batchId))

  def createSnsClient(ref: Ref[IO, List[OutputMessage]]): DASNSClient[IO] = new DASNSClient[IO]() {
    override def publish[T <: Product](topicArn: String)(messages: List[T])(using enc: Encoder[T]): IO[List[PublishBatchResponse]] = ref
      .update { messageList =>
        messageList ++ messages.map(_.asInstanceOf[OutputMessage])
      }
      .map(_ => Nil)
  }

  def createSqsClient(sqsMessagesRef: Ref[IO, Map[String, List[SQSMessage]]]): DASQSClient[IO] = new DASQSClient[IO]:
    override def sendMessage[T <: Product](
        queueUrl: String
    )(message: T, potentialFifoQueueConfiguration: Option[FifoQueueConfiguration], delaySeconds: Int)(using enc: Encoder[T]): IO[SendMessageResponse] = {
      sqsMessagesRef
        .update { messagesMap =>
          val newMessage = new SQSMessage()
          newMessage.setBody(message.asJson.noSpaces)
          if messagesMap.contains(queueUrl) then messagesMap.updated(queueUrl, newMessage :: messagesMap(queueUrl))
          else messagesMap + (queueUrl -> List(newMessage))
        }
        .map(_ => SendMessageResponse.builder.build)
    }

    override def receiveMessages[T](queueUrl: String, maxNumberOfMessages: Int)(using dec: Decoder[T]): IO[List[DASQSClient.MessageResponse[T]]] = IO.pure(Nil)

    override def deleteMessage(queueUrl: String, receiptHandle: String): IO[DeleteMessageResponse] = IO.pure(DeleteMessageResponse.builder.build)

    override def getQueueAttributes(queueUrl: String, attributeNames: List[QueueAttributeName]): IO[GetQueueAttributesResponse] = IO.raiseError(new Exception("Not implemented"))

    override def changeVisibilityTimeout(queueUrl: String)(receiptHandle: String, timeout: Duration): IO[ChangeMessageVisibilityResponse] = IO.stub

  def createDynamoClient(itemsInTableRef: Ref[IO, List[PostIngestStateTableItem]], updateRequestsRef: Ref[IO, List[DADynamoDbRequest]]): DADynamoDBClient[IO] = {
    new DADynamoDBClient[IO]():

      given DynamoFormat[String] = new DynamoFormat[String]:
        override def read(av: DynamoValue): Either[DynamoReadError, String] = av.asString.toRight(MissingProperty)

        override def write(t: String): DynamoValue = DynamoValue.fromString(t)

      override def writeItem(dynamoDbWriteRequest: DADynamoDbWriteItemRequest): IO[Int] = IO.pure(1)

      override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.pure(Nil)

      override def deleteItems[T](tableName: String, primaryKeys: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = {
        itemsInTableRef
          .update { items =>
            items.filterNot(item => primaryKeys.contains(getPrimaryKey(item)))
          }
          .map(_ => Nil)
      }

      override def updateAttributeValues(dynamoDbRequest: DADynamoDbRequest): IO[Int] =
        updateRequestsRef.update(updateRequests => dynamoDbRequest :: updateRequests).flatMap { _ =>
          itemsInTableRef
            .update { existing =>
              val assetIdToUpdate = dynamoDbRequest.primaryKeyAndItsValue.get("assetId").map(_.s()).getOrElse("")
              existing.map { eachItem =>
                if eachItem.assetId.toString == assetIdToUpdate then
                  val newFirstQueued = dynamoDbRequest.attributeNamesAndValuesToUpdate.get("firstQueued").map(_.s())
                  val newLastQueued = dynamoDbRequest.attributeNamesAndValuesToUpdate.get("lastQueued").map(_.s())
                  val newQueue = dynamoDbRequest.attributeNamesAndValuesToUpdate.get("queue").map(_.s())
                  eachItem.copy(potentialFirstQueued = newFirstQueued, potentialLastQueued = newLastQueued, potentialQueue = newQueue)
                else eachItem
              }
            }
            .map(_ => 1)
        }

      override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String] = None)(using
          returnTypeFormat: DynamoFormat[U]
      ): IO[List[U]] = IO.pure(Nil)

      override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = IO.pure(Nil)
  }
}
