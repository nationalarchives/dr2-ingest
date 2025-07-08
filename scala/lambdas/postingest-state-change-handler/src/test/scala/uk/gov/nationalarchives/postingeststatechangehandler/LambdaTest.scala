package uk.gov.nationalarchives.postingeststatechangehandler

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.implicits.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.TableDrivenPropertyChecks
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbRequest
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.postingeststatechangehandler.Lambda.*
import uk.gov.nationalarchives.postingeststatechangehandler.Utils.*
import uk.gov.nationalarchives.utils.ExternalUtils.MessageStatus.{IngestedCCDisk, IngestedPreservation}
import uk.gov.nationalarchives.utils.ExternalUtils.MessageType.{IngestComplete, IngestUpdate}
import uk.gov.nationalarchives.utils.ExternalUtils.OutputMessage

import java.time.Instant
import java.util.UUID

class LambdaTest extends AnyFlatSpec with TableDrivenPropertyChecks with EitherValues {
  private val queue1 = "CC"
  val instant: Instant = Instant.ofEpochSecond(2147483647)
  val messageId: UUID = UUID.fromString("de20e39a-948e-468d-a89d-338af262e0f5")
  private val dateTime = Some("2038-01-19T15:14:07.000Z")
  private val nonPostIngestedAsset = PostIngestStateTableItem(UUID.randomUUID, "batchId", "input", Some("correlationId"), None, None, None, None)
  private val fullyPostIngestedAsset =
    PostIngestStateTableItem(UUID.randomUUID, "batchId", "input", Some("correlationId"), Some(queue1), dateTime, dateTime, Some(s"result_$queue1"))
  private val queue1Url = "https://queueUrl1.com"
  private def getConfig(queue: String = queue1) =
    Config("ddbTable", "ddbGsi", "topicArn", s"""[{"queueAlias": "$queue", "queueOrder": 1, "queueUrl": "$queue1Url"}]""")

  private val updateTableScenarios = Table( // Should be updated as we add more checks
    ("OldImage", "NewImage", "Description"),
    (Some(nonPostIngestedAsset), fullyPostIngestedAsset, "OldImage hasn't gone through any post Ingest steps and NewImage has a result for the CC step")
  )

  private def runLambda(
      itemsInTable: List[PostIngestStateTableItem],
      event: DynamodbEvent,
      config: Config = getConfig()
  ): IO[UpdatedRefs] = {
    for {
      itemsInTableRef <- Ref[IO].of(itemsInTable)
      updateTableReqsRef <- Ref[IO].of[List[DADynamoDbRequest]](Nil)
      sqsMessagesRef <- Ref[IO].of[Map[String, List[SQSMessage]]](Map(queue1Url -> Nil))
      snsMessagesRef <- Ref[IO].of[List[OutputMessage]](Nil)
      _ <- new Lambda().handler(
        event,
        config,
        Dependencies(createDynamoClient(itemsInTableRef, updateTableReqsRef), createSnsClient(snsMessagesRef), createSqsClient(sqsMessagesRef), () => instant, () => messageId)
      )
      itemsRemainingInTable <- itemsInTableRef.get
      updateTableReqs <- updateTableReqsRef.get
      sqsMessages <- sqsMessagesRef.get
      snsMessages <- snsMessagesRef.get
    } yield UpdatedRefs(itemsRemainingInTable, updateTableReqs, sqsMessages, snsMessages)
  }

  "handler" should "queue the item and send an SQS message to the correct queue if the event is an 'INSERT' one and NewImage exists" in {
    val oldDynamoItem = None
    val newDynamoItem = fullyPostIngestedAsset
    val event = DynamodbEvent(List(DynamodbStreamRecord(EventName.INSERT, StreamRecord(getPrimaryKey(newDynamoItem).some, oldDynamoItem, newDynamoItem))))
    val refs = runLambda(Nil, event).unsafeRunSync()

    refs.itemsRemainingInTable.size should equal(0)

    refs.updateTableReqs.size should equal(1)
    val updateTableReq = refs.updateTableReqs.head

    updateTableReq.tableName should equal("ddbTable")
    updateTableReq.primaryKeyAndItsValue should equal(Map("assetId" -> fromS(newDynamoItem.assetId.toString), "batchId" -> fromS(newDynamoItem.batchId)))
    updateTableReq.attributeNamesAndValuesToUpdate("queue") should equal(Some(fromS("CC")))
    updateTableReq.attributeNamesAndValuesToUpdate("firstQueued") should equal(Some(fromS("2038-01-19T03:14:07Z")))
    updateTableReq.attributeNamesAndValuesToUpdate("lastQueued") should equal(Some(fromS("2038-01-19T03:14:07Z")))

    refs.sentSqsMessages.size should equal(1)
    val (queueUrl, messagesPerQueue) = refs.sentSqsMessages.head

    queueUrl should equal(queue1Url)
    messagesPerQueue.head.getBody should equal(s"""{"assetId":"${fullyPostIngestedAsset.assetId}","batchId":"batchId","resultAttrName":"result_CC","payload":"input"}""")

    refs.sentSnsMessages.size should equal(1)
    val sentSnsMessage = refs.sentSnsMessages.head

    sentSnsMessage.properties.executionId should equal("batchId")
    sentSnsMessage.properties.messageId should equal(UUID.fromString("de20e39a-948e-468d-a89d-338af262e0f5"))
    sentSnsMessage.properties.parentMessageId should equal(Some(correlationId))
    sentSnsMessage.properties.timestamp should equal(Instant.parse("2038-01-19T03:14:07Z"))
    sentSnsMessage.properties.messageType should equal(IngestUpdate)

    sentSnsMessage.parameters.assetId should equal(fullyPostIngestedAsset.assetId)
    sentSnsMessage.parameters.status should equal(IngestedPreservation)
  }

  "handler" should s"delete the item if the event is a 'MODIFY' one, OldImage hasn't gone through any postIngest steps and NewImage has a result for the CC step" in {
    val oldImage = Some(nonPostIngestedAsset)
    val newImage = fullyPostIngestedAsset
    val additionalItemInTable =
      PostIngestStateTableItem(UUID.fromString("e5c55836-3917-405d-8bde-a1d970136c1d"), "batchId2", "input2", Some("correlationId2"), None, None, None, None)
    val event = DynamodbEvent(List(DynamodbStreamRecord(EventName.MODIFY, StreamRecord(getPrimaryKey(newImage).some, oldImage, newImage))))
    val refs = runLambda(List(newImage, additionalItemInTable), event).unsafeRunSync()

    refs.itemsRemainingInTable.size should equal(1)
    val itemInTable = refs.itemsRemainingInTable.head

    itemInTable.assetId.toString should equal("e5c55836-3917-405d-8bde-a1d970136c1d")
    itemInTable.batchId should equal("batchId2")
    itemInTable.potentialQueue should equal(None)
    itemInTable.potentialFirstQueued should equal(None)
    itemInTable.potentialLastQueued should equal(None)

    refs.updateTableReqs.size should equal(0)

    refs.sentSnsMessages.size should equal(1)
    val sentSnsMessage = refs.sentSnsMessages.head

    sentSnsMessage.properties.executionId should equal("batchId")
    sentSnsMessage.properties.messageId should equal(UUID.fromString("de20e39a-948e-468d-a89d-338af262e0f5"))
    sentSnsMessage.properties.parentMessageId should equal(Some(correlationId))
    sentSnsMessage.properties.timestamp should equal(Instant.parse("2038-01-19T03:14:07Z"))
    sentSnsMessage.properties.messageType should equal(IngestComplete)

    sentSnsMessage.parameters.assetId should equal(fullyPostIngestedAsset.assetId)
    sentSnsMessage.parameters.status should equal(IngestedCCDisk)
  }

  "handler" should s"throw an error if the queues share any of the same values for their properties" in {
    val oldDynamoItem = Some(fullyPostIngestedAsset)
    val newDynamoItem = fullyPostIngestedAsset
    val event = DynamodbEvent(List(DynamodbStreamRecord(EventName.MODIFY, StreamRecord(getPrimaryKey(newDynamoItem).some, oldDynamoItem, newDynamoItem))))

    val duplicatedQueues =
      s"""[{"queueAlias": "CC", "queueOrder": 1, "queueUrl": "$queue1Url"},""" +
        s"""{"queueAlias": "CC", "queueOrder": 1, "queueUrl": "$queue1Url"},""" +
        s"""{"queueAlias": "CC", "queueOrder": 1, "queueUrl": "$queue1Url"}]"""

    val config = Config("ddbTable", "ddbGsi", "topicArn", duplicatedQueues)

    val ex = intercept[Exception] {
      runLambda(Nil, event, config).unsafeRunSync()
    }
    ex.getMessage should equal(
      "The values in each queue should be unique but there is more than 1 queue with:\n" +
        "Property: queueOrder, Value: 1\nProperty: queueUrl, Value: https://queueUrl1.com\nProperty: queueAlias, Value: CC"
    )
  }

  "handler" should s"throw an error if a queue alias could not be found in the 'queueAliasAndResultAttr' map" in {
    val oldDynamoItem = Some(fullyPostIngestedAsset)
    val newDynamoItem = fullyPostIngestedAsset
    val event = DynamodbEvent(List(DynamodbStreamRecord(EventName.MODIFY, StreamRecord(getPrimaryKey(newDynamoItem).some, oldDynamoItem, newDynamoItem))))
    val ex = intercept[MatchError] {
      runLambda(Nil, event, getConfig("UnexpectedQueueAlias")).unsafeRunSync()
    }
    ex.getMessage should equal("UnexpectedQueueAlias (of class java.lang.String)")
  }

  "handler" should s"throw an error if the event is a 'MODIFY' one, NewImage is present but no OldImage" in {
    val oldDynamoItem = None
    val newDynamoItem = fullyPostIngestedAsset
    val event = DynamodbEvent(List(DynamodbStreamRecord(EventName.MODIFY, StreamRecord(getPrimaryKey(newDynamoItem).some, oldDynamoItem, newDynamoItem))))
    val ex = intercept[Exception] {
      runLambda(Nil, event).unsafeRunSync()
    }
    ex.getMessage should equal("MODIFY Event was triggered but either an OldImage, NewImage or both don't exist")
  }
}

case class UpdatedRefs(
    itemsRemainingInTable: List[PostIngestStateTableItem],
    updateTableReqs: List[DADynamoDbRequest],
    sentSqsMessages: Map[String, List[SQSMessage]],
    sentSnsMessages: List[OutputMessage]
)
