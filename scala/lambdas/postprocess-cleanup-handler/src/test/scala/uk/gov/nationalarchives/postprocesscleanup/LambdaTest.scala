package uk.gov.nationalarchives.postprocesscleanup

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.AssetDynamoItem
import uk.gov.nationalarchives.postprocesscleanup.Helper.*

import java.time.{LocalDate, ZoneOffset}
import java.util.UUID
import scala.jdk.CollectionConverters.*

class LambdaTest extends AnyFlatSpec with EitherValues:
  val tomorrow: LocalDate = LocalDate.now(ZoneOffset.UTC).plusDays(1)

  "lambda handler" should "update ttl value of the current asset to be tomorrow" in {
    val id = UUID.randomUUID
    val initialAssetItem = createDynamoItem(id, "file1.txt", "s3://some-bucket/some-key", "parent1/parent2", DynamoFormatters.Type.Asset)
    val message = new SQSMessage()
    message.setBody(
      s"""{"parameters": {"assetId": "$id", "status": "Asset has been written to custodial copy disk."}, "properties": {"executionId": "some_batchId", "messageType": "preserve.digital.asset.ingest.complete"}}"""
    )
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(List(message).asJava)
    val result = runLambda(sqsEvent, List(initialAssetItem), Map.empty)
    result.result.isRight should equal(true)
    result.finalItemsInTable.size should equal(1)
    toDate(result.finalItemsInTable.head.ttl) shouldBe tomorrow
    result.finalsObjectsInS3.size shouldBe 0
  }

  "lambda handler" should "update ttl value for all items in the hierarchy of assetId sent in the message" in {
    val message = new SQSMessage()
    message.setBody(
      s"""{"parameters": {"assetId": "d5c74859-b4fa-403e-a11c-0c7652265f03", "status": "Asset has been written to custodial copy disk."}, "properties": {"executionId": "some_batchId", "messageType": "preserve.digital.asset.ingest.complete"}}"""
    )
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(List(message).asJava)
    val result = runLambda(sqsEvent, createInitialData.dynamoItems, createInitialData.s3Objects)
    result.result.isLeft should equal(false)
    result.finalItemsInTable.size should equal(6)
    all(result.finalItemsInTable.map(item => toDate(item.ttl))) shouldBe tomorrow

    val finalObjectsInS3 = result.finalsObjectsInS3
    finalObjectsInS3.size shouldBe 2
    all(finalObjectsInS3.values.map(_.keySet.contains("TO_BE_DELETED"))) shouldBe true
  }

  "lambda handler" should "not update ttl value for any item that is not in the hierarchy of assetId sent in the message" in {
    val message = new SQSMessage()
    message.setBody(
      s"""{"parameters": {"assetId": "3cc1cbed-c4fc-49c4-b09e-b80cb4e0c9ce", "status": "Asset has been written to custodial copy disk."}, "properties": {"executionId": "some_batchId", "messageType": "preserve.digital.asset.ingest.complete"}}"""
    )

    val oneItem = createDynamoItem(
      UUID.fromString("3cc1cbed-c4fc-49c4-b09e-b80cb4e0c9ce"),
      "3cc1cbed-c4fc-49c4-b09e-b80cb4e0c9ce",
      "s3://some-bucket/some-key",
      "some-other-parent",
      DynamoFormatters.Type.Asset
    )
    val allInitialItems = oneItem :: createInitialData.dynamoItems
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(List(message).asJava)
    val result = runLambda(sqsEvent, allInitialItems, createInitialData.s3Objects)
    result.result.isLeft should equal(false)
    result.finalItemsInTable.size should equal(7)
    val itemWithIdFromMessage = result.finalItemsInTable.find(_.id.toString == "3cc1cbed-c4fc-49c4-b09e-b80cb4e0c9ce")
    toDate(itemWithIdFromMessage.get.ttl) shouldBe tomorrow

    val otherItems = result.finalItemsInTable.filterNot(_.id.toString == "3cc1cbed-c4fc-49c4-b09e-b80cb4e0c9ce")
    all(otherItems.map(item => item.ttl)) shouldBe 1779382126L

    result.finalsObjectsInS3.values.forall(!_.contains("TO_BE_DELETED")) shouldBe true
  }

  "lambda handler" should "throw an exception when it cannot decode the sqs message" in {
    val id = UUID.randomUUID
    val initialFileItem = createDynamoItem(id, "file1.txt", "s3://some-bucket/some-key", "parent1/parent2", DynamoFormatters.Type.Asset)
    val message = new SQSMessage()
    message.setBody(
      s"""{"parameters": {"asset_Id": "$id", "status": "Asset has been written to custodial copy disk."}, "properties": {"executionId": "some_batchId", "messageType": "preserve.digital.asset.ingest.complete"}}"""
    )
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(List(message).asJava)
    val result = runLambda(sqsEvent, List(initialFileItem), Map.empty)
    result.result.isLeft should equal(true)
    result.result.left.value.getMessage should equal(
      "Failed to decode SQS message body: Attempt to decode value on failed cursor: DownField(assetId),DownField(parameters)"
    )
  }

  "lambda handler" should "throw an exception when asset does not exist in the table" in {
    val id = UUID.randomUUID
    val initialFileItem = createDynamoItem(id, "file1.txt", "s3://some-bucket/some-key", "parent1/parent2", DynamoFormatters.Type.Asset)
    val message = new SQSMessage()
    message.setBody(
      s"""{"parameters": {"assetId": "non-existent", "status": "Asset has been written to custodial copy disk."}, "properties": {"executionId": "some_batchId", "messageType": "preserve.digital.asset.ingest.complete"}}"""
    )
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(List(message).asJava)
    val result = runLambda(sqsEvent, List(initialFileItem), Map.empty)
    result.result.isLeft should equal(true)
    result.result.left.value.getMessage should equal("No item found for assetId=non-existent")
  }

  "lambda handler" should "throw an exception when more than one item exists in the table" in {
    val id = UUID.randomUUID
    val initialItem = createDynamoItem(id, "file1.txt", "s3://some-bucket/some-key", "parent1/parent2", DynamoFormatters.Type.Asset)
    val anotherItem = createDynamoItem(id, "file2.txt", "s3://some-bucket/another-key", "parent1/parent2/parent3", DynamoFormatters.Type.Asset)
    val message = new SQSMessage()
    message.setBody(
      s"""{"parameters": {"assetId": "$id", "status": "Asset has been written to custodial copy disk."}, "properties": {"executionId": "some_batchId", "messageType": "preserve.digital.asset.ingest.complete"}}"""
    )
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(List(message).asJava)
    val result = runLambda(sqsEvent, List(initialItem, anotherItem), Map.empty)
    result.result.isLeft should equal(true)
    result.result.left.value.getMessage should equal(s"""More than one item found for assetId=$id""")
  }

  "Lambda" should "correctly construct the parent path for querying child file items" in {
    val lambda = new Lambda()
    val id = UUID.randomUUID
    val assetItemWithParentPath = createDynamoItem(id, "file1.txt", "s3://some-bucket/some-key", "parent1/parent2", DynamoFormatters.Type.Asset)
    lambda.getParentPathForChildren(assetItemWithParentPath) should equal(s"parent1/parent2/$id")

    val assetItemWithoutParentPath = AssetDynamoItem(
      batchId = "some_batchId",
      id = id,
      potentialParentPath = None,
      `type` = DynamoFormatters.Type.Asset,
      potentialTitle = None,
      potentialDescription = None,
      transferringBody = None,
      transferCompleteDatetime = None,
      upstreamSystem = "some_upstream_system",
      digitalAssetSource = "some_digital_asset_source",
      potentialDigitalAssetSubtype = None,
      originalMetadataFiles = Nil,
      identifiers = Nil,
      childCount = 0,
      skipIngest = false,
      correlationId = None,
      filePath = "some_file_path",
      ttl = 1779382126L
    )
    lambda.getParentPathForChildren(assetItemWithoutParentPath) should equal(id.toString)
  }
