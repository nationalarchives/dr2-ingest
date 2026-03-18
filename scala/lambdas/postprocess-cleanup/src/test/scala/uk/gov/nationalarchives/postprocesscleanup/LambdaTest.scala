package uk.gov.nationalarchives.postprocesscleanup

import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{Checksum, FileDynamoItem}
import uk.gov.nationalarchives.postprocesscleanup.Helper.*

import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.UUID
import scala.jdk.CollectionConverters.*

class LambdaTest extends AnyFlatSpec with EitherValues:
  val tomorrow: LocalDate = LocalDate.now(ZoneOffset.UTC).plusDays(1)

  "lambda handler" should "update ttl value of the current asset to be tomorrow" in {
    val id = UUID.randomUUID
    val initialFileItem = createDynamoItem(id, "file1.txt", "s3://some-bucket/some-key", Some("parent1/parent2"))
    val message = new SQSMessage()
    message.setBody(
      s"""{"parameters": {"assetId": "$id", "status": "Asset has been written to custodial copy disk."}, "properties": {"executionId": "COURTDOC_TST-2025-C4PD_0", "messageType": "preserve.digital.asset.ingest.complete"}}"""
    )
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(List(message).asJava)
    val result = runLambda(sqsEvent, List(initialFileItem), Map.empty)
    result.result.isLeft should equal(false)
    result.finalItemsInTable.size should equal(1)
    toDate(result.finalItemsInTable.head.ttl) shouldBe tomorrow
    result.finalsObjectsInS3.size shouldBe 0
  }

  "lambda handler" should "update ttl value for all item in the hierarchy of assetId sent in the message" in {
    val message = new SQSMessage()
    message.setBody(
      s"""{"parameters": {"assetId": "d5c74859-b4fa-403e-a11c-0c7652265f03", "status": "Asset has been written to custodial copy disk."}, "properties": {"executionId": "COURTDOC_TST-2025-C4PD_0", "messageType": "preserve.digital.asset.ingest.complete"}}"""
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

  "lambda handler" should "not update ttl value for any item when they are not in the hierarchy of assetId sent in the message" in {
    val message = new SQSMessage()
    message.setBody(
      s"""{"parameters": {"assetId": "3cc1cbed-c4fc-49c4-b09e-b80cb4e0c9ce", "status": "Asset has been written to custodial copy disk."}, "properties": {"executionId": "COURTDOC_TST-2025-C4PD_0", "messageType": "preserve.digital.asset.ingest.complete"}}"""
    )

    val oneItem = createDynamoItem(UUID.fromString("3cc1cbed-c4fc-49c4-b09e-b80cb4e0c9ce"), "file-out-of-hierarchy.txt", "s3://some-bucket/some-key", Some("some-other-parent"))
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

  "lambda handler" should "throw an exception when there is no SQS records in the SQS event" in {
    val id = UUID.randomUUID
    val initialFileItem = createDynamoItem(id, "file1.txt", "s3://some-bucket/some-key", Some("parent1/parent2"))
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(List.empty[SQSEvent.SQSMessage].asJava)
    val result = runLambda(sqsEvent, List(initialFileItem), Map.empty)
    result.result.isLeft should equal(true)
    result.result.left.value.getMessage should equal("No SQS records")
  }

  "lambda handler" should "throw an exception when it cannot decode the sqs message" in {
    val id = UUID.randomUUID
    val initialFileItem = createDynamoItem(id, "file1.txt", "s3://some-bucket/some-key", Some("parent1/parent2"))
    val message = new SQSMessage()
    message.setBody(
      s"""{"parameters": {"asset_Id": "$id", "status": "Asset has been written to custodial copy disk."}, "properties": {"executionId": "COURTDOC_TST-2025-C4PD_0", "messageType": "preserve.digital.asset.ingest.complete"}}"""
    )
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(List(message).asJava)
    val result = runLambda(sqsEvent, List(initialFileItem), Map.empty)
    result.result.isLeft should equal(true)
    result.result.left.value.getMessage should equal("Failed to decode SQS message body: Attempt to decode value on failed cursor: DownField(assetId),DownField(parameters)")
  }

  "lambda handler" should "throw an exception when asset does not exist in the table" in {
    val id = UUID.randomUUID
    val initialFileItem = createDynamoItem(id, "file1.txt", "s3://some-bucket/some-key", Some("parent1/parent2"))
    val message = new SQSMessage()
    message.setBody(
      s"""{"parameters": {"assetId": "non-existent", "status": "Asset has been written to custodial copy disk."}, "properties": {"executionId": "COURTDOC_TST-2025-C4PD_0", "messageType": "preserve.digital.asset.ingest.complete"}}"""
    )
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(List(message).asJava)
    val result = runLambda(sqsEvent, List(initialFileItem), Map.empty)
    result.result.isLeft should equal(true)
    result.result.left.value.getMessage should equal("No item found for assetId=non-existent")
  }

  "lambda handler" should "throw an exception when more than one item exists in the table" in {
    val id = UUID.randomUUID
    val initialItem = createDynamoItem(id, "file1.txt", "s3://some-bucket/some-key", Some("parent1/parent2"))
    val anotherItem = createDynamoItem(id, "file2.txt", "s3://some-bucket/another-key", Some("parent1/parent2/parent3"))
    val message = new SQSMessage()
    message.setBody(
      s"""{"parameters": {"assetId": "$id", "status": "Asset has been written to custodial copy disk."}, "properties": {"executionId": "COURTDOC_TST-2025-C4PD_0", "messageType": "preserve.digital.asset.ingest.complete"}}"""
    )
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(List(message).asJava)
    val result = runLambda(sqsEvent, List(initialItem, anotherItem), Map.empty)
    result.result.isLeft should equal(true)
    result.result.left.value.getMessage should equal(s"""More than one item found for assetId=$id""")
  }

  private def toDate(ttl: Long): LocalDate =
    Instant
      .ofEpochSecond(ttl)
      .atZone(ZoneOffset.UTC)
      .toLocalDate

  private def createDynamoItem(id: UUID, name: String, location: String, parentPath: Option[String] = None): FileDynamoItem =
    FileDynamoItem(
      batchId = "some_batchId",
      id = id,
      potentialParentPath = parentPath,
      name = name,
      `type` = DynamoFormatters.Type.File,
      potentialTitle = None,
      potentialDescription = None,
      sortOrder = 1,
      fileSize = 46L,
      checksums = List(Checksum("some_algorithm", "some_fingerprint")),
      potentialFileExtension = Some("txt"),
      representationType = DynamoFormatters.FileRepresentationType.PreservationRepresentationType,
      representationSuffix = 1,
      identifiers = Nil,
      childCount = 0,
      location = new java.net.URI(location),
      ttl = 1779382126L
    )

//   The initial data contains a hierarchy of 6 items in total, with the following structure:
//   * 1 asset with a parent path of pattern "A/B/C"
//   * its 2 children, each has a valid location and corresponding object in s3 with tags to have variety in test data
//   * 3 ancestors each with id A, B and C respectively, with parentPath forming a hierarchy and no location
  private def createInitialData: InitialData =
    val assetItem = createDynamoItem(
      UUID.fromString("d5c74859-b4fa-403e-a11c-0c7652265f03"),
      "d5c74859-b4fa-403e-a11c-0c7652265f03",
      "",
      Some("d1ad2270-1711-47db-b663-c530bc518e87/9385ad5c-e205-40fd-8cb2-c157d1331167/d086e29a-83ed-4129-b20c-8e2041bac4f7")
    )
    val childOne = createDynamoItem(
      UUID.fromString("a5788834-3b45-491e-91d8-fd008351a3ad"),
      "child-one.json",
      "s3://some-bucket/a5788834-3b45-491e-91d8-fd008351a3ad",
      Some("d1ad2270-1711-47db-b663-c530bc518e87/9385ad5c-e205-40fd-8cb2-c157d1331167/d086e29a-83ed-4129-b20c-8e2041bac4f7/d5c74859-b4fa-403e-a11c-0c7652265f03")
    )
    val childTwo = createDynamoItem(
      UUID.fromString("d665011c-f6b6-4bf9-9df1-9218b7429cd5"),
      "child-two.json",
      "s3://some-bucket/d665011c-f6b6-4bf9-9df1-9218b7429cd5",
      Some("d1ad2270-1711-47db-b663-c530bc518e87/9385ad5c-e205-40fd-8cb2-c157d1331167/d086e29a-83ed-4129-b20c-8e2041bac4f7/d5c74859-b4fa-403e-a11c-0c7652265f03")
    )
    val ancestor1 = createDynamoItem(
      UUID.fromString("d086e29a-83ed-4129-b20c-8e2041bac4f7"),
      "parent-one",
      "",
      Some("d1ad2270-1711-47db-b663-c530bc518e87/9385ad5c-e205-40fd-8cb2-c157d1331167")
    )
    val ancestor2 = createDynamoItem(UUID.fromString("9385ad5c-e205-40fd-8cb2-c157d1331167"), "parent-two", "", Some("d1ad2270-1711-47db-b663-c530bc518e87"))
    val ancestor3 = createDynamoItem(UUID.fromString("d1ad2270-1711-47db-b663-c530bc518e87"), "parent-three", "", None)
    InitialData(
      List(assetItem, childOne, childTwo, ancestor1, ancestor2, ancestor3),
      Map(
        "s3://some-bucket/a5788834-3b45-491e-91d8-fd008351a3ad" -> Map("TAG_ONE" -> "ONE", "TAG_TWO" -> "TWO"),
        "s3://some-bucket/d665011c-f6b6-4bf9-9df1-9218b7429cd5" -> Map.empty
      )
    )

  case class InitialData(dynamoItems: List[FileDynamoItem], s3Objects: Map[String, Map[String, String]])
