package uk.gov.nationalarchives.dynamoformatters

import cats.implicits.*
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1, TableFor3}
import org.scanamo.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.{Type, *}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.FileRepresentationType.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{*, given}

import java.net.URI
import java.time.{Instant, OffsetDateTime}
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.jdk.CollectionConverters.*

class DynamoWriteUtilsTest extends AnyFlatSpec with TableDrivenPropertyChecks with EitherValues {

  "writeLockTableItem" should "convert the values of an 'IngestLockTableItem' to a DynamoValue if the IngestLockTableItem has the expected number of fields" in {
    val actualDynamoValue = DynamoWriteUtils.writeLockTableItem(
      IngestLockTableItem(
        UUID.fromString("90730c77-8faa-4dbf-b20d-bba1046dac87"),
        "groupId",
        "message",
        "createdAt"
      )
    )

    val expectedDynamoValue = DynamoObject(
      Map(
        "assetId" -> DynamoValue.fromString("90730c77-8faa-4dbf-b20d-bba1046dac87"),
        "groupId" -> DynamoValue.fromString("groupId"),
        "message" -> DynamoValue.fromString("message"),
        "createdAt" -> DynamoValue.fromString("createdAt")
      )
    ).toDynamoValue

    actualDynamoValue should be(expectedDynamoValue)
  }

  "writeLockTableItem" should "throw an error if the IngestLockTableItem has fewer fields than expected" in {
    intercept[AssertionError] {
      DynamoWriteUtils.writeLockTableItem(
        IngestLockTableItem(
          UUID.fromString("90730c77-8faa-4dbf-b20d-bba1046dac87"),
          "groupId",
          "message",
          "createdAt"
        ),
        true
      )
    }.getMessage should be("assertion failed: The fields in the Map need to be updated to match the fields in IngestLockTableItem")
  }

  "writeIngestQueueTableItem" should "convert the values of an 'IngestQueueTableItem' to a DynamoValue if the IngestQueueTableItem has the expected number of fields" in {
    val actualDynamoValue = DynamoWriteUtils.writeIngestQueueTableItem(
      IngestQueueTableItem(
        "supportedSystemName",
        "queuedTimeAndExecutionName",
        "taskToken",
        "executionName",
        2,
        1000L
      )
    )

    val expectedDynamoValue = DynamoObject(
      Map(
        "sourceSystem" -> DynamoValue.fromString("supportedSystemName"),
        "queuedAt" -> DynamoValue.fromString("queuedTimeAndExecutionName"),
        "taskToken" -> DynamoValue.fromString("taskToken"),
        "executionName" -> DynamoValue.fromString("executionName"),
        "queuedAssetCount" -> DynamoValue.fromNumber[Int](2),
        "queuedBytes" -> DynamoValue.fromNumber[Long](1000)
      )
    ).toDynamoValue

    actualDynamoValue should be(expectedDynamoValue)
  }

  "writeIngestQueueTableItem" should "throw an error if the IngestQueueTableItem has fewer fields than expected" in {
    intercept[AssertionError] {
      DynamoWriteUtils.writeIngestQueueTableItem(
        IngestQueueTableItem(
          "supportedSystemName",
          "queuedTimeAndExecutionName",
          "taskToken",
          "executionName",
          2,
          1000L
        ),
        true
      )
    }.getMessage should be("assertion failed: The fields in the Map need to be updated to match the fields in IngestQueueTableItem")
  }
}
