package uk.gov.nationalarchives.e2etests

import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import io.circe.Decoder
import io.circe.generic.auto.*
import io.circe.parser.decode
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.e2etests.E2ESpecUtils.*
import uk.gov.nationalarchives.e2etests.StepDefs.*

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

class E2ESpec extends AnyFlatSpec with EitherValues {

  "Given" should "create 2000 files for an ingest of 1000 files" in {
    val s3Results = runGiven("An ingest with 1000 files")
    s3Results.size should equal(2000)
  }

  "Given" should "create 2 files with the correct content for an ingest of one file" in {
    val s3Results = runGiven("An ingest with 1 files").sortBy(_.length)
    s3Results.size should equal(2)

    noException should be thrownBy UUID.fromString(s3Results.head)

    val metadata = decode[TDRMetadata](s3Results.last).value
    metadata.Series should equal(Some("TEST 123"))
    metadata.TransferringBody should equal("TestBody")
    metadata.description should equal(None)
  }

  "Given" should "create 1 file with an empty checksum" in {
    val s3Results = runGiven("An ingest with 1 file with an empty checksum").sortBy(_.length)

    val metadata = decode[TDRMetadata](s3Results.last).value
    metadata.SHA256ServerSideChecksum should equal("")
  }

  "Given" should "create 1 file with an invalid checksum" in {
    val s3Results = runGiven("An ingest with 1 file with an invalid checksum").sortBy(_.length)

    val metadata = decode[TDRMetadata](s3Results.last).value
    metadata.SHA256ServerSideChecksum should equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
  }

  "Given" should "create 1 file with an invalid metadata" in {
    val s3Results = runGiven("An ingest with 1 file with invalid metadata").sortBy(_.length)

    val metadata = decode[TDRMetadata](s3Results.last).value
    List(None, Some("TEST123"), Some("")).contains(metadata.Series) should equal(true)
  }

  "When" should "send messages to the queue" in {
    val ids = List.fill(1000)(UUID.randomUUID).sorted
    val sqsResults = runWhen("I send messages to the input queue", ids)._2
    val sortedResults = sqsResults.collect { case s: SqsInputMessage => s }.map(_.fileId).sorted

    ids should equal(sortedResults)
  }

  "When" should "create a batch" in {
    val id = UUID.randomUUID
    val (_, _, dynamoResponses, sfnResponses) = runWhen("I create a batch with this file", List(id))

    dynamoResponses.size should equal(1)
    sfnResponses.size should equal(1)

    val dynamoResponse = dynamoResponses.head
    dynamoResponse.tableName should equal("lock-table")
    dynamoResponse.conditionalExpression.get should equal("attribute_not_exists(assetId)")
    dynamoResponse.attributeNamesAndValuesToWrite("assetId").s() should equal(id.toString)
    val expectedResponse = s"""{"id":"$id","location":"s3://input-bucket/$id"}"""
    dynamoResponse.attributeNamesAndValuesToWrite("message").s() should equal(expectedResponse)

    val sfnResponse = sfnResponses.head
    sfnResponse.waitFor should equal(0)
    val groupIdParts = sfnResponse.groupId.split("_")
    noException should be thrownBy UUID.fromString(groupIdParts.last)
    groupIdParts.head should equal("E2E")
    val batchIdParts = sfnResponse.batchId.split("_")
    noException should be thrownBy UUID.fromString(batchIdParts(1))
    batchIdParts.head should equal("E2E")
    batchIdParts.last should equal("0")
  }

  "Then" should "wait for the specified time" in {
    noException should be thrownBy runThen("I wait for 2 milliseconds")
  }

  "Then" should "error if the time unit is invalid" in {
    val ex = intercept[Exception](runThen("I wait for 2 gigaseconds"))
    ex.getMessage should equal("key not found: gigaseconds")
  }

  "Then" should "retrieve the expected error messages" in {
    val id = UUID.randomUUID
    val properties = OutputProperties("", UUID.randomUUID, None, Instant.now, ingestUpdateType)
    val parameters = OutputParameters(id, assetErrorStatus)
    val sqsMessages = List(NotificationsOutputMessage(properties, parameters))
    val sqsResults = runThen("I receive an ingest error message", List(id), sqsMessages, 5.seconds)
  }

  "Then" should "retrieve the expected ingest complete messages" in {
    val id = UUID.randomUUID
    val properties = OutputProperties("", UUID.randomUUID, None, Instant.now, ingestCompleteType)
    val parameters = OutputParameters(id, ccDiskStatus)
    val sqsMessages = List(NotificationsOutputMessage(properties, parameters))
    val sqsResults = runThen("I receive the ingest complete messages", List(id), sqsMessages, 5.seconds)
  }

  "Then" should "retrieve the expected validation error messages" in {
    val id = UUID.randomUUID
    val sqsMessages = List(SqsInputMessage(id, "bucket"))
    val sqsResults = runThen("I receive an error in the validation queue", List(id), sqsMessages, 5.seconds)
  }

  "Then" should "error if there are no messages found before timeout" in {
    val ex = intercept[Exception](runThen("I receive an ingest error message", List(UUID.randomUUID)))
    ex.getMessage should equal("1 second")
  }

  "Then" should "error if there are no messages for the same ids before timeout" in {
    val properties = OutputProperties("", UUID.randomUUID, None, Instant.now, ingestCompleteType)
    val parameters = OutputParameters(UUID.randomUUID, ccDiskStatus)
    val sqsMessages = List(NotificationsOutputMessage(properties, parameters))
    val ex = intercept[Exception](runThen("I receive an ingest error message", List(UUID.randomUUID)))
    ex.getMessage should equal("1 second")
  }

  "loadAllScenarios" should "load the files from the features directory" in {
    val runner = IngestTestsRunner()
    val allTests = runner.loadAllScenarios()
    allTests.size should equal(4)
  }

  "processEachStep" should "call the relevant step def methods" in {
    val (createFilesCount, sendMessagesCount, createBatchCount, waitForIngestCompleteCount, waitForIngestErrorCount, pollForValidationCount) =
      runProcessEachStep()

    createFilesCount should equal(4)
    sendMessagesCount should equal(2)
    createBatchCount should equal(2)
    waitForIngestCompleteCount should equal(1)
    waitForIngestErrorCount should equal(2)
    pollForValidationCount should equal(1)
  }

  "processEachStep" should "raise an error if there is no step expression" in {
    val ex = intercept[Exception] {
      (for {
        ref <- Ref.of[IO, List[UUID]](Nil)
        _ <- IngestTestsRunner().processEachStep(List("Given"), unusedStepDefs)(using ref)
      } yield ()).unsafeRunSync()
    }
    ex.getMessage should equal(" (of class java.lang.String)")

  }
}
