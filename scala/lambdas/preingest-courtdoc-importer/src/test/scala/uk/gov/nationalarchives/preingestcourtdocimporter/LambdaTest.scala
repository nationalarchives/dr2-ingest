package uk.gov.nationalarchives.preingestcourtdocimporter

import cats.syntax.all.*
import io.circe.Json
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.preingestcourtdocimporter.Lambda.{TREInput, TREInputParameters}
import uk.gov.nationalarchives.preingestcourtdocimporter.TestUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.*

import java.time.OffsetDateTime
import java.util.UUID

class LambdaTest extends AnyFlatSpec with EitherValues {

  val reference = "TEST-REFERENCE"

  def inputMetadata(tdrUuid: UUID, fileName: String = "Test.docx"): String = TREMetadata(
    TREMetadataParameters(
      Parser("https://example.com/id/court/2023/abc".some, None, "test".some, Nil, Nil),
      TREParams(reference, Payload(fileName)),
      TDRParams("checksum", "Source", "identifier", OffsetDateTime.parse("2024-11-07T15:29:54Z"), None, tdrUuid)
    )
  ).asJson.noSpaces

  "message decoder" should "successfully decode the message parameters from json and ignore any other fields" in {
    val json = Json.obj(
      "should-be-ignored" -> Json.obj(
        "something" -> Json.fromString("not needed"),
        "to" -> Json.fromString("be in the object"),
        "ignore" -> Json.fromString("all such things"),
        "without" -> Json.fromString("failing the decode")
      ),
      "parameters" -> Json.obj(
        "reference" -> Json.fromString("ABC-2026-A1B2"),
        "s3FolderName" -> Json.fromString("ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2"),
        "originator" -> Json.fromString("ABC"),
        "s3Bucket" -> Json.fromString("some-test-bucket-name"),
        "status" -> Json.fromString("PARSE_SUCCESS")
      )
    )
    val decoded = json.as[TREInput]
    decoded match {
      case Right(message) =>
        message.parameters.reference should equal("ABC-2026-A1B2")
        message.parameters.s3FolderName should equal("ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2")
        message.parameters.originator should equal("ABC")
        message.parameters.s3Bucket should equal("some-test-bucket-name")
        message.parameters.status should equal("PARSE_SUCCESS")
      case Left(err) => fail(s"Decoding failed: $err")
    }
  }

  "message decoder" should "set skipSeriesLookup to false if it does not exist in the json" in {
    val json = Json.obj(
      "parameters" -> Json.obj(
        "reference" -> Json.fromString("ABC-2026-A1B2"),
        "s3FolderName" -> Json.fromString("ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2"),
        "originator" -> Json.fromString("ABC"),
        "s3Bucket" -> Json.fromString("some-test-bucket-name"),
        "status" -> Json.fromString("PARSE_SUCCESS")
      )
    )
    val decoded = json.as[TREInput]
    decoded match {
      case Right(message) =>
        message.parameters.skipSeriesLookup should equal(false)
      case Left(err) => fail(s"Decoding failed: $err")
    }
  }

  "message decoder" should "set skipSeriesLookup to true if a true value is provided in the json" in {
    val json = Json.obj(
      "parameters" -> Json.obj(
        "reference" -> Json.fromString("ABC-2026-A1B2"),
        "s3FolderName" -> Json.fromString("ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2"),
        "originator" -> Json.fromString("ABC"),
        "s3Bucket" -> Json.fromString("some-test-bucket-name"),
        "status" -> Json.fromString("PARSE_SUCCESS"),
        "skipSeriesLookup" -> Json.fromBoolean(true)
      )
    )
    val decoded = json.as[TREInput]
    decoded match {
      case Right(message) =>
        message.parameters.skipSeriesLookup should equal(true)
      case Left(err) => fail(s"Decoding failed: $err")
    }
  }

  "message decoder" should "throw an exception when one of the required fields is missing" in {
    val json = Json.obj(
      "parameters" -> Json.obj(
        "reference" -> Json.fromString("ABC-2026-A1B2"),
        "s3FolderName" -> Json.fromString("ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2"),
        "originator" -> Json.fromString("ABC"),
        "s3Bucket" -> Json.fromString("some-test-bucket-name")
      )
    )
    val decoded = json.as[TREInput]
    decoded match {
      case Right(_)  => fail("Decoding should have failed due to missing 'status' field")
      case Left(err) =>
        err.getMessage should include("DecodingFailure at .parameters.status: Missing required field")
    }
  }

  "lambda handler" should "copy only the data file and the metadata file to the output bucket" in {
    val tdrUuid = UUID.randomUUID
    val filesMap = initialTestDataInTRECommonBucket(inputMetadata(tdrUuid), reference, "ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2")
    val s3Objects = filesMap.map { case (fileName, content) => S3Object("some-test-bucket-name", fileName, content) }.toList
    val (res, s3State, _) = runLambda(s3Objects, event())

    val metadata = decode[TREMetadata](s3State.head.content.array().map(_.toChar).mkString).value

    res.isRight should equal(true)
    val objectsInDestinationBucket = s3State.filter(_.bucket == "bucket")
    objectsInDestinationBucket.size should equal(2)
    val expectedKeys = List(predictableUuids.head.toString, s"${predictableUuids(1)}.metadata")
    objectsInDestinationBucket.find(_.key == predictableUuids.head.toString).get.content.array().length should equal(100)
    objectsInDestinationBucket.map(_.key) should contain allElementsOf (expectedKeys)

    metadata.parameters.PARSER.uri.get should equal("https://example.com/id/court/2023/abc")
    metadata.parameters.PARSER.name.get should equal("test")
    metadata.parameters.TRE.reference should equal("TEST-REFERENCE")
    metadata.parameters.TRE.payload.filename should equal("Test.docx")
    metadata.parameters.TDR.`Document-Checksum-sha256` should equal("checksum")
  }

  "lambda handler" should "send the correct message to the sqs queue" in {
    val tdrUuid = UUID.randomUUID
    val filesMap = initialTestDataInTRECommonBucket(inputMetadata(tdrUuid), reference, "ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2")
    val s3Objects = filesMap.map { case (fileName, content) => S3Object("some-test-bucket-name", fileName, content) }.toList
    val (res, _, sqsState) = runLambda(s3Objects, event())

    res.isRight should equal(true)

    sqsState.size should equal(1)
    val sqsMessage = sqsState.head
    sqsMessage.id should equal(tdrUuid)
    sqsMessage.fileId should equal(predictableUuids.head)
    sqsMessage.location should equal(s"s3://bucket/${predictableUuids(1)}.metadata")
  }

  "lambda handler" should "error if there is a error downloading the files" in {
    val tdrUuid = UUID.randomUUID
    val filesMap = initialTestDataInTRECommonBucket(inputMetadata(tdrUuid), reference, "ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2")
    val s3Objects = filesMap.map { case (fileName, content) => S3Object("some-test-bucket-name", fileName, content) }.toList

    val (res, _, _) = runLambda(s3Objects, event(), Option(Errors(download = true)))

    res.isLeft should equal(true)
    res.left.value.getMessage should equal("Error downloading files")
  }

  "lambda handler" should "error if there is an error copying the file" in {
    val tdrUuid = UUID.randomUUID
    val filesMap = initialTestDataInTRECommonBucket(inputMetadata(tdrUuid), reference, "ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2")
    val s3Objects = filesMap.map { case (fileName, content) => S3Object("some-test-bucket-name", fileName, content) }.toList

    val (res, _, _) = runLambda(s3Objects, event(), Option(Errors(copy = true)))

    res.isLeft should equal(true)
    res.left.value.getMessage should equal("Copy failed")
  }

  "lambda handler" should "error if there is an error sending the message to the queue" in {
    val tdrUuid = UUID.randomUUID
    val filesMap = initialTestDataInTRECommonBucket(inputMetadata(tdrUuid), reference, "ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2")
    val s3Objects = filesMap.map { case (fileName, content) => S3Object("some-test-bucket-name", fileName, content) }.toList

    val (res, _, _) = runLambda(s3Objects, event(), Option(Errors(sendMessage = true)))

    res.isLeft should equal(true)
    res.left.value.getMessage should equal("Error sending messages")
  }

  "lambda handler" should "error if the metadata file cannot be found in the source location" in {
    val filesMap = initialTestDataInTRECommonBucket(inputMetadata(UUID.randomUUID()), "ANOTHER-REFERENCE", "ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2")
    val s3Objects = filesMap.map { case (fileName, content) => S3Object("some-test-bucket-name", fileName, content) }.toList

    val (res, _, _) = runLambda(s3Objects, event())

    res.isLeft should equal(true)
    res.left.value.getMessage should equal(
      s"Object not found: some-test-bucket-name/ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2/TRE-$reference-metadata.json"
    )
  }

  "lambda handler" should "error if the data file cannot be found in the source location" in {
    val filesMap =
      initialTestDataInTRECommonBucket(inputMetadata(UUID.randomUUID(), "AnotherFile.docx"), reference, "ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2")
    val s3Objects = filesMap.map { case (fileName, content) => S3Object("some-test-bucket-name", fileName, content) }.toList

    val (res, _, _) = runLambda(s3Objects, event())

    res.isLeft should equal(true)
    res.left.value.getMessage should equal(s"Object not found: some-test-bucket-name/ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2/data/AnotherFile.docx")
  }
}
