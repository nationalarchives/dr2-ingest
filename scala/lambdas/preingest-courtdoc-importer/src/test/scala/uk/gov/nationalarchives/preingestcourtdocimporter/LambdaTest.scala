package uk.gov.nationalarchives.preingestcourtdocimporter

import cats.syntax.all.*
import io.circe.Json
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.flatspec.AnyFlatSpec
import uk.gov.nationalarchives.preingestcourtdocimporter.Lambda.{TREInput, TREInputParameters}
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.generic.auto.*
import TestUtils.*
import org.scalatest.EitherValues
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

  "message decoder" should "correctly decode the message details from the json" in {
    val json = Json.obj(
      "properties" -> Json.obj(
        "messageType" -> Json.fromString("some.important.data.available"),
        "function" -> Json.fromString("some-function-name"),
        "producer" -> Json.fromString("PQR"),
        "executionId" -> Json.fromString("2B6D53BA-076A-4341-9F7E-D212C43E528B"),
        "parentExecutionId" -> Json.fromString("771D05FD-91EB-450C-BEAA-DBD5AC4A53B7"),
        "timestamp" -> Json.fromString("2026-07-16T15:27:37.750Z")
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
        message.properties.get.messageType should equal("some.important.data.available")
        message.properties.get.producer should equal("PQR")
        message.properties.get.function should equal("some-function-name")
        message.properties.get.executionId should equal("2B6D53BA-076A-4341-9F7E-D212C43E528B")
        message.properties.get.parentExecutionId should equal("771D05FD-91EB-450C-BEAA-DBD5AC4A53B7")
        message.properties.get.timestamp should equal("2026-07-16T15:27:37.750Z")
      case Left(err) => fail(s"Decoding failed: $err")
    }
  }

  "message decoder" should "error if the message is missing required fields" in {
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
        message.properties should equal(None) // properties is optional, so decoding should succeed with None
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

  "lambda handler" should "copy only the data file and the metadata file to the output bucket" in {
    val tdrUuid = UUID.randomUUID
    val filesMap = initialTestDataInTRECommonBucket(inputMetadata(tdrUuid), reference, "ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2")
    val s3Objects = filesMap.map { case (fileName, content) => S3Object("some-test-bucket-name", fileName, content) }.toList
    val (res, s3State, _) = runLambda(s3Objects, event())

    val metadata = decode[TREMetadata](s3State.head.content.array().map(_.toChar).mkString).value

    res.isRight should equal(true)
    s3State.count(_.bucket == "bucket") should equal(2)
    s3State(1).content.array().length should equal(100)

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
    sqsMessage.fileId should equal(predictableUuid)
    sqsMessage.location should equal(s"s3://bucket/$predictableUuid")
  }

  "lambda handler" should "error if there is a error downloading the files" in {
    val tdrUuid = UUID.randomUUID
    val filesMap = initialTestDataInTRECommonBucket(inputMetadata(tdrUuid), reference, "ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2")
    val s3Objects = filesMap.map { case (fileName, content) => S3Object("some-test-bucket-name", fileName, content) }.toList

    val (res, _, _) = runLambda(s3Objects, event(), Option(Errors(download = true)))

    res.isLeft should equal(true)
    res.left.value.getMessage should equal("Error downloading files")
  }

  "lambda handler" should "error if there is an error uploading the file" in {
    val tdrUuid = UUID.randomUUID
    val filesMap = initialTestDataInTRECommonBucket(inputMetadata(tdrUuid), reference, "ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2")
    val s3Objects = filesMap.map { case (fileName, content) => S3Object("some-test-bucket-name", fileName, content) }.toList

    val (res, _, _) = runLambda(s3Objects, event(), Option(Errors(upload = true)))

    res.isLeft should equal(true)
    res.left.value.getMessage should equal("Upload failed")
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
      s"Object not found: some-test-bucket-name/ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2/out/TRE-$reference-metadata.json"
    )
  }

  "lambda handler" should "error if the data file cannot be found in the source location" in {
    val filesMap =
      initialTestDataInTRECommonBucket(inputMetadata(UUID.randomUUID(), "AnotherFile.docx"), reference, "ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2")
    val s3Objects = filesMap.map { case (fileName, content) => S3Object("some-test-bucket-name", fileName, content) }.toList

    val (res, _, _) = runLambda(s3Objects, event())

    res.isLeft should equal(true)
    res.left.value.getMessage should equal(s"Object not found: some-test-bucket-name/ABC-2026-A1B2/2BFC0015-140A-4836-8F44-D918F2B9455C/ABC-2026-A1B2/out/data/AnotherFile.docx")
  }
}
