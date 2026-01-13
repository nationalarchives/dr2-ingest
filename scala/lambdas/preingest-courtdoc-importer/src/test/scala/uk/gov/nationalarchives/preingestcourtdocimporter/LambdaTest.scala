package uk.gov.nationalarchives.preingestcourtdocimporter

import cats.syntax.all.*
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.flatspec.AnyFlatSpec
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.generic.auto.*
import TestUtils.*
import org.scalatest.EitherValues
import uk.gov.nationalarchives.utils.ExternalUtils.*

import java.time.OffsetDateTime
import java.util.UUID

class LambdaTest extends AnyFlatSpec with EitherValues {

  def inputMetadata(tdrUuid: UUID): String = TREMetadata(
    TREMetadataParameters(
      Parser("https://example.com/id/court/2023/abc".some, None, "test".some, Nil, Nil),
      TREParams(reference, Payload("Test.docx")),
      TDRParams("checksum", "Source", "identifier", OffsetDateTime.parse("2024-11-07T15:29:54Z"), None, tdrUuid)
    )
  ).asJson.noSpaces

  "lambda handler" should "copy only the file and the metadata file to the output bucket" in {
    val tdrUuid = UUID.randomUUID
    val fileName = "test.tar.gz"
    val content = fileBytes(inputMetadata(tdrUuid), 100)

    val (res, s3State, _) = runLambda(List(S3Object("inputBucket", fileName, content)), event())

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
    val fileName = "test.tar.gz"
    val content = fileBytes(inputMetadata(tdrUuid), 100)

    val (res, _, sqsState) = runLambda(List(S3Object("inputBucket", fileName, content)), event())

    res.isRight should equal(true)

    sqsState.size should equal(1)
    val sqsMessage = sqsState.head
    sqsMessage.id should equal(tdrUuid)
    sqsMessage.fileId should equal(uuids.head)
    sqsMessage.location should equal(s"s3://bucket/${uuids(2)}")
  }

  "lambda handler" should "error if there is a error downloading the tar file" in {
    val tdrUuid = UUID.randomUUID
    val fileName = "test.tar.gz"
    val content = fileBytes(inputMetadata(tdrUuid), 100)

    val (res, _, _) = runLambda(List(S3Object("inputBucket", fileName, content)), event(), Option(Errors(download = true)))

    res.isLeft should equal(true)
    res.left.value.getMessage should equal("Error downloading files")
  }

  "lambda handler" should "error if there is an error uploading the extracted file" in {
    val tdrUuid = UUID.randomUUID
    val fileName = "test.tar.gz"
    val content = fileBytes(inputMetadata(tdrUuid), 100)

    val (res, _, _) = runLambda(List(S3Object("inputBucket", fileName, content)), event(), Option(Errors(upload = true)))

    res.isLeft should equal(true)
    res.left.value.getMessage should equal("Upload failed")
  }

  "lambda handler" should "error if there is an error sending the message to the queue" in {
    val tdrUuid = UUID.randomUUID
    val fileName = "test.tar.gz"
    val content = fileBytes(inputMetadata(tdrUuid), 100)

    val (res, _, _) = runLambda(List(S3Object("inputBucket", fileName, content)), event(), Option(Errors(sendMessage = true)))

    res.isLeft should equal(true)
    res.left.value.getMessage should equal("Error sending messages")
  }

}
