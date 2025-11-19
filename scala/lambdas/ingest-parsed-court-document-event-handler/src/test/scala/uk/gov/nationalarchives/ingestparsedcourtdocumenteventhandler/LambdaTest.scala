package uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler

import cats.syntax.all.*
import io.circe.Decoder
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Checksum
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.FileProcessor.*
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.TestUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.SourceSystem.`TRE: FCL Parser workflow`

import java.net.URI
import java.time.{Instant, LocalDate, OffsetDateTime, ZoneOffset}
import java.util.UUID

class LambdaTest extends AnyFlatSpec with TableDrivenPropertyChecks with EitherValues {

  "the lambda" should "write the metadata and files to the output bucket" in {
    val fileName = "test.tar.gz"
    val (res, s3State, _, _) = runLambda(List(S3Object("inputBucket", fileName, fileBytes(inputMetadata()))), event())

    val metadataString = s3State
      .find(_.key == "TEST-REFERENCE/metadata.json")
      .toList
      .flatMap(_.content.array())
      .map(_.toChar)
      .mkString

    val metadata = decode[List[MetadataObject]](metadataString)
    val transferredFileSize = s3State
      .find(_.key == "cee5851e-813f-4a9d-ae9c-577f9eb601e0")
      .map(_.content.array().length)
      .getOrElse(0)

    s3State.size should equal(4)
    transferredFileSize should equal(100)
  }

  val citeTable: TableFor2[Option[String], List[IdField]] = Table(
    ("potentialCite", "idFields"),
    (None, List(IdField("URI", "https://example.com/id/court/2023/"))),
    (Option("\"cite\""), List(IdField("Code", "cite"), IdField("Cite", "cite")))
  )

  forAll(citeTable) { (potentialCite, idFields) =>
    "the lambda" should s"write the correct metadata files to S3 with a cite ${potentialCite.orNull}" in {
      val tdrUuid = UUID.fromString("24190792-a2e5-43a0-a9e9-6a0580905d90")
      val fileName = "test.tar.gz"
      val initialS3State = List(S3Object("inputBucket", fileName, fileBytes(inputMetadata(tdrUuid))))
      val (res, s3State, dynamoState, _) = runLambda(initialS3State, event())

      val metadata = decode[List[MetadataObject]](s3State.head.content.array().map(_.toChar).mkString).value

      val folderId = UUID.fromString("c2e7866e-5e94-4b4e-a49f-043ad937c18a")
      val fileId = UUID.fromString("61ac0166-ccdf-48c4-800f-29e5fba2efda")
      val metadataFileId = UUID.fromString("4e6bac50-d80a-4c68-bd92-772ac9701f14")
      val expectedAssetMetadata = AssetMetadataObject(
        tdrUuid,
        Option(folderId),
        "Test.docx",
        "24190792-a2e5-43a0-a9e9-6a0580905d90",
        List(metadataFileId),
        Some("test"),
        Option("test-organisation"),
        OffsetDateTime.parse("2023-10-31T13:40:54Z"),
        `TRE: FCL Parser workflow`,
        "Born Digital",
        Option("FCL"),
        "/a/path/to/file",
        None,
        List(
          Option(IdField("UpstreamSystemReference", reference)),
          Option(IdField("URI", "https://example.com/id/court/2023/")),
          potentialCite.map(_ => IdField("NeutralCitation", "cite")),
          Option(IdField("ConsignmentReference", "test-identifier")),
          Option(IdField("UpstreamSystemReference", "TEST-REFERENCE")),
          Option(IdField("RecordID", "24190792-a2e5-43a0-a9e9-6a0580905d90"))
        ).flatten
      )
      val expectedFileMetadata = List(
        FileMetadataObject(
          fileId,
          Option(tdrUuid),
          "Test",
          1,
          "Test.docx",
          15684,
          RepresentationType.Preservation,
          1,
          URI.create(s"s3://$testOutputBucket/$fileId"),
          List(Checksum("sha256", "abcde"))
        ),
        FileMetadataObject(
          metadataFileId,
          Option(tdrUuid),
          "",
          2,
          "TRE-TEST-REFERENCE-metadata.json",
          215,
          RepresentationType.Preservation,
          1,
          URI.create(s"s3://$testOutputBucket/$metadataFileId"),
          List(Checksum("sha256", "78380a854ce3af9caa6448e25190a8867242adf82af6f7e3909a2242c66b3487"))
        )
      )

      val expectedFolderMetadata =
        ArchiveFolderMetadataObject(
          folderId,
          None,
          Option("test"),
          "https://example.com/id/court/2023/",
          Option("TEST SERIES"),
          if potentialCite.isDefined then idFields :+ IdField("URI", "https://example.com/id/court/2023/") else idFields
        )
      val metadataList: List[MetadataObject] =
        List(expectedFolderMetadata, expectedAssetMetadata) ++ expectedFileMetadata
      val expectedMetadata = metadataList.asJson.noSpaces
    }
  }

  "the lambda" should "write the correct values to the lock table" in {
    val tdrUuid = UUID.randomUUID
    val fileName = "test.tar.gz"
    val initialS3State = List(S3Object("inputBucket", fileName, fileBytes(inputMetadata(tdrUuid))))
    val predictableStartOfTheDay: () => Instant = () => LocalDate.now(ZoneOffset.UTC).atStartOfDay(ZoneOffset.UTC).toInstant

    val (res, _, dynamoState, _) = runLambda(initialS3State, event(), None, predictableStartOfTheDay)

    val ddbRequest = dynamoState.head

    val attributeNamesValues = ddbRequest.attributeNamesAndValuesToWrite

    ddbRequest.tableName should equal("lockTable")
    attributeNamesValues("assetId").s() should equal(tdrUuid.toString)
    attributeNamesValues("groupId").s() should equal("TEST-REFERENCE")
    attributeNamesValues("message").s() should equal("""{"messageId":"fd1557f5-8f98-4152-a2a8-726c4a484447"}""")
    attributeNamesValues("createdAt").s() should equal(predictableStartOfTheDay().toString)
    ddbRequest.conditionalExpression.get should equal("attribute_not_exists(assetId)")
  }

  "handler" should "return an error if the Dynamo API is unavailable" in {
    val fileName = "test.tar.gz"
    val initialS3State = List(S3Object("inputBucket", fileName, fileBytes(inputMetadata())))
    val (res, _, dynamoState, _) = runLambda(initialS3State, event(), Errors(write = true).some)

    res.left.value.getMessage should equal("Error writing to Dynamo")
  }

  "the lambda" should "start the state machine execution with the correct parameters" in {
    val fileName = "test.tar.gz"
    val initialS3State = List(S3Object("inputBucket", fileName, fileBytes(inputMetadata())))
    val (res, _, _, sfnState) = runLambda(initialS3State, event())
    val sfnExecution = sfnState.head

    sfnExecution.sfnArn should equal("sfnArn")
    val input = sfnExecution.input
    input.batchId should equal("COURTDOC_TEST-REFERENCE_0")
    input.metadataPackage.toString should equal("s3://bucket/TEST-REFERENCE/metadata.json")
    input.groupId should equal("COURTDOC_TEST-REFERENCE")
    input.retryCount should equal(0)
    input.retrySfnArn should equal("sfnArn")
  }

  "the lambda" should "error if the uri contains '/press-summary' but file name does not contain 'Press Summary of'" in {
    val initialS3State = List(S3Object("inputBucket", "test.tar.gz", fileBytes(inputMetadata(suffix = "press-summary/3"))))

    val (res, _, _, _) = runLambda(initialS3State, event())
    res.left.value.getMessage should equal("URI contains '/press-summary' but file does not start with 'Press Summary of '")
  }

  "the lambda" should "error if the input json is invalid" in {
    val eventWithInvalidJson = event(body = "{}".some)
    val initialS3State = List(S3Object("inputBucket", "test.tar.gz", fileBytes(inputMetadata())))

    val (res, _, _, _) = runLambda(initialS3State, eventWithInvalidJson)

    res.left.value.getMessage.contains("DownField(parameters)") should equal(true)
  }

  "the lambda" should "error if the json in the metadata file is invalid" in {
    val initialS3State = List(S3Object("inputBucket", "test.tar.gz", fileBytes("", 1)))

    val (res, _, _, _) = runLambda(initialS3State, event())

    res.left.value.getMessage should equal("""Error parsing metadata.json.
                                             |Please check that the JSON is valid and that all required fields are present""".stripMargin)
  }

  "the lambda" should "error if the json in the metadata file is missing required fields" in {
    val initialS3State = List(S3Object("inputBucket", "test.tar.gz", fileBytes("{}", 1)))

    val (res, _, _, _) = runLambda(initialS3State, event())

    res.left.value.getMessage should equal("""Attempt to decode value on failed cursor: DownField(parameters)""".stripMargin)
  }

  "the lambda" should "error if the json in the metadata file has a field with a non-optional value that is null" in {
    val metadata = inputMetadata().asJson.noSpaces.replace("\"checksum\"", "null")

    val initialS3State = List(S3Object("inputBucket", "test.tar.gz", fileBytes(metadata, 1)))

    val (res, _, _, _) = runLambda(initialS3State, event())

    res.left.value.getMessage.contains("DownField(Document-Checksum-sha256)") should equal(true)
  }

  "the lambda" should "error if the tar file contains a zero-byte file" in {
    val initialS3State = List(S3Object("inputBucket", "test.tar.gz", fileBytes(inputMetadata(), 0)))

    val (res, _, _, _) = runLambda(initialS3State, event())

    res.left.value.getMessage should equal("File id 'cee5851e-813f-4a9d-ae9c-577f9eb601e0' size is 0")

  }

  "the lambda" should "error if S3 is unavailable" in {
    val fileName = "test.tar.gz"
    val (res, _, _, _) = runLambda(Nil, event(), Errors(download = true).some)
    res.left.value.getMessage should equal("Error downloading files")
  }

  "the lambda" should "succeed even if the`skipSeriesLookup` parameter is missing from the 'parameters' json " in {
    val eventWithoutSkipParameter =
      """{"properties": {}, "parameters":{"status":"status","reference":"TEST-REFERENCE","s3Bucket":"inputBucket","s3Key":"test.tar.gz"}}"""
    val initialS3State = List(S3Object("inputBucket", "test.tar.gz", fileBytes(inputMetadata(), 1)))

    val (res, _, _, _) = runLambda(initialS3State, event(body = eventWithoutSkipParameter.some))

    res.isRight should equal(true)
  }
}
