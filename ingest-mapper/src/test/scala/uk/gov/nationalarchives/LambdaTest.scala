package uk.gov.nationalarchives

import com.github.tomakehurst.wiremock.WireMockServer
import org.apache.commons.io.output.ByteArrayOutputStream
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import uk.gov.nationalarchives.Lambda._
import uk.gov.nationalarchives.MetadataService._
import uk.gov.nationalarchives.testUtils.LambdaTestTestUtils
import uk.gov.nationalarchives.testUtils.TestUtils.{DynamoRequestBody, DynamoTable}
import upickle.default
import upickle.default._

import java.io.ByteArrayInputStream
import java.util.UUID
import scala.jdk.CollectionConverters.ListHasAsScala

class LambdaTest extends AnyFlatSpec with MockitoSugar with BeforeAndAfterEach {

  implicit val stateDataReader: default.Reader[StateOutput] = macroR[StateOutput]

  override def beforeEach(): Unit = {
    dynamoServer.resetAll()
    dynamoServer.start()
    s3Server.resetAll()
    s3Server.start()
    discoveryServer.resetAll()
    discoveryServer.start()
  }

  val s3Server = new WireMockServer(9003)
  val dynamoServer = new WireMockServer(9005)
  val discoveryServer = new WireMockServer(9004)

  private val testUtils = new LambdaTestTestUtils(dynamoServer, s3Server, discoveryServer)
  import testUtils._

  "handleRequest" should "return the correct values from the lambda" in {
    val (folderIdentifier, assetIdentifier, _, _, _, _) = stubValidNetworkRequests()

    val os = new ByteArrayOutputStream()
    IngestMapperTest().handleRequest(defaultInputStream, os, null)
    val stateData = read[StateOutput](os.toByteArray.map(_.toChar).mkString)
    val archiveFolders = stateData.archiveHierarchyFolders
    archiveFolders.size should be(3)
    archiveFolders.contains(folderIdentifier) should be(true)
    List(folderIdentifier, UUID.fromString(uuids.tail.head), UUID.fromString(uuids.head)).equals(archiveFolders) should be(true)

    stateData.contentFolders.isEmpty should be(true)

    stateData.contentAssets.size should be(1)
    stateData.contentAssets.head should equal(assetIdentifier)
  }

  "handleRequest" should "write the correct values to dynamo" in {
    val (folderIdentifier, assetIdentifier, docxIdentifier, metadataIdentifier, originalFiles, originalMetadataFiles) = stubValidNetworkRequests()
    val os = new ByteArrayOutputStream()
    IngestMapperTest().handleRequest(defaultInputStream, os, null)
    val dynamoRequestBodies = dynamoServer.getAllServeEvents.asScala.map(e => read[DynamoRequestBody](e.getRequest.getBodyAsString))
    dynamoRequestBodies.length should equal(1)
    val tableRequestItems = dynamoRequestBodies.head.RequestItems.test

    tableRequestItems.length should equal(6)
    checkDynamoItems(
      tableRequestItems,
      DynamoTable("TEST", UUID.fromString(uuids.head), "", "A", ArchiveFolder, "Test Title A", "TestDescriptionA with 0", Some("A"))
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoTable("TEST", UUID.fromString(uuids.tail.head), uuids.head, "A 1", ArchiveFolder, "Test Title A 1", "TestDescriptionA 1 with 0", Some("A 1"))
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoTable(
        "TEST",
        folderIdentifier,
        s"${uuids.head}/${uuids.tail.head}",
        "TestName",
        ArchiveFolder,
        "TestTitle",
        "",
        None,
        customMetadataAttribute2 = Option("customMetadataValue2")
      )
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoTable(
        "TEST",
        assetIdentifier,
        s"${uuids.head}/${uuids.tail.head}/$folderIdentifier",
        "TestAssetName",
        Asset,
        "TestAssetTitle",
        "",
        None,
        customMetadataAttribute2 = Option("customMetadataValueFromBagInfo"),
        attributeUniqueToBagInfo = Option("bagInfoAttributeValue"),
        originalFiles = originalFiles,
        originalMetadataFiles = originalMetadataFiles
      )
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoTable(
        "TEST",
        docxIdentifier,
        s"${uuids.head}/${uuids.tail.head}/$folderIdentifier/$assetIdentifier",
        "Test.docx",
        File,
        "Test",
        "",
        None,
        Option(1),
        customMetadataAttribute1 = Option("customMetadataValue1")
      )
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoTable(
        "TEST",
        metadataIdentifier,
        s"${uuids.head}/${uuids.tail.head}/$folderIdentifier/$assetIdentifier",
        "TEST-metadata.json",
        File,
        "",
        "",
        None,
        Option(2),
        Option("checksum"),
        Option("txt")
      )
    )
  }

  "handleRequest" should "return an error if the discovery api is unavailable" in {
    stubValidNetworkRequests()
    discoveryServer.stop()
    val os = new ByteArrayOutputStream()
    val ex = intercept[Exception] {
      IngestMapperTest().handleRequest(defaultInputStream, os, null)
    }

    ex.getMessage should equal("Exception when sending request: GET http://localhost:9004/API/records/v1/collection/A")
  }

  "handleRequest" should "return an error if the input files are not stored in S3" in {
    stubValidNetworkRequests()
    val inJson =
      s"""{
         |  "batchId": "TEST",
         |  "s3Bucket": "$inputBucket",
         |  "s3Prefix" : "INVALID/",
         |  "department": "A",
         |  "series": "A 1"
         |}""".stripMargin
    val is = new ByteArrayInputStream(inJson.getBytes())
    val os = new ByteArrayOutputStream()
    val ex = intercept[Exception] {
      IngestMapperTest().handleRequest(is, os, null)
    }

    ex.getMessage should equal("null (Service: S3, Status Code: 404, Request ID: null)")
  }

  "handleRequest" should "return an error if the dynamo table doesn't exist" in {
    stubValidNetworkRequests("invalidTable")
    val os = new ByteArrayOutputStream()
    val ex = intercept[Exception] {
      IngestMapperTest().handleRequest(defaultInputStream, os, null)
    }

    ex.getMessage should equal("Service returned HTTP status code 404 (Service: DynamoDb, Status Code: 404, Request ID: null)")
  }

  "handleRequest" should "return an error if the bag files from S3 are invalid" in {
    stubInvalidNetworkRequests()
    val os = new ByteArrayOutputStream()
    val ex = intercept[Exception] {
      IngestMapperTest().handleRequest(defaultInputStream, os, null)
    }

    ex.getMessage should equal("Expected ujson.Arr (data: {})")
  }
}
