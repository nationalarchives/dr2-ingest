package uk.gov.nationalarchives.ingestmapper

import cats.effect.unsafe.implicits.global
import com.github.tomakehurst.wiremock.WireMockServer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatestplus.mockito.MockitoSugar
import uk.gov.nationalarchives.ingestmapper.Lambda.*
import uk.gov.nationalarchives.ingestmapper.MetadataService.*
import uk.gov.nationalarchives.ingestmapper.MetadataService.Type.*
import uk.gov.nationalarchives.ingestmapper.testUtils.LambdaTestTestUtils
import uk.gov.nationalarchives.ingestmapper.testUtils.TestUtils.{DynamoRequestBody, DynamoFilesTableItem}
import upickle.default.*

import java.util.UUID
import scala.jdk.CollectionConverters.ListHasAsScala

class LambdaTest extends AnyFlatSpec with MockitoSugar with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    dynamoServer.start()
    s3Server.start()
  }

  override def afterEach(): Unit = {
    dynamoServer.resetAll()
    dynamoServer.stop()
    s3Server.resetAll()
    s3Server.stop()
  }

  val s3Server = new WireMockServer(9008)
  val dynamoServer = new WireMockServer(9009)

  "handler" should "return the correct values from the lambda" in {
    val testUtils = new LambdaTestTestUtils(dynamoServer, s3Server)
    import testUtils._
    val (folderIdentifier, assetIdentifier, _, _, _, _) = stubValidNetworkRequests()

    val stateData = new Lambda().handler(input, config, dependencies()).unsafeRunSync()
    val archiveFolders = stateData.archiveHierarchyFolders
    archiveFolders.size should be(3)
    archiveFolders.contains(folderIdentifier) should be(true)
    List(folderIdentifier, UUID.fromString(uuids.tail.head), UUID.fromString(uuids.head)).equals(archiveFolders) should be(true)

    stateData.contentFolders.isEmpty should be(true)

    stateData.contentAssets.size should be(1)
    stateData.contentAssets.head should equal(assetIdentifier)
  }

  "handler" should "write the correct values to dynamo" in {
    val testUtils = new LambdaTestTestUtils(dynamoServer, s3Server)
    val fixedTimeInSecs = 1712707200
    import testUtils._
    val (folderIdentifier, assetIdentifier, docxIdentifier, metadataIdentifier, originalFiles, originalMetadataFiles) = stubValidNetworkRequests()
    new Lambda().handler(input, config, dependencies()).unsafeRunSync()
    val dynamoRequestBodies = dynamoServer.getAllServeEvents.asScala.map(e => read[DynamoRequestBody](e.getRequest.getBodyAsString))
    dynamoRequestBodies.length should equal(1)
    val tableRequestItems = dynamoRequestBodies.head.RequestItems.test

    tableRequestItems.length should equal(6)
    checkDynamoItems(
      tableRequestItems,
      DynamoFilesTableItem("TEST", UUID.fromString(uuids.head), "", "A", ArchiveFolder, "Test Title A", "TestDescriptionA with 0", Some("A"), 1, fixedTimeInSecs)
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoFilesTableItem(
        "TEST",
        UUID.fromString(uuids.tail.head),
        uuids.head,
        "A 1",
        ArchiveFolder,
        "Test Title A 1",
        "TestDescriptionA 1 with 0",
        Some("A 1"),
        1,
        fixedTimeInSecs
      )
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoFilesTableItem(
        "TEST",
        folderIdentifier,
        s"${uuids.head}/${uuids.tail.head}",
        "TestName",
        ArchiveFolder,
        "TestTitle",
        "",
        None,
        1,
        fixedTimeInSecs,
        customMetadataAttribute2 = Option("customMetadataValue2")
      )
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoFilesTableItem(
        "TEST",
        assetIdentifier,
        s"${uuids.head}/${uuids.tail.head}/$folderIdentifier",
        "TestAssetName",
        Asset,
        "TestAssetTitle",
        "",
        None,
        2,
        fixedTimeInSecs,
        customMetadataAttribute2 = Option("customMetadataValueFromBagInfo"),
        attributeUniqueToBagInfo = Option("bagInfoAttributeValue"),
        originalFiles = originalFiles,
        originalMetadataFiles = originalMetadataFiles
      )
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoFilesTableItem(
        "TEST",
        docxIdentifier,
        s"${uuids.head}/${uuids.tail.head}/$folderIdentifier/$assetIdentifier",
        "Test.docx",
        File,
        "Test",
        "",
        None,
        0,
        fixedTimeInSecs,
        Option(1),
        customMetadataAttribute1 = Option("customMetadataValue1")
      )
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoFilesTableItem(
        "TEST",
        metadataIdentifier,
        s"${uuids.head}/${uuids.tail.head}/$folderIdentifier/$assetIdentifier",
        "TEST-metadata.json",
        File,
        "",
        "",
        None,
        0,
        fixedTimeInSecs,
        Option(2),
        Option("checksum"),
        Option("txt")
      )
    )
  }

  "handler" should "return an error if the discovery api is unavailable" in {
    val testUtils = new LambdaTestTestUtils(dynamoServer, s3Server)
    import testUtils._
    stubValidNetworkRequests()

    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies(true)).unsafeRunSync()
    }

    ex.getMessage should equal("Exception when sending request: GET http://localhost:9015/API/records/v1/collection/A")
  }

  "handler" should "return an error if the input files are not stored in S3" in {
    val testUtils = new LambdaTestTestUtils(dynamoServer, s3Server, s3Prefix = "INVALID/")
    import testUtils._
    stubValidNetworkRequests()
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies()).unsafeRunSync()
    }

    ex.getMessage should equal("null (Service: S3, Status Code: 404, Request ID: null)")
  }

  "handler" should "return an error if the dynamo table doesn't exist" in {
    val testUtils = new LambdaTestTestUtils(dynamoServer, s3Server)
    import testUtils._
    stubValidNetworkRequests("invalidTable")
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies()).unsafeRunSync()
    }

    ex.getMessage should equal("Service returned HTTP status code 404 (Service: DynamoDb, Status Code: 404, Request ID: null)")
  }

  "handler" should "return an error if the bag files from S3 are invalid" in {
    val testUtils = new LambdaTestTestUtils(dynamoServer, s3Server)
    import testUtils._
    stubInvalidNetworkRequests()
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies()).unsafeRunSync()
    }

    ex.getMessage should equal("Expected ujson.Arr (data: {})")
  }
}
