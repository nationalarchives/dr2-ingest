package uk.gov.nationalarchives.ingestmapper

import cats.effect.{IO, Ref}
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
import uk.gov.nationalarchives.ingestmapper.testUtils.TestUtils.{DynamoFilesTableItem, DynamoRequestBody}
import upickle.default.*

import java.net.URI
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

  "handler" should "return the correct values from the lambda and upload the correct number of files and content to S3" in {
    val testUtils = new LambdaTestTestUtils(dynamoServer, s3Server)
    import testUtils._
    val ((folderIdentifierOne, assetIdentifierOne, _, _, _, _), (folderIdentifierTwo, assetIdentifierTwo, _, _, _, _)) = stubValidNetworkRequests()

    val (stateOutput, keysAndFileContent) = (for {
      ref <- Ref.of[IO, List[(String, String)]](Nil)
      deps = dependencies(ref = ref)
      stateData <- new Lambda().handler(input, config, deps)
      s3KeysAndFileContent <- ref.get
      s3KeysAndFileContentMap = s3KeysAndFileContent.groupBy(_._1).view.mapValues(_.map(_._2)).toMap
    } yield (stateData, s3KeysAndFileContentMap)).unsafeRunSync()

    stateOutput.batchId should be("TEST")
    stateOutput.metadataPackage should be(URI.create(s"s3://input/TEST/metadata.json"))
    stateOutput.assets.bucket should be("testInputStateBucket")
    stateOutput.assets.key should be("executionName/assets.json")
    stateOutput.folders.bucket should be("testInputStateBucket")
    stateOutput.folders.key should be("executionName/folders.json")

    val keysUploadedToS3 = keysAndFileContent("keys")
    val fileContent = keysAndFileContent("fileContent")
    val assetsFileContent = fileContent.head
    val foldersFileContent = fileContent(1)

    keysUploadedToS3 should be(List("executionName/assets.json", "executionName/folders.json"))

    assetsFileContent should be(s"""["$assetIdentifierOne","$assetIdentifierTwo"]""")
    foldersFileContent should be(
      s"""["$folderIdentifierOne","$folderIdentifierTwo","${UUID.fromString(uuids(1))}","${UUID.fromString(uuids.head)}","${UUID.fromString(uuids(2))}"]"""
    )

    // Code below will be deleted after migration to the new process

    val archiveFolders = stateOutput.archiveHierarchyFolders
    archiveFolders.size should be(5)
    archiveFolders.contains(folderIdentifierOne) should be(true)
    val expectedArchiveFolders =
      List(folderIdentifierOne, folderIdentifierTwo, UUID.fromString(uuids(1)), UUID.fromString(uuids.head), UUID.fromString(uuids(2)))
    expectedArchiveFolders.sorted.equals(archiveFolders.sorted) should be(true)

    stateOutput.contentFolders.isEmpty should be(true)

    stateOutput.contentAssets.size should be(2)
    stateOutput.contentAssets.head should equal(assetIdentifierOne)
    stateOutput.contentAssets.last should equal(assetIdentifierTwo)
  }

  "handler" should "write the correct values to dynamo" in {
    val testUtils = new LambdaTestTestUtils(dynamoServer, s3Server)
    val fixedTimeInSecs = 1712707200
    import testUtils._
    val (responseOne, responseTwo) = stubValidNetworkRequests()
    (for {
      ref <- Ref.of[IO, List[(String, String)]](Nil)
      deps = dependencies(ref = ref)
      _ <- new Lambda().handler(input, config, deps)
    } yield ()).unsafeRunSync()
    val dynamoRequestBodies = dynamoServer.getAllServeEvents.asScala.map(e => read[DynamoRequestBody](e.getRequest.getBodyAsString))
    dynamoRequestBodies.length should equal(1)
    val tableRequestItems = dynamoRequestBodies.head.RequestItems.test

    tableRequestItems.length should equal(11)
    case class TestResponses(dynamoResponse: DynamoResponse, uuidIndices: List[Int], series: String)
    List(
      TestResponses(responseTwo, List(2, 3), "Unknown"),
      TestResponses(responseOne, List(0, 1), "A 1")
    ).foreach { testResponse =>
      val (folderIdentifier, assetIdentifier, docxIdentifier, metadataIdentifier, originalFiles, originalMetadataFiles) = testResponse.dynamoResponse
      val departmentUuid = UUID.fromString(uuids(testResponse.uuidIndices.head))
      val seriesUuid = UUID.fromString(uuids(testResponse.uuidIndices.last))
      val expectedDepartment = testResponse.series.split(" ").head
      val seriesUnknown = testResponse.series == "Unknown"
      val expectedTitle = if seriesUnknown then "" else s"Test Title $expectedDepartment"
      val expectedIdCode = if seriesUnknown then "" else expectedDepartment
      val expectedDescription = if seriesUnknown then "" else s"TestDescription$expectedDepartment with 0"
      val topFolderPath = if seriesUnknown then departmentUuid.toString else s"$departmentUuid/$seriesUuid"

      checkDynamoItems(
        tableRequestItems,
        DynamoFilesTableItem(
          "TEST",
          departmentUuid,
          "",
          expectedDepartment,
          ArchiveFolder,
          expectedTitle,
          expectedDescription,
          Some(expectedIdCode),
          1,
          fixedTimeInSecs
        )
      )
      if testResponse.series != "Unknown" then
        checkDynamoItems(
          tableRequestItems,
          DynamoFilesTableItem(
            "TEST",
            seriesUuid,
            departmentUuid.toString,
            testResponse.series,
            ArchiveFolder,
            s"Test Title ${testResponse.series}",
            s"TestDescription${testResponse.series} with 0",
            Some(testResponse.series),
            1,
            fixedTimeInSecs
          )
        )
      checkDynamoItems(
        tableRequestItems,
        DynamoFilesTableItem(
          "TEST",
          folderIdentifier,
          topFolderPath,
          "TestName",
          ArchiveFolder,
          "TestTitle",
          "",
          None,
          1,
          fixedTimeInSecs
        )
      )
      checkDynamoItems(
        tableRequestItems,
        DynamoFilesTableItem(
          "TEST",
          assetIdentifier,
          s"$topFolderPath/$folderIdentifier",
          "TestAssetName",
          Asset,
          "TestAssetTitle",
          "",
          None,
          2,
          fixedTimeInSecs,
          originalFiles = originalFiles,
          originalMetadataFiles = originalMetadataFiles
        )
      )
      checkDynamoItems(
        tableRequestItems,
        DynamoFilesTableItem(
          "TEST",
          docxIdentifier,
          s"$topFolderPath/$folderIdentifier/$assetIdentifier",
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
          s"$topFolderPath/$folderIdentifier/$assetIdentifier",
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

  }

  "handler" should "return an error if the discovery api is unavailable" in {
    val testUtils = new LambdaTestTestUtils(dynamoServer, s3Server)
    import testUtils._
    stubValidNetworkRequests()

    val ex = intercept[Exception] {
      (for {
        ref <- Ref.of[IO, List[(String, String)]](Nil)
        deps = dependencies(true, ref = ref)
        _ <- new Lambda().handler(input, config, deps)
      } yield ()).unsafeRunSync()
    }

    ex.getMessage should equal("Exception when sending request: GET http://localhost:9015/API/records/v1/collection/A")
  }

  "handler" should "return an error if the input files are not stored in S3" in {
    val testUtils = new LambdaTestTestUtils(dynamoServer, s3Server, s3Prefix = "INVALID/")
    import testUtils._
    stubValidNetworkRequests()
    val ex = intercept[Exception] {
      (for {
        ref <- Ref.of[IO, List[(String, String)]](Nil)
        deps = dependencies(ref = ref)
        _ <- new Lambda().handler(input, config, deps)
      } yield ()).unsafeRunSync()
    }

    ex.getMessage.contains("(Service: S3, Status Code: 404, Request ID: null)") should equal(true)
  }

  "handler" should "return an error if the dynamo table doesn't exist" in {
    val testUtils = new LambdaTestTestUtils(dynamoServer, s3Server)
    import testUtils._
    stubValidNetworkRequests("invalidTable")
    val ex = intercept[Exception] {
      (for {
        ref <- Ref.of[IO, List[(String, String)]](Nil)
        deps = dependencies(ref = ref)
        _ <- new Lambda().handler(input, config, deps)
      } yield ()).unsafeRunSync()
    }

    ex.getMessage should equal("Service returned HTTP status code 404 (Service: DynamoDb, Status Code: 404, Request ID: null)")
  }

  "handler" should "return an error if the bag files from S3 are invalid" in {
    val testUtils = new LambdaTestTestUtils(dynamoServer, s3Server)
    import testUtils._
    stubInvalidNetworkRequests()
    val ex = intercept[Exception] {
      (for {
        ref <- Ref.of[IO, List[(String, String)]](Nil)
        deps = dependencies(ref = ref)
        _ <- new Lambda().handler(input, config, deps)
      } yield ()).unsafeRunSync()
    }
    ex.getMessage should equal("Expected ujson.Arr (data: {})")
  }
}
