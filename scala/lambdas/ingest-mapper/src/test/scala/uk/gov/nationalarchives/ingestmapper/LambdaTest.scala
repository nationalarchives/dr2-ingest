package uk.gov.nationalarchives.ingestmapper

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import ujson.Obj
import uk.gov.nationalarchives.ingestmapper.MetadataService.Type.*
import uk.gov.nationalarchives.ingestmapper.testUtils.LambdaTestTestUtils
import uk.gov.nationalarchives.ingestmapper.testUtils.TestUtils.DynamoFilesTableItem
import uk.gov.nationalarchives.ingestmapper.testUtils.LambdaTestTestUtils.*

import java.net.URI
import java.util.UUID

class LambdaTest extends AnyFlatSpec {

  "handler" should "return the correct values from the lambda and upload the correct number of files and content to S3" in {
    val metadataResponse = getMetadata

    val (stateOutput, s3Objects) = (for {
      s3Ref <- Ref.of[IO, List[S3Object]](List(S3Object("input", "TEST/metadata.json", metadataResponse.metadata)))
      dynamoRef <- Ref.of[IO, List[Obj]](Nil)
      deps = dependencies(s3Ref, dynamoRef)
      stateData <- new Lambda().handler(input(), config, deps)
      s3Objects <- s3Ref.get
    } yield (stateData, s3Objects)).unsafeRunSync()

    stateOutput.groupId should be("TEST")
    stateOutput.batchId should be("TEST_0")
    stateOutput.metadataPackage should be(URI.create(s"s3://input/TEST/metadata.json"))
    stateOutput.assets.bucket should be("testInputStateBucket")
    stateOutput.assets.key should be("executionName/assets.json")
    stateOutput.folders.bucket should be("testInputStateBucket")
    stateOutput.folders.key should be("executionName/folders.json")

    val assetsFileContent = s3Objects.head.fileContent
    val foldersFileContent = s3Objects(1).fileContent

    s3Objects.map(_.key).filter(_.startsWith("executionName")) should be(List("executionName/assets.json", "executionName/folders.json"))

    val assetIdentifierOne = metadataResponse.identifiersOne.assetIdentifier
    val assetIdentifierTwo = metadataResponse.identifiersTwo.assetIdentifier
    val folderIdentifierOne = metadataResponse.identifiersOne.folderIdentifier
    val folderIdentifierTwo = metadataResponse.identifiersTwo.folderIdentifier
    ujson.read(foldersFileContent)
    assetsFileContent should be(s"""["$assetIdentifierOne","$assetIdentifierTwo"]""")
    ujson.read(foldersFileContent).arr.toList.map(e => UUID.fromString(e.str)).sorted should be(
      List(folderIdentifierOne, folderIdentifierTwo, UUID.fromString(uuids(1)), UUID.fromString(uuids.head), UUID.fromString(uuids(2))).sorted
    )

    val archiveFolders = stateOutput.archiveHierarchyFolders
    archiveFolders.size should be(5)
    archiveFolders.contains(folderIdentifierOne) should be(true)
    val expectedArchiveFolders =
      List(folderIdentifierOne, folderIdentifierTwo, UUID.fromString(uuids(1)), UUID.fromString(uuids.head), UUID.fromString(uuids(2)))
    expectedArchiveFolders.sorted.equals(archiveFolders.sorted) should be(true)
  }

  "handler" should "create the correct archive folder structure in dynamodb if there are multiple series in the batch" in {
    val metadataResponse = getMetadata
    val dynamoItems = dynamoItemsResponse(metadataResponse.copy(metadata = metadataResponse.metadata.replace(""""series": null""", """"series":"A 2"""")))
    val archiveFolderItems = dynamoItems.filter(a => a("type").str == "ArchiveFolder")
    val a1Folder = archiveFolderItems.find(a => a("title").str == "Test Title A 1").get
    val a2Folder = archiveFolderItems.find(a => a("title").str == "Test Title A 2").get

    a2Folder("parentPath") should equal(a1Folder("parentPath"))

    val topLevelFolders = archiveFolderItems.filter(a => a("id").str == a2Folder("parentPath").str)

    topLevelFolders.length should equal(1)
    val topLevelFolder = topLevelFolders.head

    topLevelFolder("childCount").num should equal(2)
    topLevelFolder("name").str should equal("A")
  }

  "handler" should "create the correct archive folder structure in dynamodb if there are multiple unknown series in the batch" in {
    val metadataResponse = getMetadata
    val updatedMetadata = metadataResponse.metadata.replace(""""series": null""", """"series":"Unknown"""").replace("A 1", "Unknown")
    val dynamoItems = dynamoItemsResponse(metadataResponse.copy(metadata = updatedMetadata))
    val archiveFolderItems = dynamoItems.filter(a => a("type").str == "ArchiveFolder")
    val secondLevelItems = archiveFolderItems.filter(a => a.value.get("title").map(_.str).contains("TestTitle"))

    secondLevelItems.length should equal(2)

    val firstUnknownSeries = secondLevelItems.head
    val secondUnknownSeries = secondLevelItems.last

    firstUnknownSeries("parentPath") should equal(secondUnknownSeries("parentPath"))

    val topLevelFolders = archiveFolderItems.filter(a => a("id").str == secondUnknownSeries("parentPath").str)

    topLevelFolders.length should equal(1)
    val topLevelFolder = topLevelFolders.head

    topLevelFolder("childCount").num should equal(2)
    topLevelFolder("name").str should equal("Unknown")
  }

  "handler" should "write the correct values to dynamo" in {
    val fixedTimeInSecs = 1712707200
    val metadataResponse = getMetadata
    val dynamoItems = dynamoItemsResponse(metadataResponse)

    dynamoItems.length should equal(11)
    case class TestResponses(dynamoResponse: Identifiers, uuidIndices: List[Int], series: String)
    List(
      TestResponses(metadataResponse.identifiersTwo, List(2, 3), "Unknown"),
      TestResponses(metadataResponse.identifiersOne, List(0, 1), "A 1")
    ).foreach { testResponse =>
      val (folderIdentifier, assetIdentifier, docxIdentifier, metadataIdentifier, originalMetadataFiles) = Tuple.fromProductTyped(testResponse.dynamoResponse)
      val departmentUuid = UUID.fromString(uuids(testResponse.uuidIndices.head))
      val seriesUuid = UUID.fromString(uuids(testResponse.uuidIndices.last))
      val expectedDepartment = testResponse.series.split(" ").head
      val seriesUnknown = testResponse.series == "Unknown"
      val expectedTitle = if seriesUnknown then "" else s"Test Title $expectedDepartment"
      val expectedIdCode = if seriesUnknown then "" else expectedDepartment
      val expectedDescription = if seriesUnknown then "" else s"TestDescription$expectedDepartment with 0"
      val topFolderPath = if seriesUnknown then departmentUuid.toString else s"$departmentUuid/$seriesUuid"

      checkDynamoItems(
        dynamoItems,
        DynamoFilesTableItem(
          "TEST_0",
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
          dynamoItems,
          DynamoFilesTableItem(
            "TEST_0",
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
        dynamoItems,
        DynamoFilesTableItem(
          "TEST_0",
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
        dynamoItems,
        DynamoFilesTableItem(
          "TEST_0",
          assetIdentifier,
          s"$topFolderPath/$folderIdentifier",
          "TestAssetName",
          Asset,
          "TestAssetTitle",
          "",
          None,
          2,
          fixedTimeInSecs,
          originalMetadataFiles = originalMetadataFiles
        )
      )
      checkDynamoItems(
        dynamoItems,
        DynamoFilesTableItem(
          "TEST_0",
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
        dynamoItems,
        DynamoFilesTableItem(
          "TEST_0",
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

  private def dynamoItemsResponse(metadataResponse: MetadataResponse) = {
    (for {
      s3Ref <- Ref.of[IO, List[S3Object]](List(S3Object("input", "TEST/metadata.json", metadataResponse.metadata)))
      dynamoRef <- Ref.of[IO, List[Obj]](Nil)
      deps = dependencies(s3Ref, dynamoRef)
      _ <- new Lambda().handler(input(), config, deps)
      dynamoItems <- dynamoRef.get
    } yield dynamoItems).unsafeRunSync()
  }

  "handler" should "not return an error if the discovery api is unavailable" in {
    val metadataResponse = getMetadata

    val dynamoItems = (for {
      s3Ref <- Ref.of[IO, List[S3Object]](List(S3Object("input", "TEST/metadata.json", metadataResponse.metadata)))
      dynamoRef <- Ref.of[IO, List[Obj]](Nil)
      deps = dependencies(s3Ref, dynamoRef, discoveryServiceException = true)
      _ <- new Lambda().handler(input(), config, deps)
      dynamoItems <- dynamoRef.get
    } yield dynamoItems).unsafeRunSync()

    val departmentId = UUID.fromString(uuids.head)
    val seriesId = UUID.fromString(uuids.tail.head)
    val fixedTimeInSeconds = 1712707200

    checkDynamoItems(
      dynamoItems,
      DynamoFilesTableItem(
        "TEST_0",
        departmentId,
        "",
        "A",
        ArchiveFolder,
        "",
        "",
        Some("A"),
        1,
        fixedTimeInSeconds
      )
    )

    checkDynamoItems(
      dynamoItems,
      DynamoFilesTableItem(
        "TEST_0",
        seriesId,
        departmentId.toString,
        "A 1",
        ArchiveFolder,
        "",
        "",
        Some("A 1"),
        1,
        fixedTimeInSeconds
      )
    )
  }

  "handler" should "return an error if the input files are not stored in S3" in {
    val ex = intercept[Exception] {
      (for {
        s3Ref <- Ref.of[IO, List[S3Object]](Nil)
        dynamoRef <- Ref.of[IO, List[Obj]](Nil)
        deps = dependencies(s3Ref, dynamoRef)
        _ <- new Lambda().handler(input("INVALID/"), config, deps)
      } yield ()).unsafeRunSync()
    }

    ex.getMessage should equal("Key INVALID/metadata.json not found in bucket input")
  }

  "handler" should "return an error if the dynamo table doesn't exist" in {
    val metadataResponse = getMetadata
    val ex = intercept[Exception] {
      (for {
        s3Ref <- Ref.of[IO, List[S3Object]](List(S3Object("input", "TEST/metadata.json", metadataResponse.metadata)))
        dynamoRef <- Ref.of[IO, List[Obj]](Nil)
        deps = dependencies(s3Ref, dynamoRef)
        _ <- new Lambda().handler(input(), config.copy(dynamoTableName = "invalid"), deps)
      } yield ()).unsafeRunSync()
    }

    ex.getMessage should equal("Table invalid does not exist")
  }

  "handler" should "return an error if the bag files from S3 are invalid" in {
    val ex = intercept[Exception] {
      (for {
        s3Ref <- Ref.of[IO, List[S3Object]](List(S3Object("input", "TEST/metadata.json", "{}")))
        dynamoRef <- Ref.of[IO, List[Obj]](Nil)
        deps = dependencies(s3Ref, dynamoRef)
        _ <- new Lambda().handler(input(), config, deps)
      } yield ()).unsafeRunSync()
    }
    ex.getMessage should equal("Expected ujson.Arr (data: {})")
  }
}
