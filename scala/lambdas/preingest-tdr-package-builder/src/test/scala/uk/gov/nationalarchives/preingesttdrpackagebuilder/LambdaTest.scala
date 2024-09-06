package uk.gov.nationalarchives.preingesttdrpackagebuilder

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import fs2.Collector.string
import fs2.hashing.HashAlgorithm
import io.circe.syntax.*
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.IngestLockTable
import uk.gov.nationalarchives.preingesttdrpackagebuilder.Lambda.*
import uk.gov.nationalarchives.preingesttdrpackagebuilder.TestUtils.{*, given}
import uk.gov.nationalarchives.utils.ExternalUtils.*

import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID

class LambdaTest extends AnyFlatSpec with ScalaCheckDrivenPropertyChecks:
  val config: Config = Config("", "", "cacheBucket", 1)
  case class FileName(prefix: String, suffix: String) {
    def fileString: String = s"$prefix.$suffix"
  }
  case class TestData(
      series: String,
      body: String,
      date: String,
      tdrRef: String,
      fileName: FileName,
      fileSize: Long,
      checksum: String,
      fileRef: String,
      groupId: String,
      batchId: String
  )
  val dateGen: Gen[String] = for {
    year <- Gen.posNum[Int]
    month <- Gen.choose(1, 12)
    day <- Gen.choose(1, 28)
    hour <- Gen.choose(0, 23)
    minute <- Gen.choose(0, 59)
    second <- Gen.choose(0, 59)
  } yield DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.of(year, month, day, hour, minute, second))

  val fileNameGen: Gen[FileName] = for {
    prefix <- Gen.asciiStr
    suffix <- Gen.asciiStr
  } yield FileName(prefix, suffix)

  val testCount: Iterator[Int] = (1 to 100).toList.iterator

  val testDataGen: Gen[TestData] = for {
    series <- Gen.asciiStr
    body <- Gen.asciiStr
    date <- dateGen
    tdrRef <- Gen.asciiStr
    fileName <- fileNameGen
    fileSize <- Gen.posNum[Long]
    checksum <- Gen.alphaStr
    fileRef <- Gen.asciiStr
    groupId <- Gen.asciiStr
    batchId <- Gen.alphaStr
  } yield TestData(series, body, date, tdrRef, fileName, fileSize, checksum, fileRef, groupId, batchId)

  given PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 100)

  forAll(testDataGen) { testData =>
    "lambda handler" should s"write the correct metadata to s3 ${testCount.next}" in {
      val archiveFolderId = UUID.fromString("dd28dde8-9d94-4843-8f46-fd3da71afaae")
      val contentFolderId = UUID.fromString("41ca3e41-e846-439f-9b2f-4397ffee646a")
      val fileId = UUID.fromString("4e03a500-4f29-47c5-9c08-26e12f631fd8")
      val metadataFileId = UUID.fromString("427789a1-e7af-4172-9fa7-02da1d60125f")
      val tdrFileId = UUID.fromString("a2834c9d-46e8-42d9-a300-2a4ed31c1e1a")
      val uuids = Iterator(archiveFolderId, contentFolderId, fileId, metadataFileId)
      val uuidIterator: () => UUID = () => uuids.next
      val tdrMetadata = TDRMetadata(
        testData.series,
        tdrFileId,
        None,
        testData.body,
        testData.date,
        testData.tdrRef,
        s"${testData.fileName.prefix}.${testData.fileName.suffix}",
        testData.fileSize,
        testData.checksum,
        testData.fileRef
      )
      val metadataChecksum = fs2.Stream
        .emits(tdrMetadata.asJson.noSpaces.getBytes)
        .through(fs2.hashing.Hashing[IO].hash(HashAlgorithm.SHA256))
        .flatMap(hash => fs2.Stream.emits(hash.bytes.toList))
        .through(fs2.text.hex.encode)
        .compile
        .to(string)
        .unsafeRunSync()

      val initialS3Objects = Map("key.metadata" -> tdrMetadata)
      val lockTableMessage = LockTableMessage(UUID.randomUUID(), URI.create("s3://bucket/key")).asJson.noSpaces
      val initialDynamoObjects = List(IngestLockTable(UUID.randomUUID(), testData.groupId, lockTableMessage))
      val input = Input(testData.groupId, testData.batchId, 1, 2)
      val (s3Contents, output) = runHandler(uuidIterator, initialS3Objects, initialDynamoObjects, input)

      def stripFileExtension(title: String) = if title.contains(".") then title.substring(0, title.lastIndexOf('.')) else title

      val metadataObjects = s3Contents(s"${testData.batchId}/metadata.json").asInstanceOf[List[MetadataObject]]
      val archiveFolderMetadataObject = metadataObjects.head.asInstanceOf[ArchiveFolderMetadataObject]
      val contentFolderMetadataObject = metadataObjects(1).asInstanceOf[ContentFolderMetadataObject]
      val assetMetadataObject = metadataObjects(2).asInstanceOf[AssetMetadataObject]
      val fileMetadataObject = metadataObjects(3).asInstanceOf[FileMetadataObject]
      val metadataFileMetadataObject = metadataObjects(4).asInstanceOf[FileMetadataObject]

      archiveFolderMetadataObject.id should equal(archiveFolderId)
      archiveFolderMetadataObject.name should equal(testData.series)
      archiveFolderMetadataObject.title should equal(None)
      archiveFolderMetadataObject.parentId should equal(None)
      archiveFolderMetadataObject.series should equal(testData.series)

      contentFolderMetadataObject.id should equal(contentFolderId)
      contentFolderMetadataObject.name should equal(testData.tdrRef)
      contentFolderMetadataObject.title should equal(None)
      contentFolderMetadataObject.parentId should equal(Option(archiveFolderId))

      assetMetadataObject.id should equal(tdrFileId)
      assetMetadataObject.parentId should equal(Option(contentFolderId))
      assetMetadataObject.title should equal(stripFileExtension(testData.fileName.fileString))
      assetMetadataObject.name should equal(testData.fileName.fileString)
      assetMetadataObject.originalFiles should equal(List(fileId))
      assetMetadataObject.originalMetadataFiles should equal(List(metadataFileId))
      assetMetadataObject.description should equal(None)
      assetMetadataObject.transferringBody should equal(testData.body)
      assetMetadataObject.transferCompleteDatetime should equal(LocalDateTime.parse(testData.date.replace(" ", "T")).atOffset(ZoneOffset.UTC))
      assetMetadataObject.upstreamSystem should equal("TDR")
      assetMetadataObject.digitalAssetSource should equal("Born Digital")
      assetMetadataObject.digitalAssetSubtype should equal("TDR")

      fileMetadataObject.id should equal(fileId)
      fileMetadataObject.parentId should equal(Option(tdrFileId))
      fileMetadataObject.title should equal(stripFileExtension(testData.fileName.fileString))
      fileMetadataObject.sortOrder should equal(1)
      fileMetadataObject.name should equal(testData.fileName.fileString)
      fileMetadataObject.fileSize should equal(testData.fileSize)
      fileMetadataObject.representationType should equal(RepresentationType.Preservation)
      fileMetadataObject.representationSuffix should equal(1)
      fileMetadataObject.location should equal(URI.create("s3://bucket/key"))
      fileMetadataObject.checksumSha256 should equal(testData.checksum)

      metadataFileMetadataObject.id should equal(metadataFileId)
      metadataFileMetadataObject.parentId should equal(Option(tdrFileId))
      metadataFileMetadataObject.title should equal(s"${testData.tdrRef}-metadata")
      metadataFileMetadataObject.sortOrder should equal(2)
      metadataFileMetadataObject.name should equal(s"${testData.tdrRef}-metadata.json")
      metadataFileMetadataObject.fileSize should equal(tdrMetadata.asJson.noSpaces.getBytes.length)
      metadataFileMetadataObject.representationType should equal(RepresentationType.Preservation)
      metadataFileMetadataObject.representationSuffix should equal(1)
      metadataFileMetadataObject.location should equal(URI.create("s3://bucket/key.metadata"))
      metadataFileMetadataObject.checksumSha256 should equal(metadataChecksum)

      output.groupId should equal(testData.groupId)
      output.batchId should equal(testData.batchId)
      output.retryCount should equal(2)
      output.packageMetadata should equal(URI.create(s"s3://cacheBucket/${testData.batchId}/metadata.json"))
    }
  }

  "lambda handler" should "return an error if the metadata list is empty" in {
    val ex = intercept[Exception] {
      runHandler()
    }
    ex.getMessage should equal("Metadata list for TST-123 is empty")
  }

  "lambda handler" should "return an error if the s3 download fails" in {
    val lockTableMessage = LockTableMessage(UUID.randomUUID(), URI.create("s3://bucket/key")).asJson.noSpaces
    val initialDynamoObjects = List(IngestLockTable(UUID.randomUUID(), "TST-123", lockTableMessage))

    val ex = intercept[Throwable] {
      runHandler(initialDynamoObjects = initialDynamoObjects, downloadError = true)
    }
    ex.getMessage should equal("Error downloading key.metadata from S3 bucket")
  }

  "lambda handler" should "return an error if the s3 upload fails" in {
    val lockTableMessage = LockTableMessage(UUID.randomUUID(), URI.create("s3://bucket/key")).asJson.noSpaces
    val initialDynamoObjects = List(IngestLockTable(UUID.randomUUID(), "TST-123", lockTableMessage))

    val tdrMetadata = TDRMetadata("", UUID.randomUUID, None, "", "2024-10-04 10:00:00", "", "test.txt", 1, "", "")
    val initialS3Objects = Map("key.metadata" -> tdrMetadata)
    val ex = intercept[Throwable] {
      runHandler(initialS3Objects = initialS3Objects, initialDynamoObjects = initialDynamoObjects, uploadError = true)
    }
    ex.getMessage should equal("Error uploading /metadata.json to cacheBucket")
  }

  "lambda handler" should "return an error if the dynamo query fails" in {
    val lockTableMessage = LockTableMessage(UUID.randomUUID(), URI.create("s3://bucket/key")).asJson.noSpaces

    val ex = intercept[Throwable] {
      runHandler(queryError = true)
    }
    ex.getMessage should equal("Dynamo has returned an error")
  }

  private def runHandler(
      uuidIterator: () => UUID = () => UUID.randomUUID,
      initialS3Objects: Map[String, TDRMetadata] = Map.empty,
      initialDynamoObjects: List[IngestLockTable] = Nil,
      input: Input = Input("TST-123", "", 1, 1),
      downloadError: Boolean = false,
      uploadError: Boolean = false,
      queryError: Boolean = false
  ): (Map[String, S3Objects], Output) = {
    (for {
      initialS3Objects <- Ref.of[IO, Map[String, S3Objects]](initialS3Objects)
      initialDynamoObjects <- Ref.of[IO, List[IngestLockTable]](initialDynamoObjects)
      dependencies = Dependencies(mockDynamoClient(initialDynamoObjects, queryError), mockS3(initialS3Objects, downloadError, uploadError), uuidIterator)
      output <- new Lambda().handler(input, config, dependencies)
      s3Objects <- initialS3Objects.get
    } yield (s3Objects, output)).unsafeRunSync()
  }
