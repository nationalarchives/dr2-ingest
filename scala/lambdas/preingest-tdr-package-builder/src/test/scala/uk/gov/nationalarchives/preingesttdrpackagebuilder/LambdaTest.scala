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
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{Checksum, IngestLockTableItem}
import uk.gov.nationalarchives.preingesttdrpackagebuilder.Lambda.*
import uk.gov.nationalarchives.preingesttdrpackagebuilder.TestUtils.{*, given}
import uk.gov.nationalarchives.utils.ExternalUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.SourceSystem.TDR

import java.net.URI
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.UUID

class LambdaTest extends AnyFlatSpec with ScalaCheckDrivenPropertyChecks:
  val config: Config = Config("", "", "cacheBucket", 1, TDR)
  private val dateTimeNow: Instant = Instant.now()
  case class FileName(prefix: String, suffix: String) {
    def fileString: String = s"$prefix.$suffix"
  }

  private def checksum(fingerprint: String) = List(Checksum("sha256", fingerprint))

  case class TestData(
      series: String,
      body: String,
      date: String,
      tdrRef: String,
      fileName: FileName,
      fileSize: Long,
      checksums: List[Checksum],
      fileRef: String,
      groupId: String,
      batchId: String,
      filePath: String,
      driBatchRef: Option[String]
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

  val checksumGen: Gen[Checksum] = for {
    algorithm <- Gen.alphaStr
    fingerprint <- Gen.alphaStr
  } yield Checksum(algorithm, fingerprint)

  val testCount: Iterator[Int] = (1 to 100).toList.iterator

  val testDataGen: Gen[TestData] = for {
    series <- Gen.asciiStr
    body <- Gen.asciiStr
    date <- dateGen
    tdrRef <- Gen.asciiStr
    fileName <- fileNameGen
    fileSize <- Gen.posNum[Long]
    checksum <- checksumGen
    fileRef <- Gen.asciiStr
    groupId <- Gen.asciiStr
    batchId <- Gen.alphaStr
    filePath <- Gen.asciiStr
    driBatchRef <- Gen.option(Gen.asciiStr)
  } yield TestData(series, body, date, tdrRef, fileName, fileSize, List(checksum), fileRef, groupId, batchId, filePath, driBatchRef)

  given PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 100)

  forAll(testDataGen) { testData =>
    "lambda handler" should s"write the correct metadata to s3 ${testCount.next}" in {
      val archiveFolderId = UUID.fromString("dd28dde8-9d94-4843-8f46-fd3da71afaae")
      val fileId = UUID.fromString("4e03a500-4f29-47c5-9c08-26e12f631fd8")
      val metadataFileId = UUID.fromString("427789a1-e7af-4172-9fa7-02da1d60125f")
      val tdrFileId = UUID.fromString("a2834c9d-46e8-42d9-a300-2a4ed31c1e1a")
      val uuids = Iterator(metadataFileId, fileId, archiveFolderId)
      val uuidIterator: () => UUID = () => uuids.next
      val tdrMetadata = PackageMetadata(
        testData.series,
        tdrFileId,
        None,
        None,
        Option(testData.body),
        testData.date,
        testData.tdrRef,
        s"${testData.fileName.prefix}.${testData.fileName.suffix}",
        testData.checksums,
        testData.fileRef,
        testData.filePath,
        testData.driBatchRef
      )
      val metadataChecksum = fs2.Stream
        .emits(tdrMetadata.asJson.noSpaces.getBytes)
        .through(fs2.hashing.Hashing[IO].hash(HashAlgorithm.SHA256))
        .flatMap(hash => fs2.Stream.emits(hash.bytes.toList))
        .through(fs2.text.hex.encode)
        .compile
        .to(string)
        .unsafeRunSync()

      val potentialLockTableMessageId = Some("messageId")
      val initialS3Objects = Map("key.metadata" -> tdrMetadata, "key" -> MockTdrFile(testData.fileSize))
      val lockTableMessageAsString = new LockTableMessage(UUID.randomUUID(), URI.create("s3://bucket/key"), potentialLockTableMessageId).asJson.noSpaces
      val initialDynamoObjects = List(IngestLockTableItem(UUID.randomUUID(), testData.groupId, lockTableMessageAsString, dateTimeNow.toString))
      val input = Input(testData.groupId, testData.batchId, 1, 2)
      val (s3Contents, output) = runHandler(uuidIterator, initialS3Objects, initialDynamoObjects, input)

      def stripFileExtension(title: String) = if title.contains(".") then title.substring(0, title.lastIndexOf('.')) else title

      val metadataObjects: List[MetadataObject] = s3Contents(s"${testData.batchId}/metadata.json").asInstanceOf[List[MetadataObject]]
      val contentFolderMetadataObject = metadataObjects.collect { case contentFolderMetadataObject: ContentFolderMetadataObject => contentFolderMetadataObject }.head
      val assetMetadataObject = metadataObjects.collect { case assetMetadataObject: AssetMetadataObject => assetMetadataObject }.head
      val fileMetadataObjects = metadataObjects.collect { case fileMetadataObject: FileMetadataObject => fileMetadataObject }
      val fileMetadataObject = fileMetadataObjects.filterNot(_.name.endsWith("-metadata.json")).head
      val metadataFileMetadataObject = fileMetadataObjects.filter(_.name.endsWith("-metadata.json")).head

      contentFolderMetadataObject.name should equal(testData.tdrRef)
      contentFolderMetadataObject.title should equal(None)
      contentFolderMetadataObject.parentId should equal(None)
      contentFolderMetadataObject.series should equal(Option(testData.series))

      assetMetadataObject.id should equal(tdrFileId)
      assetMetadataObject.parentId should equal(Option(archiveFolderId))
      assetMetadataObject.title should equal(testData.fileName.fileString)
      assetMetadataObject.name should equal(tdrFileId.toString)
      assetMetadataObject.originalFiles should equal(List(fileId))
      assetMetadataObject.originalMetadataFiles should equal(List(metadataFileId))
      assetMetadataObject.description should equal(None)
      assetMetadataObject.transferringBody should equal(testData.body)
      assetMetadataObject.transferCompleteDatetime should equal(LocalDateTime.parse(testData.date.replace(" ", "T")).atOffset(ZoneOffset.UTC))
      assetMetadataObject.upstreamSystem should equal(TDR)
      assetMetadataObject.digitalAssetSource should equal("Born Digital")
      assetMetadataObject.digitalAssetSubtype should equal(None)
      assetMetadataObject.correlationId should equal(potentialLockTableMessageId)
      def checkIdField(name: String, value: String) =
        assetMetadataObject.idFields.find(_.name == name).map(_.value).get should equal(value)
      checkIdField("Code", s"${testData.series}/${testData.fileRef}")
      checkIdField("UpstreamSystemReference", testData.fileRef)
      checkIdField("BornDigitalRef", testData.fileRef)
      checkIdField("ConsignmentReference", testData.tdrRef)
      checkIdField("RecordID", tdrFileId.toString)

      fileMetadataObject.parentId should equal(Option(tdrFileId))
      fileMetadataObject.title should equal(testData.fileName.fileString)
      fileMetadataObject.sortOrder should equal(1)
      fileMetadataObject.name should equal(testData.fileName.fileString)
      fileMetadataObject.fileSize should equal(testData.fileSize)
      fileMetadataObject.representationType should equal(RepresentationType.Preservation)
      fileMetadataObject.representationSuffix should equal(1)
      fileMetadataObject.location should equal(URI.create("s3://bucket/key"))
      fileMetadataObject.checksums should equal(testData.checksums)

      metadataFileMetadataObject.parentId should equal(Option(tdrFileId))
      metadataFileMetadataObject.title should equal(s"$tdrFileId-metadata")
      metadataFileMetadataObject.sortOrder should equal(2)
      metadataFileMetadataObject.name should equal(s"$tdrFileId-metadata.json")
      metadataFileMetadataObject.fileSize should equal(tdrMetadata.asJson.noSpaces.getBytes.length)
      metadataFileMetadataObject.representationType should equal(RepresentationType.Preservation)
      metadataFileMetadataObject.representationSuffix should equal(1)
      metadataFileMetadataObject.location should equal(URI.create("s3://bucket/key.metadata"))
      metadataFileMetadataObject.checksums.head should equal(Checksum("sha256", metadataChecksum))

      output.groupId should equal(testData.groupId)
      output.batchId should equal(testData.batchId)
      output.retryCount should equal(2)
      output.metadataPackage should equal(URI.create(s"s3://cacheBucket/${testData.batchId}/metadata.json"))
      output.retrySfnArn should equal("")
    }
  }

  "lambda handler" should "create two content folders for two consignments with 10 files each" in {
    val archiveFolderId = UUID.fromString("dd28dde8-9d94-4843-8f46-fd3da71afaae")
    val fileId = UUID.fromString("4e03a500-4f29-47c5-9c08-26e12f631fd8")
    val metadataFileId = UUID.fromString("427789a1-e7af-4172-9fa7-02da1d60125f")
    val tdrFileId = UUID.fromString("a2834c9d-46e8-42d9-a300-2a4ed31c1e1a")
    val uuids = Iterator(metadataFileId, fileId, archiveFolderId)
    val uuidIterator: () => UUID = () => UUID.randomUUID
    val tdrMetadataOne = PackageMetadata(
      "TST 123",
      tdrFileId,
      None,
      None,
      Option("body"),
      "2024-10-18 00:00:01",
      "TDR-ABCD",
      s"file.txt",
      checksum("checksum"),
      "reference",
      "/path/to/file.txt",
      None
    )

    val tdrMetadataTwo = tdrMetadataOne.copy(consignmentReference = "TDR-EFGH")

    val groupId = UUID.randomUUID.toString
    val batchId = s"${groupId}_0"
    val initialS3Objects: Map[String, S3Objects] = Map.from((1 to 10).toList.flatMap { idx =>
      List(
        s"keyOne$idx.metadata" -> tdrMetadataOne,
        s"keyOne$idx" -> MockTdrFile(1),
        s"keyTwo$idx.metadata" -> tdrMetadataTwo,
        s"keyTwo$idx" -> MockTdrFile(1)
      )
    })

    val initialDynamoObjects = (1 to 10).toList.flatMap { idx =>
      val lockTableMessageOne = NotificationMessage(UUID.randomUUID(), URI.create(s"s3://bucket/keyOne$idx")).asJson.noSpaces
      val lockTableMessageTwo = NotificationMessage(UUID.randomUUID(), URI.create(s"s3://bucket/keyTwo$idx")).asJson.noSpaces
      List(
        IngestLockTableItem(UUID.randomUUID(), groupId, lockTableMessageOne, dateTimeNow.toString),
        IngestLockTableItem(UUID.randomUUID(), groupId, lockTableMessageTwo, dateTimeNow.toString)
      )
    }
    val input = Input(groupId, batchId, 1, 2)
    val (s3Contents, output) = runHandler(uuidIterator, initialS3Objects, initialDynamoObjects, input)

    val metadataObjects: List[MetadataObject] = s3Contents(s"$batchId/metadata.json").asInstanceOf[List[MetadataObject]]
    val assetMetadataObjects = metadataObjects.collect { case assetMetadataObject: AssetMetadataObject => assetMetadataObject }
    val contentFolderMetadataObjects = metadataObjects.collect { case contentFolderMetadataObject: ContentFolderMetadataObject => contentFolderMetadataObject }

    contentFolderMetadataObjects.size should equal(2)

    assetMetadataObjects.count(_.parentId == Option(contentFolderMetadataObjects.head.id)) should equal(10)
    assetMetadataObjects.count(_.parentId == Option(contentFolderMetadataObjects.last.id)) should equal(10)

  }

  "lambda handler" should "return an error if the metadata list is empty" in {
    val ex = intercept[Exception] {
      runHandler()
    }
    ex.getMessage should equal("Metadata list for TST-123 is empty")
  }

  "lambda handler" should "return an error if the s3 download fails" in {
    val lockTableMessageAsString = new LockTableMessage(UUID.randomUUID(), URI.create("s3://bucket/key")).asJson.noSpaces
    val initialDynamoObjects = List(IngestLockTableItem(UUID.randomUUID(), "TST-123", lockTableMessageAsString, dateTimeNow.toString))

    val ex = intercept[Throwable] {
      runHandler(initialDynamoObjects = initialDynamoObjects, downloadError = true)
    }
    ex.getMessage should equal("Error downloading key.metadata from S3 bucket")
  }

  "lambda handler" should "return an error if the s3 upload fails" in {
    val lockTableMessageAsString = new LockTableMessage(UUID.randomUUID(), URI.create("s3://bucket/key")).asJson.noSpaces
    val initialDynamoObjects = List(IngestLockTableItem(UUID.randomUUID(), "TST-123", lockTableMessageAsString, dateTimeNow.toString))

    val tdrMetadata = PackageMetadata("", UUID.randomUUID, None, None, Option(""), "2024-10-04 10:00:00", "", "test.txt", checksum(""), "", "", None)
    val initialS3Objects = Map("key.metadata" -> tdrMetadata, "key" -> MockTdrFile(1))
    val ex = intercept[Throwable] {
      runHandler(initialS3Objects = initialS3Objects, initialDynamoObjects = initialDynamoObjects, uploadError = true)
    }
    ex.getMessage should equal("Error uploading /metadata.json to cacheBucket")
  }

  "lambda handler" should "return an error if the dynamo query fails" in {
    val lockTableMessageAsString = new LockTableMessage(UUID.randomUUID(), URI.create("s3://bucket/key")).asJson.noSpaces

    val ex = intercept[Throwable] {
      runHandler(queryError = true)
    }
    ex.getMessage should equal("Dynamo has returned an error")
  }

  private def runHandler(
      uuidIterator: () => UUID = () => UUID.randomUUID,
      initialS3Objects: Map[String, S3Objects] = Map.empty,
      initialDynamoObjects: List[IngestLockTableItem] = Nil,
      input: Input = Input("TST-123", "", 1, 1),
      downloadError: Boolean = false,
      uploadError: Boolean = false,
      queryError: Boolean = false
  ): (Map[String, S3Objects], Output) = {
    (for {
      initialS3Objects <- Ref.of[IO, Map[String, S3Objects]](initialS3Objects)
      initialDynamoObjects <- Ref.of[IO, List[IngestLockTableItem]](initialDynamoObjects)
      dependencies = Dependencies(mockDynamoClient(initialDynamoObjects, queryError), mockS3(initialS3Objects, downloadError, uploadError), uuidIterator)
      output <- new Lambda().handler(input, config, dependencies)
      s3Objects <- initialS3Objects.get
    } yield (s3Objects, output)).unsafeRunSync()
  }
