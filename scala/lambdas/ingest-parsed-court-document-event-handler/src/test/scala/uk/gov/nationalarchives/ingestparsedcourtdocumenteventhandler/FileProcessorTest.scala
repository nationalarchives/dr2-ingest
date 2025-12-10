package uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, DecodingFailure, HCursor, ParsingFailure}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.*
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Checksum
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.FileProcessor.*
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.TestUtils.*
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.UriProcessor.ParsedUri
import uk.gov.nationalarchives.utils.ExternalUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.SourceSystem.`TRE: FCL Parser workflow`

import java.net.URI
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.{Base64, HexFormat, UUID}

class FileProcessorTest extends AnyFlatSpec with TableDrivenPropertyChecks {
  val testTarGz: ByteBuffer = fileBytes(inputMetadata(UUID.fromString("321e5ea8-27ff-4941-97d2-5c315aace704")))
  val reference = "TEST-REFERENCE"
  val potentialUri: Some[String] = Some("http://example.com/id/abcde/2023/1537")

  given Decoder[Type] = (c: HCursor) =>
    for {
      decodedType <- c.downField("type").as[String]
    } yield {
      decodedType match {
        case "ArchiveFolder" => Type.ArchiveFolder
        case "Asset"         => Type.Asset
        case "File"          => Type.File
      }
    }

  private val tdrParams = Map(
    "Document-Checksum-sha256" -> "abcde",
    "Source-Organization" -> "test-organisation",
    "Internal-Sender-Identifier" -> "test-identifier",
    "Consignment-Export-Datetime" -> "2023-10-31T13:40:54Z",
    "UUID" -> "24190792-a2e5-43a0-a9e9-6a0580905d90"
  )

  val metadataJson: String =
    s"""{"parameters":{"TDR": ${tdrParams.asJson.noSpaces},
       |"TRE":{"reference":"$reference","payload":{"filename":"Test.docx"}},
       |"PARSER":{"cite":"cite","uri":"https://example.com","court":"test","date":"2023-07-26","name":"test"}}}""".stripMargin

  private val uuids: List[UUID] = List(
    UUID.fromString("6e827e19-6a33-46c3-8730-b242c203d8c1"),
    UUID.fromString("49e4a726-6297-4f8e-8867-fb50bd5acd86"),
    UUID.fromString("593cf06e-0832-49e0-a20f-d259c8192d70")
  )

  def createS3Client(s3Objects: List[S3Object], errors: Option[Errors] = None): DAS3Client[IO] = s3Client(Ref.unsafe[IO, List[S3Object]](s3Objects), errors)

  case class UUIDGenerator() {
    val uuidsIterator: Iterator[UUID] = uuids.iterator

    val uuidGenerator: () => UUID = () => uuidsIterator.next()
  }

  def convertChecksumToS3Format(cs: Option[String]): String =
    cs.map { c =>
      Base64.getEncoder
        .encode(HexFormat.of().parseHex(c))
        .map(_.toChar)
        .mkString
    }.orNull

  "copyFilesFromDownloadToUploadBucket" should "return the correct file metadata for a valid tar.gz file" in {
    val generator = UUIDGenerator()
    val s3 = createS3Client(S3Object("download", "key", testTarGz) :: Nil)

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, generator.uuidGenerator)
    val res = fileProcessor.copyFilesFromDownloadToUploadBucket("key").unsafeRunSync()

    res.size should equal(3)
    val docx = res.get(s"$reference/Test.docx")
    val metadata = res.get(s"$reference/TRE-$reference-metadata.json")
    val unusedTreFile = res.get(s"$reference/unused.txt")
    docx.isDefined should be(true)
    metadata.isDefined should be(true)
    unusedTreFile.isDefined should be(true)

    val docxInfo = docx.get
    docxInfo.id should equal(uuids.head)
    docxInfo.fileName should equal("Test.docx")
    docxInfo.fileSize should equal(100)
    docxInfo.sha256Checksum should equal("2816597888e4a0d3a36b82b83316ab32680eb8f00f8cd3b904d681246d285a0e")

    val unusedTreFileInfo = unusedTreFile.get
    unusedTreFileInfo.id should equal(uuids(1))
    unusedTreFileInfo.fileName should equal("unused.txt")
    unusedTreFileInfo.fileSize should equal(0)
    unusedTreFileInfo.sha256Checksum should equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

    val metadataInfo = metadata.get
    metadataInfo.id should equal(uuids(2))
    metadataInfo.fileName should equal(s"TRE-$reference-metadata.json")
    metadataInfo.fileSize should equal(448)
    metadataInfo.sha256Checksum should equal("26d0afe6da31b86ff5d4c08b17a52081be24f8febfe79b14b8d181549afbe641")
  }

  "copyFilesFromDownloadToUploadBucket" should "return an error if the downloaded file is not a valid tar.gz" in {
    val s3 = createS3Client(S3Object("download", "key", ByteBuffer.wrap("invalid".getBytes)) :: Nil)

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)
    val ex = intercept[Exception] {
      fileProcessor.copyFilesFromDownloadToUploadBucket("key").unsafeRunSync()
    }
    ex.getMessage should equal("UpStream failed")
  }

  "copyFilesFromDownloadToUploadBucket" should "return an error if the file download fails" in {
    val s3 = createS3Client(Nil, Errors(download = true).some)

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)
    val ex = intercept[Exception] {
      fileProcessor.copyFilesFromDownloadToUploadBucket("key").unsafeRunSync()
    }
    ex.getMessage should equal("Error downloading files")
  }

  "copyFilesFromDownloadToUploadBucket" should "return an error if the upload fails" in {
    val s3 = createS3Client(S3Object("download", "key", testTarGz) :: Nil, Errors(upload = true).some)

    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)
    val ex = intercept[Exception] {
      fileProcessor.copyFilesFromDownloadToUploadBucket("key").unsafeRunSync()
    }
    ex.getMessage should equal("Upload failed")
  }

  "readJsonFromPackage" should "return the correct object for valid json" in {
    val metadataId = UUID.randomUUID()
    val s3 = createS3Client(S3Object("upload", metadataId.toString, ByteBuffer.wrap(metadataJson.getBytes())) :: Nil)
    val expectedMetadata = decode[TREMetadata](metadataJson).toOption.get
    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)

    val res = fileProcessor.readJsonFromPackage(metadataId).unsafeRunSync()

    res should equal(expectedMetadata)
  }

  tdrParams.foreach { case (paramNameToMakeNull, _) =>
    "readJsonFromPackage" should s"return an error if a null was passed in for the '$paramNameToMakeNull' field in the json" in {
      val tdrParamsWithANullValue = tdrParams.map { case (paramName, value) =>
        if paramName == paramNameToMakeNull then (paramName, None) else (paramName, Some(value))
      }

      val metadataJsonWithMissingParams: String =
        s"""{"parameters":{"TDR": ${tdrParamsWithANullValue.asJson.toString()},
         |"TRE":{"reference":"$reference","payload":{"filename":"Test.docx"}},
         |"PARSER":{"cite":"cite","uri":"https://example.com","court":"test","date":"2023-07-26","name":"test"}}}""".stripMargin

      val metadataId = UUID.randomUUID()

      val s3 = createS3Client(S3Object("upload", metadataId.toString, ByteBuffer.wrap(metadataJsonWithMissingParams.getBytes())) :: Nil)
      val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)

      val ex = intercept[DecodingFailure] {
        fileProcessor.readJsonFromPackage(metadataId).unsafeRunSync()
      }

      ex.getMessage.contains(s"""DownField($paramNameToMakeNull)""".stripMargin) should equal(true)
    }
  }

  tdrParams.foreach { case (paramNameToExclude, _) =>
    "readJsonFromPackage" should s"return an error for a json that is missing the '$paramNameToExclude' field" in {
      val tdrParamsWithMissingParam = tdrParams.filterNot { case (paramName, _) => paramName == paramNameToExclude }
      val metadataJsonWithMissingParams: String =
        s"""{"parameters":{"TDR": ${tdrParamsWithMissingParam.asJson.toString()},
         |"TRE":{"reference":"$reference","payload":{"filename":"Test.docx"}},
         |"PARSER":{"cite":"cite","uri":"https://example.com","court":"test","date":"2023-07-26","name":"test"}}}""".stripMargin
      val metadataId = UUID.randomUUID()
      val s3 = createS3Client(S3Object("upload", metadataId.toString, ByteBuffer.wrap(metadataJsonWithMissingParams.getBytes())) :: Nil)

      val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)

      val ex = intercept[DecodingFailure] {
        fileProcessor.readJsonFromPackage(metadataId).unsafeRunSync()
      }

      ex.getMessage.contains(s"DownField($paramNameToExclude)") should equal(true)
    }
  }

  "readJsonFromPackage" should "return an error for an invalid json" in {
    val invalidJson = "invalid"
    val metadataId = UUID.randomUUID()
    val s3 = createS3Client(S3Object("upload", metadataId.toString, ByteBuffer.wrap(invalidJson.getBytes())) :: Nil)
    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)

    val ex = intercept[ParsingFailure] {
      fileProcessor.readJsonFromPackage(metadataId).unsafeRunSync()
    }

    ex.getMessage should equal(
      """expected json value got 'invali...' (line 1, column 1)""".stripMargin
    )
  }

  "readJsonFromPackage" should "return an error if the download from s3 fails" in {
    val s3 = createS3Client(Nil, Errors(download = true).some)

    val metadataId = UUID.randomUUID()
    val fileProcessor = new FileProcessor("download", "upload", "ref", s3, UUIDGenerator().uuidGenerator)

    val ex = intercept[Exception] {
      fileProcessor.readJsonFromPackage(metadataId).unsafeRunSync()
    }
    ex.getMessage should equal("Error downloading files")
  }

  val department: Option[String] = Option("Department")
  val series: Option[String] = Option("Series")
  val trimmedUri: String = "http://example.com/id/abcde"
  val withoutCourt: Option[ParsedUri] = Option(ParsedUri(None, trimmedUri))
  val withCourt: Option[ParsedUri] = Option(ParsedUri(Option("TEST-COURT"), trimmedUri))
  val notMatched = "Court Documents (court not matched)"
  val unknown = "Court Documents (court unknown)"
  val tdrUuid: String = UUID.randomUUID().toString
  val potentialCorrelationId: Option[String] = Option(UUID.randomUUID().toString)

  private val treMetadata = TREMetadata(
    TREMetadataParameters(
      Parser(None, None, None, Nil, Nil),
      TREParams(reference, Payload("")),
      TDRParams(
        tdrParams("Document-Checksum-sha256"),
        tdrParams("Source-Organization"),
        tdrParams("Internal-Sender-Identifier"),
        OffsetDateTime.parse(tdrParams("Consignment-Export-Datetime")),
        Option("FileReference"),
        UUID.fromString(tdrUuid)
      )
    )
  )

  val citeTable: TableFor2[Option[String], List[IdField]] = Table(
    ("potentialCite", "idFields"),
    (None, Nil),
    (Option("cite"), List(IdField("Code", "cite"), IdField("Cite", "cite")))
  )

  val fileReferenceTable: TableFor1[Option[String]] = Table(
    "potentialFileReference",
    None,
    Option("fileReference")
  )

  val urlDepartmentAndSeriesTable: TableFor6[Option[String], Option[String], Boolean, Option[ParsedUri], String, Boolean] = Table(
    ("department", "series", "includeBagInfo", "url", "expectedFolderName", "titleExpected"),
    (department, series, true, withCourt, trimmedUri, true),
    (department, None, false, withCourt, notMatched, false),
    (None, series, false, withCourt, notMatched, false),
    (None, None, false, withCourt, notMatched, false),
    (department, series, true, withoutCourt, unknown, false),
    (department, None, false, withoutCourt, unknown, false),
    (None, series, false, withoutCourt, unknown, false),
    (None, None, false, withoutCourt, unknown, false),
    (department, series, true, None, unknown, false),
    (department, None, false, None, unknown, false),
    (None, series, false, None, unknown, false),
    (None, None, false, None, unknown, false)
  )

  val treNameTable: TableFor4[Option[String], String, String, String] = Table(
    ("treName", "treFileName", "expectedFolderTitle", "expectedAssetTitle"),
    (Option("Test title"), "fileName.txt", "Test title", "fileName.txt"),
    (None, "fileName.txt", null, "fileName.txt"),
    (Option("Press Summary of test"), "Press Summary of test.txt", "test", "Press Summary of test.txt")
  )
  forAll(fileReferenceTable) { potentialFileReference =>
    forAll(citeTable) { (potentialCite, idFields) =>
      forAll(treNameTable) { (treName, treFileName, expectedFolderTitle, expectedAssetTitle) =>
        forAll(urlDepartmentAndSeriesTable) { (department, potentialSeries, _, parsedUri, expectedFolderName, titleExpected) =>
          val updatedIdFields =
            if (department.isEmpty) Nil
            else if (potentialCite.isDefined && expectedFolderName == trimmedUri) idFields :+ IdField("URI", trimmedUri)
            else if (potentialCite.isEmpty && expectedFolderName == trimmedUri) List(IdField("URI", trimmedUri))
            else idFields

          "createMetadata" should s"generate the correct Metadata with $potentialCite, $potentialFileReference, " +
            s"$expectedFolderTitle, $expectedAssetTitle and $updatedIdFields for $department, $potentialSeries, $parsedUri and TRE name $treName" in {
              val fileId = UUID.randomUUID()
              val metadataId = UUID.randomUUID()
              val folderId = uuids.head
              val assetId = treMetadata.parameters.TDR.`UUID`
              val fileName = treFileName.split('.').dropRight(1).mkString(".")
              val folderTitle = if titleExpected then Option(expectedFolderTitle) else None
              val expectedSeries = potentialSeries.orElse(Option("Unknown"))
              val folder =
                ArchiveFolderMetadataObject(folderId, None, folderTitle, expectedFolderName, expectedSeries, updatedIdFields)
              val asset =
                AssetMetadataObject(
                  assetId,
                  Option(folderId),
                  expectedAssetTitle,
                  tdrUuid,
                  List(metadataId),
                  treName,
                  Option("test-organisation"),
                  Option(OffsetDateTime.parse("2023-10-31T13:40:54Z")),
                  `TRE: FCL Parser workflow`,
                  "Born Digital",
                  Option("FCL"),
                  treFileName,
                  potentialCorrelationId,
                  List(
                    Option(IdField("UpstreamSystemReference", reference)),
                    potentialUri.map(uri => IdField("URI", uri)),
                    potentialCite.map(cite => IdField("NeutralCitation", cite)),
                    potentialFileReference.map(fileReference => IdField("BornDigitalRef", fileReference)),
                    Option(IdField("ConsignmentReference", "test-identifier")),
                    Option(IdField("UpstreamSystemReference", "TEST-REFERENCE")),
                    Option(IdField("RecordID", tdrUuid))
                  ).flatten
                )
              val files = List(
                FileMetadataObject(
                  fileId,
                  Option(assetId),
                  fileName,
                  1,
                  treFileName,
                  1,
                  RepresentationType.Preservation,
                  1,
                  URI.create("s3://bucket/key"),
                  List(Checksum("sha256", "abcde"))
                ),
                FileMetadataObject(
                  metadataId,
                  Option(assetId),
                  "",
                  2,
                  "metadataFileName.txt",
                  2,
                  RepresentationType.Preservation,
                  1,
                  URI.create("s3://bucket/metadataKey"),
                  List(Checksum("sha256", "metadataChecksum"))
                )
              )
              val expectedMetadataObjects: List[MetadataObject] = List(folder, asset) ++ files

              val fileProcessor =
                new FileProcessor("download", "upload", "ref", createS3Client(Nil), UUIDGenerator().uuidGenerator)
              val fileInfo = FileInfo(fileId, 1, treFileName, "fileChecksum", URI.create("s3://bucket/key"))
              val metadataFileInfo = FileInfo(metadataId, 2, "metadataFileName.txt", "metadataChecksum", URI.create("s3://bucket/metadataKey"))

              val metadataObjects =
                fileProcessor
                  .createMetadata(
                    fileInfo,
                    metadataFileInfo,
                    parsedUri,
                    potentialCite,
                    treName,
                    potentialUri,
                    treMetadata,
                    potentialFileReference,
                    department,
                    potentialSeries,
                    tdrUuid,
                    potentialCorrelationId
                  )

              metadataObjects should equal(expectedMetadataObjects)
            }
        }
      }
    }
  }
}
