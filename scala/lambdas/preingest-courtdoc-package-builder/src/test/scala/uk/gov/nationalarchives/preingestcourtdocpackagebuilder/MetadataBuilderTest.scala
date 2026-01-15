package uk.gov.nationalarchives.preingestcourtdocpackagebuilder

import io.circe.{Encoder, Json}
import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1, TableFor2, TableFor4, TableFor6}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{Checksum, IngestLockTableItem}
import MetadataBuilder.*
import TestUtils.{S3Object, s3Client}
import UriProcessor.*
import cats.effect.{IO, Ref}
import uk.gov.nationalarchives.utils.ExternalUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.SourceSystem.`TRE: FCL Parser workflow`
import io.circe.syntax.*
import io.circe.generic.auto.*
import org.apache.commons.codec.digest.DigestUtils
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.preingestcourtdocpackagebuilder.SeriesMapper.DepartmentSeries

import java.net.URI
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.UUID

class MetadataBuilderTest extends AnyFlatSpec with TableDrivenPropertyChecks:

  given Encoder[LockTableMessage] = (m: LockTableMessage) =>
    Json.obj(
      ("id", Json.fromString(m.id.toString)),
      ("location", Json.fromString(m.location.toString)),
      ("fileId", Json.fromString(m.fileId.toString)),
      ("messageId", m.messageId.map(Json.fromString).getOrElse(Json.Null))
    )

  val department: Option[String] = Option("Department")
  val series: Option[String] = Option("Series")
  val trimmedUri: String = "http://example.com/id/abcde"
  val withoutCourt: Option[ParsedUri] = Option(ParsedUri(None, trimmedUri))
  val withCourt: Option[ParsedUri] = Option(ParsedUri(Option("TEST-COURT"), trimmedUri))
  val notMatched = "Court Documents (court not matched)"
  val unknown = "Court Documents (court unknown)"
  val tdrUuid: String = UUID.randomUUID().toString
  val potentialCorrelationId: Option[String] = Option(UUID.randomUUID().toString)
  val reference = "TEST-REFERENCE"
  val potentialUri: Some[String] = Some("http://example.com/id/abcde/2023/1537")

  private val uuids: List[UUID] = List(
    UUID.fromString("6e827e19-6a33-46c3-8730-b242c203d8c1"),
    UUID.fromString("49e4a726-6297-4f8e-8867-fb50bd5acd86"),
    UUID.fromString("593cf06e-0832-49e0-a20f-d259c8192d70")
  )

  val cites: TableFor2[Option[String], List[IdField]] = Table(
    ("potentialCite", "idFields"),
    (None, Nil),
    (Option("cite"), List(IdField("Code", "cite"), IdField("Cite", "cite")))
  )

  val fileReferences: TableFor1[Option[String]] = Table(
    "potentialFileReference",
    None,
    Option("fileReference")
  )

  val urlDepartmentSeries: TableFor6[Option[String], Option[String], Boolean, Option[ParsedUri], String, Boolean] = Table(
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
  forAll(fileReferences) { potentialFileReference =>
    forAll(cites) { (potentialCite, idFields) =>
      forAll(treNameTable) { (treName, treFileName, expectedFolderTitle, expectedAssetTitle) =>
        forAll(urlDepartmentSeries) { (department, potentialSeries, _, parsedUri, expectedFolderName, titleExpected) =>
          val updatedIdFields =
            if (department.isEmpty) Nil
            else if (potentialCite.isDefined && expectedFolderName == trimmedUri) idFields :+ IdField("URI", trimmedUri)
            else if (potentialCite.isEmpty && expectedFolderName == trimmedUri) List(IdField("URI", trimmedUri))
            else idFields

          "createMetadata" should s"generate the correct Metadata with $potentialCite, $potentialFileReference, " +
            s"$expectedFolderTitle, $expectedAssetTitle and $updatedIdFields for $department, $potentialSeries, $parsedUri and TRE name $treName" in {

              val seriesMapper: SeriesMapper = (potentialCourt: Option[String], skipSeriesLookup: Boolean) => IO(DepartmentSeries(department, potentialSeries))
              val uriProcessor: UriProcessor = new UriProcessor {
                override def verifyJudgmentName(treMetadata: TREMetadata): IO[Unit] = UriProcessor().verifyJudgmentName(treMetadata)

                override def parseUri(treMetadata: TREMetadata): IO[Option[ParsedUri]] = IO.pure(parsedUri)
              }

              val treMetadata = TREMetadata(
                TREMetadataParameters(
                  Parser(Some("http://example.com/id/abcde/2023/1537"), potentialCite, treName, Nil, Nil),
                  TREParams(reference, Payload(treFileName)),
                  TDRParams(
                    "abcde",
                    "test-organisation",
                    "test-identifier",
                    OffsetDateTime.parse("2023-10-31T13:40:54Z"),
                    potentialFileReference,
                    UUID.fromString(tdrUuid)
                  )
                )
              )

              val treMetadataString = treMetadata.asJson.noSpaces

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
                  4,
                  RepresentationType.Preservation,
                  1,
                  URI.create(s"s3://bucket/$fileId"),
                  List(Checksum("sha256", "abcde"))
                ),
                FileMetadataObject(
                  metadataId,
                  Option(assetId),
                  "",
                  2,
                  "TRE-test-identifier-metadata.json",
                  treMetadataString.length,
                  RepresentationType.Preservation,
                  1,
                  URI.create(s"s3://bucket/$metadataId"),
                  List(Checksum("sha256", DigestUtils.sha256Hex(treMetadataString)))
                )
              )
              val expectedMetadataObjects: List[MetadataObject] = List(folder, asset) ++ files

              val uuidsIterator = uuids.iterator

              val s3Objects = List(
                S3Object("bucket", metadataId.toString, ByteBuffer.wrap(treMetadata.asJson.noSpaces.getBytes)),
                S3Object("bucket", fileId.toString, ByteBuffer.wrap("test".getBytes))
              )

              val metadataBuilder =
                MetadataBuilder(() => uuidsIterator.next, s3Client(s3Objects, Ref.unsafe(Nil)), seriesMapper, uriProcessor)

              val message = LockTableMessage(assetId, URI.create(s"s3://bucket/$metadataId"), fileId, potentialCorrelationId)
              val item = IngestLockTableItem(assetId, "group_ID", message.asJson.noSpaces, "1")

              val metadataObjects = metadataBuilder.createMetadata(item).unsafeRunSync()

              metadataObjects should equal(expectedMetadataObjects)
            }
        }
      }
    }
  }
