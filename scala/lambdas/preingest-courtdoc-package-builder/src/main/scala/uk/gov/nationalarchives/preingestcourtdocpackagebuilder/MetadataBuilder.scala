package uk.gov.nationalarchives.preingestcourtdocpackagebuilder

import cats.effect.IO
import fs2.interop.reactivestreams.*
import fs2.{Chunk, Stream, text}
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.{Decoder, HCursor}
import org.apache.commons.codec.digest.DigestUtils
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{Checksum, IngestLockTableItem}
import uk.gov.nationalarchives.preingestcourtdocpackagebuilder.SeriesMapper.DepartmentSeries
import uk.gov.nationalarchives.preingestcourtdocpackagebuilder.UriProcessor.ParsedUri
import uk.gov.nationalarchives.utils.ExternalUtils.SourceSystem.`TRE: FCL Parser workflow`
import uk.gov.nationalarchives.utils.ExternalUtils.{*, given}

import java.net.URI
import java.util.UUID

trait MetadataBuilder:
  def createMetadata(item: IngestLockTableItem): IO[List[MetadataObject]]
object MetadataBuilder:

  case class LockTableMessage(id: UUID, location: URI, fileId: UUID, messageId: Option[String], skipSeries: Boolean = false)

  private case class MetadataInfo(id: UUID, fileSize: Long, location: URI, checksum: String, treMetadata: TREMetadata)

  given Decoder[LockTableMessage] = (c: HCursor) =>
    for
      id <- c.downField("id").as[UUID]
      location <- c.downField("location").as[URI]
      fileId <- c.downField("fileId").as[UUID]
      messageId <- c.downField("messageId").as[Option[String]]
      skipSeries <- c.downField("skipSeries").as[Option[Boolean]]
    yield LockTableMessage(id, location, fileId, messageId, skipSeries.exists(identity))

  def apply(uuidGenerator: () => UUID, s3Client: DAS3Client[IO], seriesMapper: SeriesMapper, uriProcessor: UriProcessor): MetadataBuilder = new MetadataBuilder {

    def metadataFromS3[T](s3Uri: URI): IO[MetadataInfo] = {
      val key = s3Uri.getPath.drop(1)
      for
        pub <- s3Client.download(s3Uri.getHost, key)
        s3FileStrings <- pub
          .toStreamBuffered[IO](1024)
          .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
          .through(text.utf8.decode)
          .compile
          .toList
        metadataString = s3FileStrings.mkString
        metadata <- IO.fromEither(decode[TREMetadata](metadataString))
      yield MetadataInfo(UUID.fromString(key), metadataString.getBytes.length, s3Uri, DigestUtils.sha256Hex(metadataString), metadata)
    }

    def generateMetadata(
        parsedUri: Option[ParsedUri],
        metadataInfo: MetadataInfo,
        departmentSeries: DepartmentSeries,
        fileSize: Long,
        fileId: UUID,
        potentialCorrelationId: Option[String]
    ): List[MetadataObject] = {
      val treMetadata = metadataInfo.treMetadata
      val metadataFileId = metadataInfo.id
      val potentialName = treMetadata.parameters.PARSER.name
      val potentialCourtFromUri = parsedUri.flatMap(_.potentialCourt)
      val potentialDepartment = departmentSeries.potentialDepartment
      val potentialSeries = departmentSeries.potentialSeries
      val potentialCite = treMetadata.parameters.PARSER.cite
      val fileName = treMetadata.parameters.TRE.payload.filename
      val tdrUuid = treMetadata.parameters.TDR.`UUID`.toString

      val (folderName, potentialFolderTitle, uriIdField) =
        if (potentialDepartment.flatMap(_ => potentialSeries).isEmpty && potentialCourtFromUri.isDefined)
          ("Court Documents (court not matched)", None, Nil)
        else if (potentialCourtFromUri.isEmpty) ("Court Documents (court unknown)", None, Nil)
        else
          (
            parsedUri.get.uriWithoutDocType,
            potentialName.map(_.stripPrefix("Press Summary of ")),
            List(IdField("URI", parsedUri.get.uriWithoutDocType))
          )

      val folderMetadataIdFields = potentialDepartment
        .map { _ =>
          val potentialIdFields = potentialCite
            .map(cite => List(IdField("Code", cite), IdField("Cite", cite)) ++ uriIdField)
          potentialIdFields.getOrElse(uriIdField)
        }
        .getOrElse(Nil)

      val assetMetadataIdFields = List(
        Option(IdField("UpstreamSystemReference", treMetadata.parameters.TRE.reference)),
        treMetadata.parameters.PARSER.uri.map(uri => IdField("URI", uri)),
        potentialCite.map(cite => IdField("NeutralCitation", cite)),
        treMetadata.parameters.TDR.`File-Reference`.map(ref => IdField("BornDigitalRef", ref)),
        Option(IdField("ConsignmentReference", treMetadata.parameters.TDR.`Internal-Sender-Identifier`)),
        Option(IdField("UpstreamSystemReference", treMetadata.parameters.TRE.reference)),
        Option(IdField("RecordID", tdrUuid))
      ).flatten
      val fileTitle = fileName.split('.').dropRight(1).mkString(".")
      val folderId = uuidGenerator()
      val assetId = UUID.fromString(tdrUuid)
      val series = potentialSeries.orElse(Option("Unknown"))
      val archiveFolderMetadataObject = ArchiveFolderMetadataObject(folderId, None, potentialFolderTitle, folderName, series, folderMetadataIdFields)
      val assetMetadataObject =
        AssetMetadataObject(
          assetId,
          Option(folderId),
          fileName,
          tdrUuid,
          List(metadataFileId),
          potentialName,
          Option(treMetadata.parameters.TDR.`Source-Organization`),
          Option(treMetadata.parameters.TDR.`Consignment-Export-Datetime`),
          `TRE: FCL Parser workflow`,
          "Born Digital",
          Option("FCL"),
          fileName,
          potentialCorrelationId,
          assetMetadataIdFields
        )
      val fileRowMetadataObject =
        FileMetadataObject(
          fileId,
          Option(assetId),
          fileTitle,
          1,
          fileName,
          fileSize,
          RepresentationType.Preservation,
          1,
          URI.create(s"s3://${metadataInfo.location.getHost}/$fileId"),
          List(Checksum("sha256", treMetadata.parameters.TDR.`Document-Checksum-sha256`))
        )
      val fileMetadataObject = FileMetadataObject(
        metadataFileId,
        Option(assetId),
        "",
        2,
        s"TRE-${treMetadata.parameters.TDR.`Internal-Sender-Identifier`}-metadata.json",
        metadataInfo.fileSize,
        RepresentationType.Preservation,
        1,
        metadataInfo.location,
        List(Checksum("sha256", metadataInfo.checksum))
      )
      List(archiveFolderMetadataObject, assetMetadataObject, fileRowMetadataObject, fileMetadataObject)
    }

    override def createMetadata(item: IngestLockTableItem): IO[List[MetadataObject]] =
      for
        message <- IO.fromEither(decode[LockTableMessage](item.message))
        metadataInfo <- metadataFromS3(message.location)
        _ <- uriProcessor.verifyJudgmentName(metadataInfo.treMetadata)
        parsedUri <- uriProcessor.parseUri(metadataInfo.treMetadata)
        departmentSeries <- seriesMapper.createDepartmentAndSeries(parsedUri.flatMap(_.potentialCourt), message.skipSeries)
        headResponse <- s3Client.headObject(message.location.getHost, message.fileId.toString)
      yield generateMetadata(
        parsedUri,
        metadataInfo,
        departmentSeries,
        headResponse.contentLength(),
        message.fileId,
        message.messageId
      )
  }
