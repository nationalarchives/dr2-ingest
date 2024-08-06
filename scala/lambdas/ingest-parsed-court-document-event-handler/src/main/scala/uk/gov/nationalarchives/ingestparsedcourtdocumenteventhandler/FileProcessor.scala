package uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.implicits.*
import fs2.compression.Compression
import fs2.io.*
import fs2.{Chunk, Pipe, Stream, text}
import io.circe.Decoder.Result
import io.circe.Json.Null
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.{Decoder, Encoder, HCursor, Json, Printer}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.reactivestreams.{FlowAdapters, Publisher}
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.FileProcessor.*
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.UriProcessor.ParsedUri

import java.io.{BufferedInputStream, InputStream}
import java.net.URI
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.{Base64, UUID}

class FileProcessor(
    downloadBucket: String,
    uploadBucket: String,
    consignmentRef: String,
    s3: DAS3Client[IO],
    uuidGenerator: () => UUID
) {

  def copyFilesFromDownloadToUploadBucket(downloadBucketKey: String): IO[Map[String, FileInfo]] = {
    s3.download(downloadBucket, downloadBucketKey)
      .flatMap(
        _.publisherToStream
          .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
          .through(Compression[IO].gunzip())
          .flatMap(_.content)
          .through(unarchiveToS3)
          .compile
          .toList
      )
      .map(_.toMap)
  }

  def readJsonFromPackage(metadataId: UUID): IO[TREMetadata] = {
    for {
      s3Publisher <- s3.download(uploadBucket, metadataId.toString)
      contentString <- s3Publisher.publisherToStream
        .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
        .through(extractMetadataFromJson)
        .compile
        .toList
      parsedJson <- IO.fromOption(contentString.headOption)(
        new RuntimeException(
          "Error parsing metadata.json.\nPlease check that the JSON is valid and that all required fields are present"
        )
      )
    } yield parsedJson
  }

  def createMetadata(
      fileInfo: FileInfo,
      metadataFileInfo: FileInfo,
      parsedUri: Option[ParsedUri],
      potentialCite: Option[String],
      potentialJudgmentName: Option[String],
      potentialUri: Option[String],
      treMetadata: TREMetadata,
      fileReference: Option[String],
      potentialDepartment: Option[String],
      potentialSeries: Option[String],
      tdrUuid: String
  ): List[MetadataObject] = {
    val potentialCourtFromUri = parsedUri.flatMap(_.potentialCourt)
    val (folderName, potentialFolderTitle, uriIdField) =
      if (potentialDepartment.flatMap(_ => potentialSeries).isEmpty && potentialCourtFromUri.isDefined)
        ("Court Documents (court not matched)", None, Nil)
      else if (potentialCourtFromUri.isEmpty) ("Court Documents (court unknown)", None, Nil)
      else
        (
          parsedUri.get.uriWithoutDocType,
          potentialJudgmentName.map(_.stripPrefix("Press Summary of ")),
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
      potentialUri.map(uri => IdField("URI", uri)),
      potentialCite.map(cite => IdField("NeutralCitation", cite)),
      fileReference.map(ref => IdField("BornDigitalRef", ref)),
      Option(IdField("ConsignmentReference", treMetadata.parameters.TDR.`Internal-Sender-Identifier`)),
      Option(IdField("UpstreamSystemReference", treMetadata.parameters.TRE.reference)),
      Option(IdField("RecordID", tdrUuid))
    ).flatten
    val fileTitle = fileInfo.fileName.split('.').dropRight(1).mkString(".")
    val folderId = uuidGenerator()
    val assetId = UUID.fromString(tdrUuid)
    val folderMetadataObject = FolderMetadataObject(folderId, None, potentialFolderTitle, folderName, potentialSeries.getOrElse("Unknown"), folderMetadataIdFields)
    val assetMetadataObject =
      AssetMetadataObject(
        assetId,
        Option(folderId),
        fileInfo.fileName,
        tdrUuid,
        List(fileInfo.id),
        List(metadataFileInfo.id),
        potentialJudgmentName,
        treMetadata.parameters.TDR.`Source-Organization`,
        treMetadata.parameters.TDR.`Consignment-Export-Datetime`,
        "TRE: FCL Parser workflow",
        "Born Digital",
        "FCL",
        assetMetadataIdFields
      )
    val fileRowMetadataObject =
      FileMetadataObject(
        fileInfo.id,
        Option(assetId),
        fileTitle,
        1,
        fileInfo.fileName,
        fileInfo.fileSize,
        RepresentationType.Preservation,
        1,
        fileInfo.location,
        fileInfo.checksum
      )
    val fileMetadataObject = FileMetadataObject(
      metadataFileInfo.id,
      Option(assetId),
      "",
      2,
      metadataFileInfo.fileName,
      metadataFileInfo.fileSize,
      RepresentationType.Preservation,
      1,
      metadataFileInfo.location,
      metadataFileInfo.checksum
    )
    List(folderMetadataObject, assetMetadataObject, fileRowMetadataObject, fileMetadataObject)
  }

  private def extractMetadataFromJson(str: Stream[IO, Byte]): Stream[IO, TREMetadata] = {
    str
      .through(text.utf8.decode)
      .flatMap { jsonString =>
        Stream.fromEither[IO](decode[TREMetadata](jsonString))
      }
  }

  private def unarchiveAndUploadToS3(tarInputStream: TarArchiveInputStream): Stream[IO, (String, FileInfo)] = {
    Stream
      .eval(IO.blocking(Option(tarInputStream.getNextEntry)))
      .flatMap(Stream.fromOption[IO](_))
      .flatMap { tarEntry =>
        Stream
          .eval(IO(readInputStream(IO.pure[InputStream](tarInputStream), chunkSize, closeAfterUse = false)))
          .flatMap { stream =>
            if (!tarEntry.isDirectory) {
              val id = uuidGenerator()
              Stream.eval[IO, (String, FileInfo)](
                stream.chunks
                  .map(_.toByteBuffer)
                  .toPublisherResource
                  .use(pub => s3.upload(uploadBucket, id.toString, tarEntry.getSize, FlowAdapters.toPublisher(pub)))
                  .map { res =>
                    val checksum = checksumToString(res.response().checksumSHA256())
                    tarEntry.getName -> FileInfo(id, tarEntry.getSize, tarEntry.getName.split('/').last, checksum, URI.create(s"s3://$uploadBucket/${id.toString}"))
                  }
              )
            } else Stream.empty
          } ++
          unarchiveAndUploadToS3(tarInputStream)
      }
  }

  private def unarchiveToS3: Pipe[IO, Byte, (String, FileInfo)] = { stream =>
    stream
      .through(toInputStream[IO])
      .map(new BufferedInputStream(_, chunkSize))
      .flatMap(is => Stream.resource(Resource.fromAutoCloseable(IO.blocking(new TarArchiveInputStream(is)))))
      .flatMap(unarchiveAndUploadToS3)
  }

  private def uploadAsFile(fileContent: String, s3Location: URI): IO[Unit] = {
    Stream
      .eval(IO.pure(fileContent))
      .map(s => ByteBuffer.wrap(s.getBytes()))
      .toPublisherResource
      .use { pub =>
        s3.upload(s3Location.getHost, s3Location.getPath.drop(1), fileContent.getBytes.length, FlowAdapters.toPublisher(pub))
      }
      .map(_ => ())
  }

  def createMetadataJson(metadata: List[MetadataObject], s3Location: URI): IO[Unit] =
    uploadAsFile(metadata.asJson.printWith(Printer.noSpaces), s3Location)

  private def checksumToString(checksum: String): String =
    Option(checksum)
      .map(c => Hex.encodeHex(Base64.getDecoder.decode(c.getBytes())).mkString)
      .getOrElse("")

}

object FileProcessor {
  private val chunkSize: Int = 1024 * 64
  private val convertIdFieldsToJson = (idFields: List[IdField]) =>
    idFields.map { idField =>
      (s"id_${idField.name}", Json.fromString(idField.value))
    }

  given Decoder[TREInputParameters] = (c: HCursor) =>
    for {
      status <- c.downField("status").as[String]
      reference <- c.downField("reference").as[String]
      s3Bucket <- c.downField("s3Bucket").as[String]
      s3Key <- c.downField("s3Key").as[String]
      skipSeriesLookup <- c.getOrElse("skipSeriesLookup")(false)
    } yield TREInputParameters(status, reference, skipSeriesLookup, s3Bucket, s3Key)
  given Encoder[MetadataObject] = {
    case FolderMetadataObject(id, parentId, title, name, series, folderMetadataIdFields) =>
      jsonFromMetadataObject(id, parentId, title, Type.ArchiveFolder, name)
        .deepMerge {
          Json.fromFields(convertIdFieldsToJson(folderMetadataIdFields))
        }
        .deepMerge {
          Json
            .obj(
              ("series", Json.fromString(series))
            )
        }
    case AssetMetadataObject(
          id,
          parentId,
          title,
          name,
          originalFilesUuids,
          originalMetadataFilesUuids,
          description,
          transferringBody,
          transferCompleteDatetime,
          upstreamSystem,
          digitalAssetSource,
          digitalAssetSubtype,
          assetMetadataIdFields
        ) =>
      val convertListOfUuidsToJsonStrArray = (fileUuids: List[UUID]) => fileUuids.map(fileUuid => Json.fromString(fileUuid.toString))

      jsonFromMetadataObject(id, parentId, Option(title), Type.Asset, name)
        .deepMerge {
          Json.fromFields(convertIdFieldsToJson(assetMetadataIdFields))
        }
        .deepMerge {
          Json
            .obj(
              ("originalFiles", Json.fromValues(convertListOfUuidsToJsonStrArray(originalFilesUuids))),
              ("originalMetadataFiles", Json.fromValues(convertListOfUuidsToJsonStrArray(originalMetadataFilesUuids))),
              ("description", description.map(Json.fromString).getOrElse(Null)),
              ("transferringBody", Json.fromString(transferringBody)),
              ("transferCompleteDatetime", Json.fromString(transferCompleteDatetime.toString)),
              ("upstreamSystem", Json.fromString(upstreamSystem)),
              ("digitalAssetSource", Json.fromString(digitalAssetSource)),
              ("digitalAssetSubtype", Json.fromString(digitalAssetSubtype))
            )
            .deepDropNullValues
        }

    case FileMetadataObject(id, parentId, title, sortOrder, name, fileSize, representationType, representationSuffix, location, checksumSha256) =>
      Json
        .obj(
          ("sortOrder", Json.fromInt(sortOrder)),
          ("fileSize", Json.fromLong(fileSize)),
          ("representationType", Json.fromString(representationType.toString)),
          ("representationSuffix", Json.fromInt(representationSuffix)),
          ("location", Json.fromString(location.toString)),
          ("checksum_sha256", Json.fromString(checksumSha256))
        )
        .deepMerge(jsonFromMetadataObject(id, parentId, Option(title), Type.File, name))
  }

  private def jsonFromMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: Option[String],
      objectType: Type,
      name: String
  ) = {
    Json.obj(
      ("id", Json.fromString(id.toString)),
      ("parentId", parentId.map(_.toString).map(Json.fromString).getOrElse(Null)),
      ("title", title.map(Json.fromString).getOrElse(Null)),
      ("type", objectType.asJson),
      ("name", Json.fromString(name))
    )
  }

  given Encoder[Type] = {
    case Type.ArchiveFolder => Json.fromString("ArchiveFolder")
    case Type.Asset         => Json.fromString("Asset")
    case Type.File          => Json.fromString("File")
  }

  enum RepresentationType:
    case Preservation

  enum Type:
    case ArchiveFolder, Asset, File

  case class AdditionalMetadata(key: String, value: String)
  sealed trait MetadataObject {
    def id: UUID
    def parentId: Option[UUID]
  }

  case class IdField(name: String, value: String)

  case class FolderMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: Option[String],
      name: String,
      series: String,
      idFields: List[IdField] = Nil
  ) extends MetadataObject

  case class AssetMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: String,
      name: String,
      originalFiles: List[UUID],
      originalMetadataFiles: List[UUID],
      description: Option[String],
      transferringBody: String,
      transferCompleteDatetime: OffsetDateTime,
      upstreamSystem: String,
      digitalAssetSource: String,
      digitalAssetSubtype: String,
      idFields: List[IdField] = Nil
  ) extends MetadataObject

  case class FileMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: String,
      sortOrder: Int,
      name: String,
      fileSize: Long,
      representationType: RepresentationType,
      representationSuffix: Int,
      location: URI,
      checksumSha256: String
  ) extends MetadataObject

  case class FileInfo(id: UUID, fileSize: Long, fileName: String, checksum: String, location: URI)

  case class TREInputParameters(status: String, reference: String, skipSeriesLookup: Boolean, s3Bucket: String, s3Key: String)

  case class TREInput(parameters: TREInputParameters)

  case class TREMetadata(parameters: TREMetadataParameters)

  extension (c: HCursor)
    private def listOrNil(fieldName: String): Result[List[String]] =
      if c.keys.getOrElse(Nil).toList.contains(fieldName) then c.downField(fieldName).as[List[String]] else Right(Nil)

  given parserDecoder: Decoder[Parser] = (c: HCursor) =>
    for {
      uri <- c.downField("uri").as[Option[String]]
      cite <- c.downField("cite").as[Option[String]]
      name <- c.downField("name").as[Option[String]]
      attachments <- c.listOrNil("attachments")
      errorMessages <- c.listOrNil("error-messages")
    } yield Parser(uri, cite, name, attachments, errorMessages)

  case class Parser(
      uri: Option[String],
      cite: Option[String] = None,
      name: Option[String],
      attachments: List[String] = Nil,
      `error-messages`: List[String] = Nil
  )

  case class Payload(filename: String)

  case class TREParams(reference: String, payload: Payload)

  case class TDRParams(
      `Document-Checksum-sha256`: String,
      `Source-Organization`: String,
      `Internal-Sender-Identifier`: String,
      `Consignment-Export-Datetime`: OffsetDateTime,
      `File-Reference`: Option[String],
      `UUID`: UUID
  )

  case class TREMetadataParameters(PARSER: Parser, TRE: TREParams, TDR: TDRParams)

  extension (publisher: Publisher[ByteBuffer])
    def publisherToStream: Stream[IO, ByteBuffer] = Stream.eval(IO.delay(publisher)).flatMap { publisher =>
      fs2.interop.flow.fromPublisher[IO](FlowAdapters.toFlowPublisher(publisher), chunkSize = 16)
    }

  case class Config(outputBucket: String, sfnArn: String, dynamoLockTableName: String) derives ConfigReader
}
