package uk.gov.nationalarchives.utils

import cats.data.Validated
import io.circe.Decoder.{AccumulatingResult, Result}
import io.circe.Json.Null
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, DecodingFailure, Encoder, HCursor, Json}
import io.circe.syntax.*
import uk.gov.nationalarchives.utils.ExternalUtils.Type.*
import cats.implicits.*
import pureconfig.ConfigReader
import pureconfig.error.KeyNotFound
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{Checksum, checksumPrefix}

import java.net.URI
import java.time.{Instant, OffsetDateTime}
import java.util.UUID
import scala.util.Try

object ExternalUtils {
  enum DetailType:
    case DR2Message, DR2DevMessage

  enum RepresentationType:
    case Preservation, Access

  sealed trait MetadataObject {
    def id: UUID

    def parentId: Option[UUID]

    def getType: Type = this match
      case _: ArchiveFolderMetadataObject => ArchiveFolder
      case _: ContentFolderMetadataObject => ContentFolder
      case _: AssetMetadataObject         => Asset
      case _: FileMetadataObject          => File
  }

  given Encoder[Type] = (t: Type) => Json.fromString(t.toString)

  enum Type:
    case ArchiveFolder, ContentFolder, Asset, File

    def validParent(parentType: Option[Type]): Boolean = this match
      case ArchiveFolder => List(ArchiveFolder).containsSlice(parentType.toList)
      case ContentFolder => List(ContentFolder, ArchiveFolder).containsSlice(parentType.toList)
      case Asset         => List(ContentFolder, ArchiveFolder, Asset).containsSlice(parentType.toList)
      case File          => List(Asset).containsSlice(parentType.toList)

  private val convertIdFieldsToJson = (idFields: List[IdField]) =>
    idFields.map { idField =>
      (s"id_${idField.name}", Json.fromString(idField.value))
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

  private def createFolderMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: Option[String],
      name: String,
      series: Option[String],
      folderMetadataIdFields: List[IdField],
      folderType: Type
  ) = {
    jsonFromMetadataObject(id, parentId, title, folderType, name)
      .deepMerge {
        Json.fromFields(convertIdFieldsToJson(folderMetadataIdFields))
      }
      .deepMerge {
        Json
          .obj(
            ("series", series.map(Json.fromString).getOrElse(Json.Null))
          )
      }
  }

  given Encoder[MetadataObject] = {
    case ArchiveFolderMetadataObject(id, parentId, title, name, series, folderMetadataIdFields) =>
      createFolderMetadataObject(id, parentId, title, name, series, folderMetadataIdFields, ArchiveFolder)
    case ContentFolderMetadataObject(id, parentId, title, name, series, folderMetadataIdFields) =>
      createFolderMetadataObject(id, parentId, title, name, series, folderMetadataIdFields, ContentFolder)
    case AssetMetadataObject(
          id,
          parentId,
          title,
          name,
          originalMetadataFilesUuids,
          description,
          transferringBody,
          transferCompleteDatetime,
          upstreamSystem,
          digitalAssetSource,
          digitalAssetSubtype,
          filePath,
          correlationId,
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
              ("originalMetadataFiles", Json.fromValues(convertListOfUuidsToJsonStrArray(originalMetadataFilesUuids))),
              ("description", description.map(Json.fromString).getOrElse(Null)),
              ("transferringBody", transferringBody.map(Json.fromString).getOrElse(Null)),
              ("transferCompleteDatetime", Json.fromString(transferCompleteDatetime.toString)),
              ("upstreamSystem", Json.fromString(upstreamSystem.toString)),
              ("digitalAssetSource", Json.fromString(digitalAssetSource)),
              ("digitalAssetSubtype", digitalAssetSubtype.map(Json.fromString).getOrElse(Null)),
              ("correlationId", correlationId.map(Json.fromString).getOrElse(Null)),
              ("filePath", Json.fromString(filePath))
            )
            .deepDropNullValues
        }
    case FileMetadataObject(id, parentId, title, sortOrder, name, fileSize, representationType, representationSuffix, location, checksums) =>
      val fileObjects: List[(String, Json)] = List(
        ("sortOrder", Json.fromInt(sortOrder)),
        ("fileSize", Json.fromLong(fileSize)),
        ("representationType", Json.fromString(representationType.toString)),
        ("representationSuffix", Json.fromInt(representationSuffix)),
        ("location", Json.fromString(location.toString))
      ) ++ checksums.map(checksum => (s"checksum_${checksum.algorithm}", Json.fromString(checksum.fingerprint)))
      Json
        .obj(fileObjects*)
        .deepMerge(jsonFromMetadataObject(id, parentId, Option(title), Type.File, name))
  }

  case class IdField(name: String, value: String)

  private def decodeFolder(c: HCursor) = (
    c.downField("id").as[UUID].toValidatedNel,
    c.downField("parentId").as[Option[UUID]].toValidatedNel,
    c.downField("title").as[Option[String]].toValidatedNel,
    c.downField("name").as[String].toValidatedNel,
    c.downField("series").as[Option[String]].toValidatedNel,
    getIdFields(c).toValidatedNel
  )

  given Decoder[RepresentationType] = (c: HCursor) => c.as[String].map(RepresentationType.valueOf)

  given Decoder[IdField] = deriveDecoder[IdField]

  given Decoder[ArchiveFolderMetadataObject] = new Decoder[ArchiveFolderMetadataObject]:
    override def apply(c: HCursor): Result[ArchiveFolderMetadataObject] = convertToFailFast(decodeAccumulating(c))

    override def decodeAccumulating(c: HCursor): AccumulatingResult[ArchiveFolderMetadataObject] = decodeFolder(c).mapN(ArchiveFolderMetadataObject.apply)

  given Decoder[ContentFolderMetadataObject] = new Decoder[ContentFolderMetadataObject]:
    override def apply(c: HCursor): Result[ContentFolderMetadataObject] = convertToFailFast(decodeAccumulating(c))

    override def decodeAccumulating(c: HCursor): AccumulatingResult[ContentFolderMetadataObject] = decodeFolder(c).mapN(ContentFolderMetadataObject.apply)

  private def convertToFailFast[T](result: AccumulatingResult[T]): Either[DecodingFailure, T] = result match
    case Validated.Valid(value)    => Right(value)
    case Validated.Invalid(errors) => Left(errors.head)

  given Decoder[AssetMetadataObject] = new Decoder[AssetMetadataObject]:
    override def apply(c: HCursor): Result[AssetMetadataObject] = convertToFailFast(decodeAccumulating(c))

    def toSourceSystem(c: HCursor, sourceSystem: String): Either[DecodingFailure, SourceSystem] =
      Try(SourceSystem.valueOf(sourceSystem)).toEither.left.map(err => DecodingFailure(err.getMessage, c.history))

    override def decodeAccumulating(c: HCursor): AccumulatingResult[AssetMetadataObject] = {
      (
        c.downField("id").as[UUID].toValidatedNel,
        c.downField("parentId").as[Option[UUID]].toValidatedNel,
        c.downField("title").as[String].toValidatedNel,
        c.downField("name").as[String].toValidatedNel,
        c.downField("originalMetadataFiles").as[List[UUID]].toValidatedNel,
        c.downField("description").as[Option[String]].toValidatedNel,
        c.downField("transferringBody").as[Option[String]].toValidatedNel,
        c.downField("transferCompleteDatetime").as[String].toValidatedNel.map(OffsetDateTime.parse),
        c.downField("upstreamSystem").as[String].flatMap(str => toSourceSystem(c, str)).toValidatedNel,
        c.downField("digitalAssetSource").as[String].toValidatedNel,
        c.downField("digitalAssetSubtype").as[Option[String]].toValidatedNel,
        c.downField("filePath").as[String].toValidatedNel,
        c.downField("correlationId").as[Option[String]].toValidatedNel,
        getIdFields(c).toValidatedNel
      ).mapN(AssetMetadataObject.apply)
    }

  private def getIdFields(c: HCursor) = Right {
    c.keys
      .map(_.toList)
      .getOrElse(Nil)
      .filter(_.startsWith("id_"))
      .flatMap { key =>
        c.downField(key).as[String].toOption.map { value =>
          IdField(key.drop(3), value)
        }
      }
  }

  given Decoder[URI] = Decoder.decodeString.emap { str =>
    Try(URI.create(str)).toEither.left.map(_.getMessage)
  }

  given Encoder[URI] = Encoder.encodeString.contramap(_.toString)

  def getChecksums(c: HCursor): Result[List[Checksum]] =
    val keys = c.keys.toList.flatten
    val checksumKeys = keys.filter(_.startsWith(checksumPrefix))
    if keys.contains("SHA256ServerSideChecksum") then
      c.downField("SHA256ServerSideChecksum").as[String].map { sha256Checksum =>
        List(Checksum("sha256", sha256Checksum))
      }
    else
      checksumKeys.traverse { key =>
        val algorithm = key.replace(checksumPrefix, "")
        c.downField(key).as[String].map(fingerprint => Checksum(algorithm, fingerprint))
      }

  given Decoder[FileMetadataObject] = new Decoder[FileMetadataObject]:

    override def apply(c: HCursor): Result[FileMetadataObject] = convertToFailFast(decodeAccumulating(c))

    override def decodeAccumulating(c: HCursor): AccumulatingResult[FileMetadataObject] = (
      c.downField("id").as[UUID].toValidatedNel,
      c.downField("parentId").as[Option[UUID]].toValidatedNel,
      c.downField("title").as[String].toValidatedNel,
      c.downField("sortOrder").as[Int].toValidatedNel,
      c.downField("name").as[String].toValidatedNel,
      c.downField("fileSize").as[Long].toValidatedNel,
      c.downField("representationType").as[RepresentationType].toValidatedNel,
      c.downField("representationSuffix").as[Int].toValidatedNel,
      c.downField("location").as[URI].toValidatedNel,
      getChecksums(c).toValidatedNel
    ).mapN(FileMetadataObject.apply)

  given Decoder[MetadataObject] =
    for {
      objectType <- Decoder[String].prepare(_.downField("type"))
      mandatoryFields <- objectType match
        case "ArchiveFolder" => Decoder[ArchiveFolderMetadataObject]
        case "ContentFolder" => Decoder[ContentFolderMetadataObject]
        case "Asset"         => Decoder[AssetMetadataObject]
        case "File"          => Decoder[FileMetadataObject]
    } yield mandatoryFields

  case class ArchiveFolderMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: Option[String],
      name: String,
      series: Option[String],
      idFields: List[IdField] = Nil
  ) extends MetadataObject

  case class ContentFolderMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: Option[String],
      name: String,
      series: Option[String],
      idFields: List[IdField] = Nil
  ) extends MetadataObject

  case class AssetMetadataObject(
      id: UUID,
      parentId: Option[UUID],
      title: String,
      name: String,
      originalMetadataFiles: List[UUID],
      description: Option[String],
      transferringBody: Option[String],
      transferCompleteDatetime: OffsetDateTime,
      upstreamSystem: SourceSystem,
      digitalAssetSource: String,
      digitalAssetSubtype: Option[String],
      filePath: String,
      correlationId: Option[String],
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
      checksums: List[Checksum]
  ) extends MetadataObject

  given ConfigReader[SourceSystem] = ConfigReader.fromString[SourceSystem] { str =>
    Try(SourceSystem.valueOf(str)).toEither.left.map { e =>
      KeyNotFound(str, SourceSystem.values.map(_.toString).toSet)
    }
  }

  enum SourceSystem:
    case TDR, DRI, `TRE: FCL Parser workflow`, `Parliament Migration`
    
  enum MessageType:
    override def toString: String = this match
      case IngestUpdate   => "preserve.digital.asset.ingest.update"
      case IngestComplete => "preserve.digital.asset.ingest.complete"

    case IngestUpdate, IngestComplete

  enum MessageStatus(val value: String):
    case IngestedPreservation extends MessageStatus("Asset has been ingested to the Preservation System.")
    case IngestedCCDisk extends MessageStatus("Asset has been written to custodial copy disk.")
    case IngestStarted extends MessageStatus("Asset has started the ingest process.")
    case IngestError extends MessageStatus("There has been an error ingesting the asset.")

  case class NotificationMessage(id: UUID, location: URI, messageId: Option[String] = None)

  case class OutputProperties(executionId: String, messageId: UUID, parentMessageId: Option[String], timestamp: Instant, messageType: MessageType)

  case class OutputParameters(assetId: UUID, status: MessageStatus)

  case class OutputMessage(properties: OutputProperties, parameters: OutputParameters)

  case class StepFunctionInput(batchId: String, groupId: String, metadataPackage: URI, retryCount: Int, retrySfnArn: String)
}
