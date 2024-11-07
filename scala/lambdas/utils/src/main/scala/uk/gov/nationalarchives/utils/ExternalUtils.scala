package uk.gov.nationalarchives.utils

import io.circe.Json.Null
import io.circe.derivation.Configuration
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, Encoder, HCursor, Json}
import io.circe.syntax.*
import uk.gov.nationalarchives.utils.ExternalUtils.Type.{ArchiveFolder, ContentFolder}

import java.net.URI
import java.time.{Instant, OffsetDateTime}
import java.util.UUID

object ExternalUtils {
  enum DetailType:
    case DR2Message, DR2DevMessage

  enum RepresentationType:
    case Preservation, Access

  sealed trait MetadataObject {
    def id: UUID

    def parentId: Option[UUID]
  }

  given Encoder[Type] = {
    case Type.ArchiveFolder => Json.fromString("ArchiveFolder")
    case ContentFolder => Json.fromString("ContentFolder")
    case Type.Asset => Json.fromString("Asset")
    case Type.File => Json.fromString("File")
  }

  enum Type:
    case ArchiveFolder, ContentFolder, Asset, File

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
                                          series: String,
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
            ("series", Json.fromString(series))
          )
      }
  }

  given Encoder[MetadataObject] = {
    case ArchiveFolderMetadataObject(id, parentId, title, name, series, folderMetadataIdFields) =>
      createFolderMetadataObject(id, parentId, title, name, series, folderMetadataIdFields, ArchiveFolder)
    case ContentFolderMetadataObject(id, parentId, title, name, series, folderMetadataIdFields) =>
      createFolderMetadataObject(id, parentId, title, name, series, folderMetadataIdFields, ContentFolder)
    case AssetMetadataObject(id, parentId, title, name, originalFilesUuids, originalMetadataFilesUuids, description, transferringBody, transferCompleteDatetime, upstreamSystem, digitalAssetSource, digitalAssetSubtype, correlationId, assetMetadataIdFields) =>
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
              ("digitalAssetSubtype", digitalAssetSubtype.map(Json.fromString).getOrElse(Null)),
              ("correlationId", correlationId.map(Json.fromString).getOrElse(Null))
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

  private def decodeFolder(c: HCursor) = for {
    id <- c.downField("id").as[UUID]
    parentId <- c.downField("parentId").as[Option[UUID]]
    title <- c.downField("title").as[Option[String]]
    name <- c.downField("name").as[String]
    series <- c.downField("series").as[String]
  } yield (id, parentId, title, name, series)

  given Decoder[RepresentationType] = (c: HCursor) => c.as[String].map(RepresentationType.valueOf)

  given Decoder[IdField] = deriveDecoder[IdField]

  given Decoder[ArchiveFolderMetadataObject] = (c: HCursor) =>
    decodeFolder(c).map {
      case (id, parentId, title, name, series) => ArchiveFolderMetadataObject(id, parentId, title, name, series, getIdFields(c))
    }

  given Decoder[ContentFolderMetadataObject] = (c: HCursor) =>
    decodeFolder(c).map {
      case (id, parentId, title, name, series) => ContentFolderMetadataObject(id, parentId, title, name, series, getIdFields(c))
    }

  given Decoder[AssetMetadataObject] = (c: HCursor) =>
    for {
      id <- c.downField("id").as[UUID]
      parentId <- c.downField("parentId").as[Option[UUID]]
      title <- c.downField("title").as[String]
      name <- c.downField("name").as[String]
      originalFiles <- c.downField("originalFiles").as[List[UUID]]
      originalMetadataFiles <- c.downField("originalMetadataFiles").as[List[UUID]]
      description <- c.downField("description").as[Option[String]]
      transferringBody <- c.downField("transferringBody").as[String]
      transferCompleteDatetime <- c.downField("transferCompleteDatetime").as[String]
      upstreamSystem <- c.downField("upstreamSystem").as[String]
      digitalAssetSource <- c.downField("digitalAssetSource").as[String]
      digitalAssetSubtype <- c.downField("digitalAssetSubtype").as[Option[String]]
      correlationId <- c.downField("correlationId").as[Option[String]]
    } yield AssetMetadataObject(
      id,
      parentId,
      title,
      name,
      originalFiles,
      originalMetadataFiles,
      description,
      transferringBody,
      OffsetDateTime.parse(transferCompleteDatetime),
      upstreamSystem,
      digitalAssetSource,
      digitalAssetSubtype,
      correlationId,
      getIdFields(c)
    )

  private def getIdFields(c: HCursor) = {
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

  given Decoder[FileMetadataObject] = (c: HCursor) =>
    for {
      id <- c.downField("id").as[UUID]
      parentId <- c.downField("parentId").as[Option[UUID]]
      title <- c.downField("title").as[String]
      sortOrder <- c.downField("sortOrder").as[Int]
      name <- c.downField("name").as[String]
      fileSize <- c.downField("fileSize").as[Long]
      representationType <- c.downField("representationType").as[RepresentationType]
      representationSuffix <- c.downField("representationSuffix").as[Int]
      location <- c.downField("location").as[URI]
      checksum <- c.downField("checksum_sha256").as[String]
    } yield FileMetadataObject(id, parentId, title, sortOrder, name, fileSize, representationType, representationSuffix, location, checksum)

  given Decoder[MetadataObject] = (c: HCursor) =>
    for {
      objectType <- c.downField("type").as[String]
      metadataObject <- objectType match
        case "ArchiveFolder" => c.as[ArchiveFolderMetadataObject]
        case "ContentFolder" => c.as[ContentFolderMetadataObject]
        case "Asset" => c.as[AssetMetadataObject]
        case "File" => c.as[FileMetadataObject]
    } yield metadataObject


  case class IdField(name: String, value: String)

  case class ArchiveFolderMetadataObject(
                                          id: UUID,
                                          parentId: Option[UUID],
                                          title: Option[String],
                                          name: String,
                                          series: String,
                                          idFields: List[IdField] = Nil
                                        ) extends MetadataObject

  case class ContentFolderMetadataObject(
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
      digitalAssetSubtype: Option[String],
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
                                 checksumSha256: String
                               ) extends MetadataObject

  enum MessageType:
    override def toString: String = this match
      case IngestUpdate => "preserve.digital.asset.ingest.update"
      case IngestComplete => "preserve.digital.asset.ingest.complete"

    case IngestUpdate, IngestComplete

  enum MessageStatus(val value: String):
    case IngestedPreservation extends MessageStatus("Asset has been ingested to the Preservation System.")
    case IngestedCCDisk extends MessageStatus("Asset has been written to custodial copy disk.")
    case IngestStarted extends MessageStatus("Asset has started the ingest process.")
    case IngestError extends MessageStatus("There has been an error ingesting the asset.")

  case class NotificationMessage(id: UUID, location: URI, messageId: Option[String]=None)

  case class OutputProperties(executionId: String, messageId: UUID, parentMessageId: Option[String], timestamp: Instant, messageType: MessageType)

  case class OutputParameters(assetId: UUID, status: MessageStatus)

  case class OutputMessage(properties: OutputProperties, parameters: OutputParameters)
}
