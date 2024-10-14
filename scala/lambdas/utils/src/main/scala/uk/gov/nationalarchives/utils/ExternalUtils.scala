package uk.gov.nationalarchives.utils

import io.circe.Json.Null
import io.circe.{Encoder, Json}
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
  
  private def createFolderMetadataObject(id: UUID, parentId: Option[UUID], title: Option[String], name: String, series: String, folderMetadataIdFields: List[IdField], folderType: Type) = {
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
              ("originalFiles", Json.fromValues(convertListOfUuidsToJsonStrArray(originalFilesUuids))),
              ("originalMetadataFiles", Json.fromValues(convertListOfUuidsToJsonStrArray(originalMetadataFilesUuids))),
              ("description", description.map(Json.fromString).getOrElse(Null)),
              ("transferringBody", Json.fromString(transferringBody)),
              ("transferCompleteDatetime", Json.fromString(transferCompleteDatetime.toString)),
              ("upstreamSystem", Json.fromString(upstreamSystem)),
              ("digitalAssetSource", Json.fromString(digitalAssetSource)),
              ("digitalAssetSubtype", Json.fromString(digitalAssetSubtype)),
              ("correlationId", correlationId.map(Json.fromString).getOrElse(Null)),
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
                                  digitalAssetSubtype: String,
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

  case class OutputProperties(executionId: String, messageId: UUID, parentMessageId: Option[String], timestamp: Instant, messageType:  MessageType)

  case class OutputParameters(assetId: UUID, status: MessageStatus)

  case class OutputMessage(properties: OutputProperties, parameters: OutputParameters)
}
