package uk.gov.nationalarchives.dynamoformatters

import cats.implicits.*
import cats.syntax.all.*
import cats.data.*
import dynosaur.*
import dynosaur.Schema.{ReadError, WriteError}
import uk.gov.nationalarchives.dynamoformatters.DynamoWriteUtils.*
import uk.gov.nationalarchives.dynamoformatters.DynamoReadError.*
import uk.gov.nationalarchives.dynamoformatters.DynamoReadError.given

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.Try

object DynamoFormatters {

  given Schema[UUID] = Schema[String].imap(UUID.fromString)(_.toString)
  
  given Schema[FilesTablePrimaryKey] = Schema.record[FilesTablePrimaryKey] { field =>
    (
      field("id", _.partitionKey.id),
      field("batchId", _.sortKey.batchId)
    ).mapN { (id, batchId) =>
      FilesTablePrimaryKey(FilesTablePartitionKey(id), FilesTableSortKey(batchId))
    }
  }

  given Schema[AssetDynamoTable] = Schema[DynamoValue].xmap { value =>
    val rowAsMap = value.m.getOrElse(Map()).view.mapValues(_.value).toMap
    val utils = new DynamoReadUtils(rowAsMap)
    utils.readAssetRow.left.map { err =>
      val errorMessage = err.errors.map {
        case (key, error) => s"'$key': ${error.show}"
      }.toList.mkString("\n")
      ReadError(errorMessage)
    }
  }(asset => Try(writeAssetTable(asset)).toEither.left.map(err => WriteError(err.getMessage)))


  val batchId = "batchId"
  val groupId = "groupId"
  val assetId = "assetId"
  val id = "id"
  val message = "message"
  val name = "name"
  val typeField = "type"
  val fileSize = "fileSize"
  val sortOrder = "sortOrder"
  val parentPath = "parentPath"
  val title = "title"
  val description = "description"
  val checksumSha256 = "checksum_sha256"
  val fileExtension = "fileExtension"
  val transferringBody = "transferringBody"
  val transferCompleteDatetime = "transferCompleteDatetime"
  val upstreamSystem = "upstreamSystem"
  val digitalAssetSource = "digitalAssetSource"
  val digitalAssetSubtype = "digitalAssetSubtype"
  val originalFiles = "originalFiles"
  val originalMetadataFiles = "originalMetadataFiles"
  val representationType = "representationType"
  val representationSuffix = "representationSuffix"
  val ingestedPreservica = "ingested_PS"
  val ingestedCustodialCopy = "ingested_CC"
  val childCount = "childCount"
  val skipIngest = "skipIngest"
  val location = "location"
  val correlationId = "correlationId"




  enum Type:
//    def formatter: DynamoFormat[? >: ArchiveFolderDynamoTable & ContentFolderDynamoTable & AssetDynamoTable & FileDynamoTable <: DynamoTable] = this match
//      case ArchiveFolder => archiveFolderTableFormat
//      case ContentFolder => contentFolderTableFormat
//      case Asset         => assetTableFormat
//      case File          => fileTableFormat
    case ArchiveFolder, ContentFolder, Asset, File

  sealed trait DynamoTable {
    def batchId: String
    def id: UUID
    def parentPath: Option[String]
    def name: String
    def `type`: Type
    def title: Option[String]
    def description: Option[String]
    def identifiers: List[Identifier]
    def childCount: Int
  }

  private type ValidatedField[T] = ValidatedNel[(String, DynamoReadError), T]

  case class LockTableValidatedFields(
      assetId: ValidatedField[UUID],
      groupId: ValidatedField[String],
      message: ValidatedField[String]
  )

  case class FilesTableValidatedFields(
      batchId: ValidatedField[String],
      id: ValidatedField[UUID],
      name: ValidatedField[String],
      parentPath: Option[String],
      title: Option[String],
      description: Option[String],
      `type`: ValidatedField[Type],
      transferringBody: ValidatedField[String],
      transferCompleteDatetime: ValidatedField[OffsetDateTime],
      upstreamSystem: ValidatedField[String],
      digitalAssetSource: ValidatedField[String],
      digitalAssetSubtype: ValidatedField[String],
      originalFiles: ValidatedField[List[UUID]],
      originalMetadataFiles: ValidatedField[List[UUID]],
      sortOrder: ValidatedField[Int],
      fileSize: ValidatedField[Long],
      checksumSha256: ValidatedField[String],
      fileExtension: ValidatedField[String],
      representationType: ValidatedField[FileRepresentationType],
      representationSuffix: ValidatedField[Int],
      ingestedPreservica: Option[String],
      ingestedCustodialCopy: Option[String],
      identifiers: List[Identifier],
      childCount: ValidatedField[Int],
      skipIngest: ValidatedField[Boolean],
      location: ValidatedField[URI],
      correlationId: Option[String]
  )

  case class ArchiveFolderDynamoTable(
      batchId: String,
      id: UUID,
      parentPath: Option[String],
      name: String,
      `type`: Type,
      title: Option[String],
      description: Option[String],
      identifiers: List[Identifier],
      childCount: Int
  ) extends DynamoTable

  case class ContentFolderDynamoTable(
      batchId: String,
      id: UUID,
      parentPath: Option[String],
      name: String,
      `type`: Type,
      title: Option[String],
      description: Option[String],
      identifiers: List[Identifier],
      childCount: Int
  ) extends DynamoTable

  case class AssetDynamoTable(
      batchId: String,
      id: UUID,
      parentPath: Option[String],
      name: String,
      `type`: Type,
      title: Option[String],
      description: Option[String],
      transferringBody: String,
      transferCompleteDatetime: OffsetDateTime,
      upstreamSystem: String,
      digitalAssetSource: String,
      digitalAssetSubtype: String,
      originalFiles: List[UUID],
      originalMetadataFiles: List[UUID],
      ingestedPreservica: Boolean,
      ingestedCustodialCopy: Boolean,
      identifiers: List[Identifier],
      childCount: Int,
      skipIngest: Boolean,
      correlationId: Option[String]
  ) extends DynamoTable

  case class FileDynamoTable(
      batchId: String,
      id: UUID,
      parentPath: Option[String],
      name: String,
      `type`: Type,
      title: Option[String],
      description: Option[String],
      sortOrder: Int,
      fileSize: Long,
      checksumSha256: String,
      fileExtension: String,
      representationType: FileRepresentationType,
      representationSuffix: Int,
      ingestedPreservica: Boolean,
      ingestedCustodialCopy: Boolean,
      identifiers: List[Identifier],
      childCount: Int,
      location: URI
  ) extends DynamoTable

  case class Identifier(identifierName: String, value: String)

  case class FilesTablePartitionKey(id: UUID)

  case class FilesTableSortKey(batchId: String)

  case class FilesTablePrimaryKey(partitionKey: FilesTablePartitionKey, sortKey: FilesTableSortKey)
  case class LockTablePartitionKey(assetId: UUID)

  case class IngestLockTable(assetId: UUID, groupId: String, message: String)

  enum FileRepresentationType:
    override def toString: String = this match
      case PreservationRepresentationType => "Preservation"
      case AccessRepresentationType       => "Access"

    case PreservationRepresentationType, AccessRepresentationType

}
