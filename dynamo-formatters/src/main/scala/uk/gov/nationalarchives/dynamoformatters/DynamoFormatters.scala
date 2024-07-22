package uk.gov.nationalarchives.dynamoformatters

import cats.implicits.*
import cats.data.*
import org.scanamo.*
import org.scanamo.generic.semiauto.{FieldName, Typeclass, deriveDynamoFormat}
import uk.gov.nationalarchives.dynamoformatters.DynamoWriteUtils.*

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID
import scala.jdk.CollectionConverters.*

object DynamoFormatters {

  private def createReadDynamoUtils(dynamoValue: DynamoValue) = {
    val folderRowAsMap = dynamoValue.toAttributeValue.m().asScala.toMap
    new DynamoReadUtils(folderRowAsMap)
  }

  given archiveFolderTableFormat: DynamoFormat[ArchiveFolderDynamoTable] =
    new DynamoFormat[ArchiveFolderDynamoTable] {
      override def read(dynamoValue: DynamoValue): Either[DynamoReadError, ArchiveFolderDynamoTable] =
        createReadDynamoUtils(dynamoValue).readArchiveFolderRow

      override def write(table: ArchiveFolderDynamoTable): DynamoValue =
        writeArchiveFolderTable(table)
    }

  given contentFolderTableFormat: DynamoFormat[ContentFolderDynamoTable] =
    new DynamoFormat[ContentFolderDynamoTable] {
      override def read(dynamoValue: DynamoValue): Either[DynamoReadError, ContentFolderDynamoTable] =
        createReadDynamoUtils(dynamoValue).readContentFolderRow

      override def write(table: ContentFolderDynamoTable): DynamoValue =
        writeContentFolderTable(table)
    }

  given assetTableFormat: DynamoFormat[AssetDynamoTable] = new DynamoFormat[AssetDynamoTable] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, AssetDynamoTable] =
      createReadDynamoUtils(dynamoValue).readAssetRow

    override def write(table: AssetDynamoTable): DynamoValue =
      writeAssetTable(table)
  }

  given fileTableFormat: DynamoFormat[FileDynamoTable] = new DynamoFormat[FileDynamoTable] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, FileDynamoTable] =
      createReadDynamoUtils(dynamoValue).readFileRow

    override def write(table: FileDynamoTable): DynamoValue =
      writeFileTable(table)
  }

  given ingestLockTableFormat: DynamoFormat[IngestLockTable] = new DynamoFormat[IngestLockTable] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, IngestLockTable] =
      createReadDynamoUtils(dynamoValue).readLockTableRow

    override def write(ingestLockTable: IngestLockTable): DynamoValue = writeLockTable(ingestLockTable)
  }

  val batchId = "batchId"
  val ioId = "ioId"
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
  val childCount = "childCount"
  val skipIngest = "skipIngest"
  val location = "location"

  given filesTablePkFormat: Typeclass[FilesTablePrimaryKey] = new DynamoFormat[FilesTablePrimaryKey]:
    override def read(av: DynamoValue): Either[DynamoReadError, FilesTablePrimaryKey] = {
      val valueMap = av.toAttributeValue.m().asScala

      def validateProperty(name: String) =
        valueMap.get(name).map(_.s()).map(Validated.Valid.apply).getOrElse(Validated.Invalid(name -> MissingProperty)).toValidatedNel

      (validateProperty(id), validateProperty(batchId))
        .mapN { (id, batchId) =>
          FilesTablePrimaryKey(FilesTablePartitionKey(UUID.fromString(id)), FilesTableSortKey(batchId))
        }
        .toEither
        .left
        .map(InvalidPropertiesError.apply)
    }

    override def write(t: FilesTablePrimaryKey): DynamoValue = {
      DynamoValue.fromMap(Map(id -> DynamoValue.fromString(t.partitionKey.id.toString), batchId -> DynamoValue.fromString(t.sortKey.batchId)))
    }

  given lockTablePkFormat: Typeclass[LockTablePartitionKey] = deriveDynamoFormat[LockTablePartitionKey]

  enum Type:
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

  private type ValidatedField[T] = ValidatedNel[(FieldName, DynamoReadError), T]

  case class LockTableValidatedFields(
      assetId: ValidatedField[UUID],
      batchId: ValidatedField[String],
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
      identifiers: List[Identifier],
      childCount: ValidatedField[Int],
      skipIngest: ValidatedField[Boolean],
      location: ValidatedField[URI]
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
      identifiers: List[Identifier],
      childCount: Int,
      skipIngest: Boolean
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
      identifiers: List[Identifier],
      childCount: Int,
      location: URI
  ) extends DynamoTable

  case class Identifier(identifierName: String, value: String)

  case class FilesTablePartitionKey(id: UUID)

  case class FilesTableSortKey(batchId: String)

  case class FilesTablePrimaryKey(partitionKey: FilesTablePartitionKey, sortKey: FilesTableSortKey)
  case class LockTablePartitionKey(ioId: UUID)

  case class IngestLockTable(ioId: UUID, batchId: String, message: String)

  enum FileRepresentationType:
    override def toString: String = this match
      case PreservationRepresentationType => "Preservation"
      case AccessRepresentationType       => "Access"

    case PreservationRepresentationType, AccessRepresentationType

}
