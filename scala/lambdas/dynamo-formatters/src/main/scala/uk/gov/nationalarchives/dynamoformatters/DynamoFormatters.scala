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
import java.time.Instant

object DynamoFormatters {

  final val checksumPrefix = "checksum_"

  private def createReadDynamoUtils(dynamoValue: DynamoValue) = {
    val folderItemAsMap = dynamoValue.toAttributeValue.m().asScala.toMap
    new DynamoReadUtils(folderItemAsMap)
  }

  given archiveFolderItemFormat: DynamoFormat[ArchiveFolderDynamoItem] =
    new DynamoFormat[ArchiveFolderDynamoItem] {
      override def read(dynamoValue: DynamoValue): Either[DynamoReadError, ArchiveFolderDynamoItem] =
        createReadDynamoUtils(dynamoValue).readArchiveFolderItem

      override def write(table: ArchiveFolderDynamoItem): DynamoValue =
        writeArchiveFolderItem(table)
    }

  given contentFolderItemFormat: DynamoFormat[ContentFolderDynamoItem] =
    new DynamoFormat[ContentFolderDynamoItem] {
      override def read(dynamoValue: DynamoValue): Either[DynamoReadError, ContentFolderDynamoItem] =
        createReadDynamoUtils(dynamoValue).readContentFolderItem

      override def write(table: ContentFolderDynamoItem): DynamoValue =
        writeContentFolderItem(table)
    }

  given assetItemFormat: DynamoFormat[AssetDynamoItem] = new DynamoFormat[AssetDynamoItem] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, AssetDynamoItem] =
      createReadDynamoUtils(dynamoValue).readAssetRow

    override def write(table: AssetDynamoItem): DynamoValue =
      writeAssetItem(table)
  }

  given fileItemFormat: DynamoFormat[FileDynamoItem] = new DynamoFormat[FileDynamoItem] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, FileDynamoItem] =
      createReadDynamoUtils(dynamoValue).readFileRow

    override def write(table: FileDynamoItem): DynamoValue =
      writeFileItem(table)
  }

  given ingestLockTableItemFormat: DynamoFormat[IngestLockTableItem] = new DynamoFormat[IngestLockTableItem] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, IngestLockTableItem] =
      createReadDynamoUtils(dynamoValue).readLockTableItem

    override def write(ingestLockTableItem: IngestLockTableItem): DynamoValue = writeLockTableItem(ingestLockTableItem)
  }

  given ingestQueueTableItemFormat: DynamoFormat[IngestQueueTableItem] = new DynamoFormat[IngestQueueTableItem]:
    override def read(av: DynamoValue): Either[DynamoReadError, IngestQueueTableItem] =
      createReadDynamoUtils(av).readIngestQueueTableItem

    override def write(t: IngestQueueTableItem): DynamoValue = writeIngestQueueTableItem(t)

  // Attribute names as defined in the dynamo table
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
  val queuedAt = "queuedAt"
  val sourceSystem = "sourceSystem"
  val taskToken = "taskToken"
  val executionName = "executionName"

  private def validateProperty(av: DynamoValue, name: String) =
    av.toAttributeValue.m().asScala.get(name).map(_.s()).map(Validated.Valid.apply).getOrElse(Validated.Invalid(name -> MissingProperty)).toValidatedNel

  given filesTablePkFormat: Typeclass[FilesTablePrimaryKey] = new DynamoFormat[FilesTablePrimaryKey]:
    override def read(av: DynamoValue): Either[DynamoReadError, FilesTablePrimaryKey] = {

      (validateProperty(av, id), validateProperty(av, batchId))
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

  given queueTablePkFormat: Typeclass[IngestQueuePrimaryKey] = new DynamoFormat[IngestQueuePrimaryKey]:
    override def read(av: DynamoValue): Either[DynamoReadError, IngestQueuePrimaryKey] = {

      (validateProperty(av, sourceSystem), validateProperty(av, queuedAt))
        .mapN { (sourceSystem, queuedAt) =>
          IngestQueuePrimaryKey(IngestQueuePartitionKey(sourceSystem), IngestQueueSortKey(Instant.parse(queuedAt)))
        }
        .toEither
        .left
        .map(InvalidPropertiesError.apply)
    }

    override def write(t: IngestQueuePrimaryKey): DynamoValue = {
      DynamoValue.fromMap(Map(sourceSystem -> DynamoValue.fromString(t.partitionKey.sourceSystem), queuedAt -> DynamoValue.fromString(t.sortKey.queuedAt.toString)))
    }

  given lockTablePkFormat: Typeclass[LockTablePartitionKey] = deriveDynamoFormat[LockTablePartitionKey]

  given Typeclass[IngestQueuePartitionKey] = deriveDynamoFormat[IngestQueuePartitionKey]
  given Typeclass[IngestQueueSortKey] = deriveDynamoFormat[IngestQueueSortKey]

  given typeFormatter: DynamoFormat[Type] = new DynamoFormat[Type]:
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, Type] = dynamoValue.as[String].map(Type.valueOf)

    override def write(t: Type): DynamoValue = DynamoValue.fromString(t.toString)

  enum Type:
    def formatter: DynamoFormat[? >: ArchiveFolderDynamoItem & ContentFolderDynamoItem & AssetDynamoItem & FileDynamoItem <: DynamoItem] = this match
      case ArchiveFolder => archiveFolderItemFormat
      case ContentFolder => contentFolderItemFormat
      case Asset         => assetItemFormat
      case File          => fileItemFormat
    case ArchiveFolder, ContentFolder, Asset, File

  sealed trait DynamoItem {
    def batchId: String
    def id: UUID
    def potentialParentPath: Option[String]
    def `type`: Type
    def potentialTitle: Option[String]
    def potentialDescription: Option[String]
    def identifiers: List[Identifier]
    def childCount: Int
  }

  sealed trait FolderDynamoItem extends DynamoItem {
    def name: String
  }

  private type ValidatedAttribute[T] = ValidatedNel[(FieldName, DynamoReadError), T]

  case class LockTableValidatedAttributes(
      assetId: ValidatedAttribute[UUID],
      groupId: ValidatedAttribute[String],
      message: ValidatedAttribute[String]
  )

  case class FilesTableValidatedAttributes(
      batchId: ValidatedAttribute[String],
      id: ValidatedAttribute[UUID],
      name: ValidatedAttribute[String],
      potentialParentPath: Option[String],
      potentialTitle: Option[String],
      potentialDescription: Option[String],
      `type`: ValidatedAttribute[Type],
      transferringBody: ValidatedAttribute[String],
      transferCompleteDatetime: ValidatedAttribute[OffsetDateTime],
      upstreamSystem: ValidatedAttribute[String],
      digitalAssetSource: ValidatedAttribute[String],
      potentialDigitalAssetSubtype: Option[String],
      originalFiles: ValidatedAttribute[List[UUID]],
      originalMetadataFiles: ValidatedAttribute[List[UUID]],
      sortOrder: ValidatedAttribute[Int],
      fileSize: ValidatedAttribute[Long],
      checksums: ValidatedAttribute[List[Checksum]],
      fileExtension: Option[String],
      representationType: ValidatedAttribute[FileRepresentationType],
      representationSuffix: ValidatedAttribute[Int],
      ingestedPreservica: Option[String],
      ingestedCustodialCopy: Option[String],
      identifiers: List[Identifier],
      childCount: ValidatedAttribute[Int],
      skipIngest: ValidatedAttribute[Boolean],
      location: ValidatedAttribute[URI],
      correlationId: Option[String]
  )

  case class ArchiveFolderDynamoItem(
      batchId: String,
      id: UUID,
      potentialParentPath: Option[String],
      name: String,
      `type`: Type,
      potentialTitle: Option[String],
      potentialDescription: Option[String],
      identifiers: List[Identifier],
      childCount: Int
  ) extends FolderDynamoItem

  case class ContentFolderDynamoItem(
      batchId: String,
      id: UUID,
      potentialParentPath: Option[String],
      name: String,
      `type`: Type,
      potentialTitle: Option[String],
      potentialDescription: Option[String],
      identifiers: List[Identifier],
      childCount: Int
  ) extends FolderDynamoItem

  case class AssetDynamoItem(
      batchId: String,
      id: UUID,
      potentialParentPath: Option[String],
      `type`: Type,
      potentialTitle: Option[String],
      potentialDescription: Option[String],
      transferringBody: String,
      transferCompleteDatetime: OffsetDateTime,
      upstreamSystem: String,
      digitalAssetSource: String,
      potentialDigitalAssetSubtype: Option[String],
      originalFiles: List[UUID],
      originalMetadataFiles: List[UUID],
      ingestedPreservica: Boolean,
      ingestedCustodialCopy: Boolean,
      identifiers: List[Identifier],
      childCount: Int,
      skipIngest: Boolean,
      correlationId: Option[String]
  ) extends DynamoItem

  case class FileDynamoItem(
      batchId: String,
      id: UUID,
      potentialParentPath: Option[String],
      name: String,
      `type`: Type,
      potentialTitle: Option[String],
      potentialDescription: Option[String],
      sortOrder: Int,
      fileSize: Long,
      checksums: List[Checksum],
      potentialFileExtension: Option[String],
      representationType: FileRepresentationType,
      representationSuffix: Int,
      ingestedPreservica: Boolean,
      ingestedCustodialCopy: Boolean,
      identifiers: List[Identifier],
      childCount: Int,
      location: URI
  ) extends DynamoItem

  case class Identifier(identifierName: String, value: String)

  case class FilesTablePartitionKey(id: UUID)

  case class FilesTableSortKey(batchId: String)

  case class FilesTablePrimaryKey(partitionKey: FilesTablePartitionKey, sortKey: FilesTableSortKey)
  case class LockTablePartitionKey(assetId: UUID)

  case class IngestLockTableItem(assetId: UUID, groupId: String, message: String)

  case class IngestQueueTableItem(sourceSystem: String, queuedAt: Instant, taskToken: String)
  case class IngestQueuePartitionKey(sourceSystem: String)
  case class IngestQueueSortKey(queuedAt: Instant)
  case class IngestQueuePrimaryKey(partitionKey: IngestQueuePartitionKey, sortKey: IngestQueueSortKey)

  enum FileRepresentationType:
    override def toString: String = this match
      case PreservationRepresentationType => "Preservation"
      case AccessRepresentationType       => "Access"

    case PreservationRepresentationType, AccessRepresentationType

  case class Checksum(algorithm: String, fingerprint: String)
}
