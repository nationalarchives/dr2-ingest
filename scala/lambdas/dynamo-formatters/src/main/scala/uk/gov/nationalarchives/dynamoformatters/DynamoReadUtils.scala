package uk.gov.nationalarchives.dynamoformatters

import cats.data.*
import cats.implicits.*
import org.scanamo.*
import org.scanamo.generic.semiauto.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Type.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.FileRepresentationType.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*

import java.lang
import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.reflect.{ClassTag, classTag}

class DynamoReadUtils(folderItemAsMap: Map[String, AttributeValue]) {

  private type InvalidProperty = (String, DynamoReadError)

  val identifiers: List[Identifier] = folderItemAsMap.collect {
    case (name, value) if name.startsWith("id_") => Identifier(name.drop(3), value.s())
  }.toList

  val checksums: List[Checksum] = folderItemAsMap.collect {
    case (name, value) if name.startsWith(checksumPrefix) => Checksum(name.drop(checksumPrefix.length), value.s())
  }.toList

  private val allValidatedPostIngestStatusTableAttributes: PostIngestStatusTableValidatedAttributes = PostIngestStatusTableValidatedAttributes(
    stringToScalaType[UUID](
      assetId,
      getPotentialStringValue(assetId),
      UUID.fromString
    ),
    getValidatedMandatoryAttributeAsString(batchId),
    getValidatedMandatoryAttributeAsString(input),
    getPotentialStringValue(correlationId),
    getPotentialStringValue(queue),
    getPotentialStringValue(firstQueued),
    getPotentialStringValue(lastQueued),
    getPotentialStringValue(resultCC)
  )

  private val allValidatedLockTableAttributes: LockTableValidatedAttributes = LockTableValidatedAttributes(
    stringToScalaType[UUID](
      assetId,
      getPotentialStringValue(assetId),
      UUID.fromString
    ),
    getValidatedMandatoryAttributeAsString(groupId),
    getValidatedMandatoryAttributeAsString(message),
    getValidatedMandatoryAttributeAsString(createdAt)
  )

  private val allValidatedFileTableAttributes: FilesTableValidatedAttributes = FilesTableValidatedAttributes(
    getValidatedMandatoryAttributeAsString(batchId),
    stringToScalaType[UUID](
      id,
      getPotentialStringValue(id),
      UUID.fromString
    ),
    getValidatedMandatoryAttributeAsString(name),
    getPotentialStringValue(parentPath),
    getPotentialStringValue(title),
    getPotentialStringValue(description),
    stringToType(getPotentialStringValue(typeField)),
    getPotentialStringValue(transferringBody),
    getPotentialStringValue(transferCompleteDatetime).map(OffsetDateTime.parse),
    getValidatedMandatoryAttributeAsString(upstreamSystem),
    getValidatedMandatoryAttributeAsString(digitalAssetSource),
    getPotentialStringValue(digitalAssetSubtype),
    getPotentialListOfValues(originalMetadataFiles, convertListOfStringsToT(UUID.fromString)),
    getNumber(sortOrder, _.toInt),
    getNumber(fileSize, _.toLong),
    getValidatedMandatoryChecksumsAsList(checksums),
    getPotentialStringValue(fileExtension),
    stringToRepresentationType(getPotentialStringValue(representationType)),
    getNumber(representationSuffix, _.toInt),
    identifiers,
    getNumber(childCount, _.toInt),
    getBoolean(skipIngest),
    stringToScalaType[URI](location, getPotentialStringValue(location), URI.create),
    getPotentialStringValue(correlationId),
    getValidatedMandatoryAttributeAsString(filePath)
  )

  private def stringToType(potentialTypeString: Option[String]): ValidatedNel[InvalidProperty, Type] =
    potentialTypeString match {
      case Some("ArchiveFolder") => ArchiveFolder.validNel
      case Some("ContentFolder") => ContentFolder.validNel
      case Some("Asset")         => Asset.validNel
      case Some("File")          => File.validNel
      case Some(otherTypeString) =>
        (typeField -> TypeCoercionError(new Exception(s"Type $otherTypeString not found"))).invalidNel
      case None => (typeField -> MissingProperty).invalidNel
    }

  private def stringToRepresentationType(
      potentialRepresentationTypeString: Option[String]
  ): ValidatedNel[InvalidProperty, FileRepresentationType] =
    potentialRepresentationTypeString match {
      case Some("Preservation") => PreservationRepresentationType.validNel
      case Some("Access")       => AccessRepresentationType.validNel
      case Some(otherRepresentationTypeString) =>
        (representationType -> TypeCoercionError(
          new Exception(s"Representation type $otherRepresentationTypeString not found")
        )).invalidNel
      case None => (representationType -> MissingProperty).invalidNel
    }

  private def typeCoercionError[T: ClassTag](name: String, value: String): (FieldName, TypeCoercionError) =
    name -> TypeCoercionError(
      new RuntimeException(s"Cannot parse $value for field $name into ${classTag[T].runtimeClass}")
    )

  private def getBoolean(name: String): ValidatedNel[InvalidProperty, Boolean] = {
    folderItemAsMap
      .get(name)
      .map { attributeValue =>
        attributeValue.`type`() match {
          case BOOL =>
            Validated.Valid(attributeValue.bool().booleanValue()).toValidatedNel
          case _ =>
            (name, NoPropertyOfType("Boolean", DynamoValue.fromAttributeValue(attributeValue))).invalidNel
        }
      }
      .getOrElse({
        Validated.Valid(false).toValidatedNel
      })
  }

  private def getNumber[T: ClassTag](name: String, toNumberFunction: String => T): ValidatedNel[InvalidProperty, T] = {
    folderItemAsMap
      .get(name)
      .map { attributeValue =>
        attributeValue.`type`() match {
          case N =>
            val value = attributeValue.n()
            Validated
              .catchOnly[Throwable](toNumberFunction(value))
              .leftMap(_ => typeCoercionError(name, value))
              .toValidatedNel
          case _ => (name -> NoPropertyOfType("Number", DynamoValue.fromAttributeValue(attributeValue))).invalidNel
        }
      }
      .getOrElse((name -> MissingProperty).invalidNel)
  }

  private def getValidatedMandatoryAttributeAsString(name: String): ValidatedNel[InvalidProperty, String] = {
    getPotentialStringValue(name)
      .map(_.validNel)
      .getOrElse((name -> MissingProperty).invalidNel)
  }

  private def getValidatedMandatoryChecksumsAsList(listOfValues: List[Checksum]): ValidatedNel[InvalidProperty, List[Checksum]] = {
    listOfValues match {
      case Nil => ("checksum" -> MissingProperty).invalidNel
      case _   => listOfValues.validNel
    }
  }

  private def getPotentialStringValue(name: String): Option[FieldName] = folderItemAsMap.get(name).map(_.s())

  private def getPotentialListOfValues[T](
      name: String,
      convertListOfAttributesToT: (
          String,
          List[AttributeValue]
      ) => ValidatedNel[(FieldName, DynamoReadError), List[T]]
  ): ValidatedNel[InvalidProperty, List[T]] =
    folderItemAsMap
      .get(name)
      .map { attributeValue =>
        attributeValue.`type`() match {
          case L =>
            val attributes: List[AttributeValue] = attributeValue.l().asScala.toList
            convertListOfAttributesToT(name, attributes)
          case _ => (name -> NoPropertyOfType("List", DynamoValue.fromAttributeValue(attributeValue))).invalidNel
        }
      }
      .getOrElse((name -> MissingProperty).invalidNel)

  private def stringToScalaType[T: ClassTag](
      name: String,
      potentialString: Option[String],
      toScalaTypeFunction: String => T
  ): ValidatedNel[InvalidProperty, T] =
    potentialString match {
      case Some(value) =>
        Validated
          .catchOnly[Throwable](toScalaTypeFunction(value))
          .leftMap(_ => typeCoercionError[T](name, value))
          .toValidatedNel

      case None => (name -> MissingProperty).invalidNel
    }

  private def convertListOfStringsToT[T: ClassTag](fromStringToAnotherType: String => T)(
      attributeName: String,
      attributes: List[AttributeValue]
  ): ValidatedNel[(FieldName, DynamoReadError), List[T]] =
    attributes
      .map(stringValue => stringToScalaType(attributeName, Option(stringValue.s()), fromStringToAnotherType))
      .sequence

  def readStateTableItem: Either[InvalidPropertiesError, PostIngestStateTableItem] =
    (
      allValidatedPostIngestStatusTableAttributes.assetId,
      allValidatedPostIngestStatusTableAttributes.batchId,
      allValidatedPostIngestStatusTableAttributes.input
    ).mapN { (assetId, batchId, input) =>
      PostIngestStateTableItem(
        assetId,
        batchId,
        input,
        allValidatedPostIngestStatusTableAttributes.correlationId,
        allValidatedPostIngestStatusTableAttributes.queue,
        allValidatedPostIngestStatusTableAttributes.firstQueued,
        allValidatedPostIngestStatusTableAttributes.lastQueued,
        allValidatedPostIngestStatusTableAttributes.resultCC
      )
    }.toEither
      .left
      .map(InvalidPropertiesError.apply)

  def readLockTableItem: Either[InvalidPropertiesError, IngestLockTableItem] =
    (
      allValidatedLockTableAttributes.assetId,
      allValidatedLockTableAttributes.groupId,
      allValidatedLockTableAttributes.message,
      allValidatedLockTableAttributes.createdAt
    ).mapN { (assetId, groupId, message, createdAt) =>
      IngestLockTableItem(assetId, groupId, message, createdAt)
    }.toEither
      .left
      .map(InvalidPropertiesError.apply)

  def readIngestQueueTableItem: Either[InvalidPropertiesError, IngestQueueTableItem] =
    (
      getValidatedMandatoryAttributeAsString(sourceSystem),
      getValidatedMandatoryAttributeAsString(queuedAt),
      getValidatedMandatoryAttributeAsString(taskToken),
      getValidatedMandatoryAttributeAsString(executionName)
    ).mapN(IngestQueueTableItem.apply).toEither.left.map(InvalidPropertiesError.apply)

  def readArchiveFolderItem: Either[InvalidPropertiesError, ArchiveFolderDynamoItem] =
    (
      allValidatedFileTableAttributes.batchId,
      allValidatedFileTableAttributes.id,
      allValidatedFileTableAttributes.name,
      allValidatedFileTableAttributes.`type`,
      allValidatedFileTableAttributes.childCount
    ).mapN { (batchId, id, name, rowType, childCount) =>
      ArchiveFolderDynamoItem(
        batchId,
        id,
        allValidatedFileTableAttributes.potentialParentPath,
        name,
        rowType,
        allValidatedFileTableAttributes.potentialTitle,
        allValidatedFileTableAttributes.potentialDescription,
        allValidatedFileTableAttributes.identifiers,
        childCount
      )
    }.toEither
      .left
      .map(InvalidPropertiesError.apply)

  def readContentFolderItem: Either[InvalidPropertiesError, ContentFolderDynamoItem] =
    (
      allValidatedFileTableAttributes.batchId,
      allValidatedFileTableAttributes.id,
      allValidatedFileTableAttributes.name,
      allValidatedFileTableAttributes.`type`,
      allValidatedFileTableAttributes.childCount
    ).mapN { (batchId, id, name, rowType, childCount) =>
      ContentFolderDynamoItem(
        batchId,
        id,
        allValidatedFileTableAttributes.potentialParentPath,
        name,
        rowType,
        allValidatedFileTableAttributes.potentialTitle,
        allValidatedFileTableAttributes.potentialDescription,
        allValidatedFileTableAttributes.identifiers,
        childCount
      )
    }.toEither
      .left
      .map(InvalidPropertiesError.apply)

  def readAssetRow: Either[InvalidPropertiesError, AssetDynamoItem] =
    (
      allValidatedFileTableAttributes.batchId,
      allValidatedFileTableAttributes.id,
      allValidatedFileTableAttributes.upstreamSystem,
      allValidatedFileTableAttributes.digitalAssetSource,
      allValidatedFileTableAttributes.originalMetadataFiles,
      allValidatedFileTableAttributes.`type`,
      allValidatedFileTableAttributes.childCount,
      allValidatedFileTableAttributes.skipIngest,
      allValidatedFileTableAttributes.filePath
    ).mapN {
      (
          batchId,
          id,
          upstreamSystem,
          digitalAssetSource,
          originalMetadataFiles,
          rowType,
          childCount,
          skipIngest,
          filePath
      ) =>
        AssetDynamoItem(
          batchId,
          id,
          allValidatedFileTableAttributes.potentialParentPath,
          rowType,
          allValidatedFileTableAttributes.potentialTitle,
          allValidatedFileTableAttributes.potentialDescription,
          allValidatedFileTableAttributes.transferringBody,
          allValidatedFileTableAttributes.transferCompleteDatetime,
          upstreamSystem,
          digitalAssetSource,
          allValidatedFileTableAttributes.potentialDigitalAssetSubtype,
          originalMetadataFiles,
          allValidatedFileTableAttributes.identifiers,
          childCount,
          skipIngest,
          allValidatedFileTableAttributes.correlationId,
          filePath
        )
    }.toEither
      .left
      .map(InvalidPropertiesError.apply)

  def readFileRow: Either[InvalidPropertiesError, FileDynamoItem] =
    (
      allValidatedFileTableAttributes.batchId,
      allValidatedFileTableAttributes.id,
      allValidatedFileTableAttributes.name,
      allValidatedFileTableAttributes.sortOrder,
      allValidatedFileTableAttributes.fileSize,
      allValidatedFileTableAttributes.checksums,
      allValidatedFileTableAttributes.`type`,
      allValidatedFileTableAttributes.representationType,
      allValidatedFileTableAttributes.representationSuffix,
      allValidatedFileTableAttributes.childCount,
      allValidatedFileTableAttributes.location
    ).mapN {
      (
          batchId,
          id,
          name,
          sortOrder,
          fileSize,
          checksumList,
          rowType,
          representationType,
          representationSuffix,
          childCount,
          location
      ) =>
        FileDynamoItem(
          batchId,
          id,
          allValidatedFileTableAttributes.potentialParentPath,
          name,
          rowType,
          allValidatedFileTableAttributes.potentialTitle,
          allValidatedFileTableAttributes.potentialDescription,
          sortOrder,
          fileSize,
          checksumList,
          allValidatedFileTableAttributes.fileExtension,
          representationType,
          representationSuffix,
          allValidatedFileTableAttributes.identifiers,
          childCount,
          location
        )
    }.toEither
      .left
      .map(InvalidPropertiesError.apply)
}
