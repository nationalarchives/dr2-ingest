package uk.gov.nationalarchives.dynamoformatters

import cats.data.*
import cats.implicits.*
import org.scanamo.*
import org.scanamo.generic.semiauto.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Type.*

import java.time.OffsetDateTime
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.reflect.{ClassTag, classTag}
import DynamoFormatters.Type.*
import DynamoFormatters.FileRepresentationType.*
import DynamoFormatters.*

import java.lang
import java.net.URI

class DynamoReadUtils(folderRowAsMap: Map[String, AttributeValue]) {

  private type InvalidProperty = (String, DynamoReadError)

  val identifiers: List[Identifier] = folderRowAsMap.collect {
    case (name, value) if name.startsWith("id_") => Identifier(name.drop(3), value.s())
  }.toList

  val checksums: List[Checksum] = folderRowAsMap.collect {
    case (name, value) if name.startsWith(checksumPrefix) => Checksum(name.drop(checksumPrefix.length), value.s())
  }.toList

  private val allValidatedLockTableFields: LockTableValidatedFields = LockTableValidatedFields(
    stringToScalaType[UUID](
      assetId,
      getPotentialStringValue(assetId),
      UUID.fromString
    ),
    getValidatedMandatoryFieldAsString(groupId),
    getValidatedMandatoryFieldAsString(message)
  )

  private val allValidatedFileTableFields: FilesTableValidatedFields = FilesTableValidatedFields(
    getValidatedMandatoryFieldAsString(batchId),
    stringToScalaType[UUID](
      id,
      getPotentialStringValue(id),
      UUID.fromString
    ),
    getValidatedMandatoryFieldAsString(name),
    getPotentialStringValue(parentPath),
    getPotentialStringValue(title),
    getPotentialStringValue(description),
    stringToType(getPotentialStringValue(typeField)),
    getValidatedMandatoryFieldAsString(transferringBody),
    stringToScalaType[OffsetDateTime](
      transferCompleteDatetime,
      getPotentialStringValue(transferCompleteDatetime),
      OffsetDateTime.parse
    ),
    getValidatedMandatoryFieldAsString(upstreamSystem),
    getValidatedMandatoryFieldAsString(digitalAssetSource),
    getValidatedMandatoryFieldAsString(digitalAssetSubtype),
    getPotentialListOfValues(originalFiles, convertListOfStringsToT(UUID.fromString)),
    getPotentialListOfValues(originalMetadataFiles, convertListOfStringsToT(UUID.fromString)),
    getNumber(sortOrder, _.toInt),
    getNumber(fileSize, _.toLong),
    getValidatedMandatoryChecksumsAsList(checksums),
    getValidatedMandatoryFieldAsString(fileExtension),
    stringToRepresentationType(getPotentialStringValue(representationType)),
    getNumber(representationSuffix, _.toInt),
    getPotentialStringValue(ingestedPreservica),
    getPotentialStringValue(ingestedCustodialCopy),
    identifiers,
    getNumber(childCount, _.toInt),
    getBoolean(skipIngest),
    stringToScalaType[URI](location, getPotentialStringValue(location), URI.create),
    getPotentialStringValue(correlationId)
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
    folderRowAsMap
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
    folderRowAsMap
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

  private def getValidatedMandatoryFieldAsString(name: String): ValidatedNel[InvalidProperty, String] = {
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

  private def getPotentialStringValue(name: String): Option[FieldName] = folderRowAsMap.get(name).map(_.s())

  private def getPotentialListOfValues[T](
      name: String,
      convertListOfAttributesToT: (
          String,
          List[AttributeValue]
      ) => ValidatedNel[(FieldName, DynamoReadError), List[T]]
  ): ValidatedNel[InvalidProperty, List[T]] =
    folderRowAsMap
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

  def readLockTableRow: Either[InvalidPropertiesError, IngestLockTable] =
    (
      allValidatedLockTableFields.assetId,
      allValidatedLockTableFields.groupId,
      allValidatedLockTableFields.message
    ).mapN { (assetId, groupId, message) =>
      IngestLockTable(assetId, groupId, message)
    }.toEither
      .left
      .map(InvalidPropertiesError.apply)

  def readArchiveFolderRow: Either[InvalidPropertiesError, ArchiveFolderDynamoTable] =
    (
      allValidatedFileTableFields.batchId,
      allValidatedFileTableFields.id,
      allValidatedFileTableFields.name,
      allValidatedFileTableFields.`type`,
      allValidatedFileTableFields.childCount
    ).mapN { (batchId, id, name, rowType, childCount) =>
      ArchiveFolderDynamoTable(
        batchId,
        id,
        allValidatedFileTableFields.parentPath,
        name,
        rowType,
        allValidatedFileTableFields.title,
        allValidatedFileTableFields.description,
        allValidatedFileTableFields.identifiers,
        childCount
      )
    }.toEither
      .left
      .map(InvalidPropertiesError.apply)

  def readContentFolderRow: Either[InvalidPropertiesError, ContentFolderDynamoTable] =
    (
      allValidatedFileTableFields.batchId,
      allValidatedFileTableFields.id,
      allValidatedFileTableFields.name,
      allValidatedFileTableFields.`type`,
      allValidatedFileTableFields.childCount
    ).mapN { (batchId, id, name, rowType, childCount) =>
      ContentFolderDynamoTable(
        batchId,
        id,
        allValidatedFileTableFields.parentPath,
        name,
        rowType,
        allValidatedFileTableFields.title,
        allValidatedFileTableFields.description,
        allValidatedFileTableFields.identifiers,
        childCount
      )
    }.toEither
      .left
      .map(InvalidPropertiesError.apply)

  def readAssetRow: Either[InvalidPropertiesError, AssetDynamoTable] =
    (
      allValidatedFileTableFields.batchId,
      allValidatedFileTableFields.id,
      allValidatedFileTableFields.name,
      allValidatedFileTableFields.transferringBody,
      allValidatedFileTableFields.transferCompleteDatetime,
      allValidatedFileTableFields.upstreamSystem,
      allValidatedFileTableFields.digitalAssetSource,
      allValidatedFileTableFields.digitalAssetSubtype,
      allValidatedFileTableFields.originalFiles,
      allValidatedFileTableFields.originalMetadataFiles,
      allValidatedFileTableFields.`type`,
      allValidatedFileTableFields.childCount,
      allValidatedFileTableFields.skipIngest
    ).mapN {
      (
          batchId,
          id,
          name,
          transferringBody,
          transferCompletedDatetime,
          upstreamSystem,
          digitalAssetSource,
          digitalAssetSubtype,
          originalFiles,
          originalMetadataFiles,
          rowType,
          childCount,
          skipIngest
      ) =>
        AssetDynamoTable(
          batchId,
          id,
          allValidatedFileTableFields.parentPath,
          name,
          rowType,
          allValidatedFileTableFields.title,
          allValidatedFileTableFields.description,
          transferringBody,
          transferCompletedDatetime,
          upstreamSystem,
          digitalAssetSource,
          digitalAssetSubtype,
          originalFiles,
          originalMetadataFiles,
          allValidatedFileTableFields.ingestedPreservica.contains("true"),
          allValidatedFileTableFields.ingestedCustodialCopy.contains("true"),
          allValidatedFileTableFields.identifiers,
          childCount,
          skipIngest,
          allValidatedFileTableFields.correlationId
        )
    }.toEither
      .left
      .map(InvalidPropertiesError.apply)

  def readFileRow: Either[InvalidPropertiesError, FileDynamoTable] =
    (
      allValidatedFileTableFields.batchId,
      allValidatedFileTableFields.id,
      allValidatedFileTableFields.name,
      allValidatedFileTableFields.sortOrder,
      allValidatedFileTableFields.fileSize,
      allValidatedFileTableFields.checksums,
      allValidatedFileTableFields.fileExtension,
      allValidatedFileTableFields.`type`,
      allValidatedFileTableFields.representationType,
      allValidatedFileTableFields.representationSuffix,
      allValidatedFileTableFields.childCount,
      allValidatedFileTableFields.location
    ).mapN {
      (
          batchId,
          id,
          name,
          sortOrder,
          fileSize,
          checksumList,
          fileExtension,
          rowType,
          representationType,
          representationSuffix,
          childCount,
          location
      ) =>
        FileDynamoTable(
          batchId,
          id,
          allValidatedFileTableFields.parentPath,
          name,
          rowType,
          allValidatedFileTableFields.title,
          allValidatedFileTableFields.description,
          sortOrder,
          fileSize,
          checksumList,
          fileExtension,
          representationType,
          representationSuffix,
          allValidatedFileTableFields.ingestedPreservica.contains("true"),
          allValidatedFileTableFields.ingestedCustodialCopy.contains("true"),
          allValidatedFileTableFields.identifiers,
          childCount,
          location
        )
    }.toEither
      .left
      .map(InvalidPropertiesError.apply)
}
