package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.data.*
import cats.effect.{IO, Resource}
import cats.implicits.*

import com.networknt.schema.InputFormat.JSON
import com.networknt.schema.SpecVersion.VersionFlag
import com.networknt.schema.{JsonSchemaFactory, ValidationMessage}
import ujson.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda.given
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.EntryTypeSchema.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.GeneralSchema.{ExactlyOneSeriesAndNullParent, MinimumAssetsAndFiles}

import java.io.InputStream
import scala.collection.immutable.Map
import scala.jdk.CollectionConverters.SetHasAsScala

class MetadataJsonSchemaValidator(validationType: EntryTypeSchema) {
  private val schemaLocation = schemaLocations(validationType)
  private val resource = getResource(schemaLocation)

  def validateMetadataJsonObject(jsonObject: Obj): IO[Map[String, ValidatedNel[SchemaValidationError, Value]]] = {
    val jsonObjectAsString = convertUjsonObjectToString(jsonObject)

    for (errors <- resource.use(schemaInputStream => validateAgainstSchema(jsonObjectAsString, schemaInputStream)))
      yield {
        val originalJsonMap = convertUjsonObjectToMap(jsonObject)
        val jsonObjectValidated =
          errors.foldLeft(originalJsonMap) { case (jsonMap, error) =>
            val missingProperty = Option(error.getProperty)
            val errorMessage = error.getMessage
            val validationError: SchemaValidationError =
              missingProperty match {
                case None =>
                  val property = error.getInstanceLocation.toString.split('.').last
                  val valueThatCausedError = error.getInstanceNode.asText()
                  ValueError(property, valueThatCausedError, errorMessage)
                case Some(property) => MissingPropertyError(property, errorMessage)
              }
            val property = validationError.propertyWithError
            val currentPropertyErrorsInNel = jsonMap.getOrElse(property, Validated.Valid(Obj()))
            val newPropertyErrorInNel = validationError.invalidNel[Value]
            val allPropertyErrorsInNel = currentPropertyErrorsInNel.combine(newPropertyErrorInNel)
            val allUniquePropertyErrorsInNel = allPropertyErrorsInNel.leftMap(errors => NonEmptyList.fromListUnsafe(errors.toList.distinct))

            jsonMap + (property -> allUniquePropertyErrorsInNel)
          }
        sortJsonObjectByFieldName(jsonObjectValidated)
      }
  }

  private def convertUjsonObjectToMap(jsonObject: Obj): Map[String, ValidatedNel[SchemaValidationError, Value]] = {
    val objectAsMap = jsonObject.obj.toMap
    objectAsMap.map { case (property, value) =>
      property -> Validated.Valid[Value](value).toValidatedNel[SchemaValidationError, Value]
    }
  }

  private def convertUjsonObjectToString(jsonObject: Obj): String = write(jsonObject)
}

object MetadataJsonSchemaValidator:
  private val schemaLocationFolder = "/metadata-validation-schemas"
  private val schemaLocations: Map[EntryTypeSchema | GeneralSchema, String] = Map(
    ExactlyOneSeriesAndNullParent -> s"$schemaLocationFolder/exactly-one-entry-with-series-and-null-parentid-validation-schema.json",
    MinimumAssetsAndFiles -> s"$schemaLocationFolder/at-least-one-asset-and-file-validation-schema.json",
    ArchiveFolder -> s"$schemaLocationFolder/archive-and-content-folder-validation-schema.json",
    ContentFolder -> s"$schemaLocationFolder/archive-and-content-folder-validation-schema.json",
    Asset -> s"$schemaLocationFolder/asset-validation-schema.json",
    File -> s"$schemaLocationFolder/file-validation-schema.json"
  )
  def apply(validationType: EntryTypeSchema) = new MetadataJsonSchemaValidator(validationType: EntryTypeSchema)

  def checkJsonForExactlyOneSeriesAndNullParent(metadataJson: String): IO[List[ValidatedNel[ExactlyOneSeriesAndNullParentError, String]]] = {
    val schemaLocation = schemaLocations(ExactlyOneSeriesAndNullParent)
    for (errors <- getResource(schemaLocation).use(schemaInputStream => validateAgainstSchema(metadataJson, schemaInputStream)))
      yield errors.map(error => ExactlyOneSeriesAndNullParentError(error.getMessage).invalidNel[String])
  }

  def checkJsonForMinimumObjects(metadataJson: String): IO[List[ValidatedNel[MinimumAssetsAndFilesError, String]]] = {
    val schemaLocation = schemaLocations(MinimumAssetsAndFiles)
    for (errors <- getResource(schemaLocation).use(schemaInputStream => validateAgainstSchema(metadataJson, schemaInputStream)))
      yield errors.map(error => MinimumAssetsAndFilesError(error.getMessage).invalidNel[String])
  }

  def sortJsonObjectByFieldName(jsonObjectsAsMap: Map[String, ValidatedNel[SchemaValidationError, Value]]): Map[String, Validated[NonEmptyList[SchemaValidationError], Value]] = {
    val sortedValidatedJsonObjects = jsonObjectsAsMap.toSeq.sortBy(_._1)
    sortedValidatedJsonObjects.map { case (field, value) => (field, value.leftMap(_.sortBy(_.errorMessage))) }.toMap
  }

  private def getResource(schemaLocation: String) = for {
    is <- Resource.make(IO(getClass.getResourceAsStream(schemaLocation)))(is => IO(is.close()))
  } yield is

  private def validateAgainstSchema(jsonToValidate: String, schemaInputStream: InputStream): IO[List[ValidationMessage]] = IO {
    val jsonFactory = JsonSchemaFactory.getInstance(VersionFlag.V202012)
    val schema = jsonFactory.getSchema(schemaInputStream)
    schema.validate(jsonToValidate, JSON).asScala.toSet.toList
  }

  sealed trait SchemaValidationError extends ValidationError:
    val propertyWithError: String

  case class ExactlyOneSeriesAndNullParentError(errorMessage: String) extends ValidationError
  
  case class MinimumAssetsAndFilesError(errorMessage: String) extends ValidationError

  case class MissingPropertyError(propertyWithError: String, errorMessage: String) extends SchemaValidationError

  case class ValueError(propertyWithError: String, valueThatCausedError: String, errorMessage: String) extends SchemaValidationError

  enum EntryTypeSchema:
    case File, Asset, ArchiveFolder, ContentFolder

  enum GeneralSchema:
    case ExactlyOneSeriesAndNullParent, MinimumAssetsAndFiles
