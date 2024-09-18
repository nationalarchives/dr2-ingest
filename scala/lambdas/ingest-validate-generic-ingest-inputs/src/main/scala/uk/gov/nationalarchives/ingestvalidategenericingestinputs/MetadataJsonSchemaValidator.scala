package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.data.*
import cats.effect.{IO, Resource}
import cats.implicits.*

import com.networknt.schema.InputFormat.JSON
import com.networknt.schema.SpecVersion.VersionFlag
import com.networknt.schema.{JsonSchemaFactory, ValidationMessage}
import ujson.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.ValidatedUtils.given
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.EntryValidationError.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.EntryTypeSchema.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.GeneralSchema.{AtLeastOneEntryWithSeriesAndNullParent, MinimumAssetsAndFiles}

import java.io.InputStream
import scala.collection.immutable.Map
import scala.jdk.CollectionConverters.SetHasAsScala

class MetadataJsonSchemaValidator(validationType: EntryTypeSchema) {
  private val schemaLocation = schemaLocations(validationType)
  private val resource = getResource(schemaLocation)

  def validateMetadataJsonObject(jsonObject: Obj): IO[Map[String, ValidatedNel[ValidationError, Value]]] = {
    val jsonObjectAsString = write(jsonObject)

    for (errors <- resource.use(schemaInputStream => validateAgainstSchema(jsonObjectAsString, schemaInputStream)))
      yield {
        val originalJsonMap = jsonObject.obj.view.mapValues(_.validNel[ValidationError]).toMap
        val jsonObjectValidated =
          errors.foldLeft(originalJsonMap) { case (jsonMap, error) =>
            val missingProperty = Option(error.getProperty)
            val errorMessage = error.getMessage
            val (property, validationError: ValidationError) =
              missingProperty match {
                case None =>
                  val property = error.getInstanceLocation.toString.split('.').last
                  val valueThatCausedError = error.getInstanceNode.asText()
                  (property, SchemaValueError(valueThatCausedError, errorMessage))
                case Some(property) => (property, MissingPropertyError(property, errorMessage))
              }
            val currentPropertyErrorsInNel = jsonMap.getOrElse(property, Validated.Valid(Obj()))
            val newPropertyErrorInNel = validationError.invalidNel[Value]
            val allPropertyErrorsInNel = currentPropertyErrorsInNel.combine(newPropertyErrorInNel)
            val allUniquePropertyErrorsInNel = allPropertyErrorsInNel.leftMap(errors => NonEmptyList.fromListUnsafe(errors.toList.distinct))

            jsonMap + (property -> allUniquePropertyErrorsInNel)
          }
        sortJsonObjectByFieldName(jsonObjectValidated)
      }
  }
}

object MetadataJsonSchemaValidator:
  private val schemaLocationFolder = "/metadata-validation-schemas"
  private val schemaLocations: Map[EntryTypeSchema | GeneralSchema, String] = Map(
    AtLeastOneEntryWithSeriesAndNullParent -> s"$schemaLocationFolder/at-least-one-entry-with-series-and-null-parentid-validation-schema.json",
    MinimumAssetsAndFiles -> s"$schemaLocationFolder/at-least-one-asset-and-file-validation-schema.json",
    ArchiveFolder -> s"$schemaLocationFolder/archive-and-content-folder-validation-schema.json",
    ContentFolder -> s"$schemaLocationFolder/archive-and-content-folder-validation-schema.json",
    Asset -> s"$schemaLocationFolder/asset-validation-schema.json",
    File -> s"$schemaLocationFolder/file-validation-schema.json",
    UnknownType -> s"$schemaLocationFolder/unknown-entry-type-validation-schema.json"
  )
  def apply(validationType: EntryTypeSchema) = new MetadataJsonSchemaValidator(validationType: EntryTypeSchema)

  def checkJsonForAtLeastOneEntryWithSeriesAndNullParent(metadataJson: String): IO[List[ValidatedNel[AtLeastOneEntryWithSeriesAndNullParentError, String]]] = {
    val schemaLocation = schemaLocations(AtLeastOneEntryWithSeriesAndNullParent)
    for (errors <- getResource(schemaLocation).use(schemaInputStream => validateAgainstSchema(metadataJson, schemaInputStream)))
      yield errors.map(error => AtLeastOneEntryWithSeriesAndNullParentError(error.getMessage).invalidNel[String])
  }

  def checkJsonForMinimumObjects(metadataJson: String): IO[List[ValidatedNel[MinimumAssetsAndFilesError, String]]] = {
    val schemaLocation = schemaLocations(MinimumAssetsAndFiles)
    for (errors <- getResource(schemaLocation).use(schemaInputStream => validateAgainstSchema(metadataJson, schemaInputStream)))
      yield errors.map(error => MinimumAssetsAndFilesError(error.getMessage).invalidNel[String])
  }

  def sortJsonObjectByFieldName(
      jsonObjectsAsMap: Map[String, ValidatedNel[ValidationError, Value]]
  ): Map[String, Validated[NonEmptyList[ValidationError], Value]] = {
    val sortedValidatedJsonObjects = jsonObjectsAsMap.toSeq.sortBy(_._1)
    sortedValidatedJsonObjects.map { case (field, value) => (field, value.leftMap(_.sortBy(_.errorMessage))) }.toMap
  }

  private def getResource(schemaLocation: String) = Resource.make(IO(getClass.getResourceAsStream(schemaLocation)))(is => IO(is.close()))

  private def validateAgainstSchema(jsonToValidate: String, schemaInputStream: InputStream): IO[List[ValidationMessage]] = IO {
    val jsonFactory = JsonSchemaFactory.getInstance(VersionFlag.V202012)
    val schema = jsonFactory.getSchema(schemaInputStream)
    schema.validate(jsonToValidate, JSON).asScala.toSet.toList
  }

  enum EntryTypeSchema:
    case UnknownType, File, Asset, ArchiveFolder, ContentFolder

  enum GeneralSchema:
    case AtLeastOneEntryWithSeriesAndNullParent, MinimumAssetsAndFiles
