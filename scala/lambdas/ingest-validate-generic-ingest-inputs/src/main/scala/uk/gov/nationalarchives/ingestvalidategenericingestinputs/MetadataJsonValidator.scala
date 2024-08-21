package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonValidator.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonValidator.SchemaType.*
import cats.effect.{IO, Resource}
import cats.data.*
import cats.implicits.*
import com.networknt.schema.SpecVersion.VersionFlag
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.InputFormat.JSON
import com.networknt.schema.ValidationMessage

import scala.jdk.CollectionConverters.SetHasAsScala
import ujson.{Obj, Value, write}

import java.io.InputStream
import scala.collection.immutable.Map

class MetadataJsonValidator(validationType: SchemaType) {
  private val schemaLocationFolder = "metadata-validation-schemas"
  private val schemaLocations = Map(
    MinimumAssetsAndFiles -> s"$schemaLocationFolder/at-least-one-asset-and-file-validation-schema.json",
    ArchiveFolder -> s"$schemaLocationFolder/archive-and-content-folder-validation-schema.json",
    ContentFolder -> s"$schemaLocationFolder/archive-and-content-folder-validation-schema.json",
    Asset -> s"$schemaLocationFolder/asset-validation-schema.json",
    File -> s"$schemaLocationFolder/file-validation-schema.json"
  )
  private val schemaLocation = schemaLocations(validationType)

  private val resource = for {
    is <- Resource.make(IO(getClass.getResourceAsStream(schemaLocation)))(is => IO(is.close()))
  } yield is

  def checkJsonForMinimumObjects(metadataJson: String): IO[List[ValidatedNel[MinimumAssetsAndFilesError, String]]] =
    for (errors <- resource.use(schemaInputStream => validateAgainstSchema(jsonObjectAsString, schemaInputStream)))
      yield errors.map(error => MinimumAssetsAndFilesError(error.getMessage).invalidNel[String])

  def validateMetadataJsonObject(jsonObject: Obj): IO[Map[String, ValidatedNel[(String, ValidationError), Obj]]] = {
    val jsonObjectAsString = convertUjsonObjectToString(jsonObject)

    for (errors <- resource.use(schemaInputStream => validateAgainstSchema(jsonObjectAsString, schemaInputStream)))
      yield {
        val originalJsonMap = convertUjsonObjectToMap(jsonObject)
        errors.foldLeft(originalJsonMap) { case (jsonMap, error) =>
          val missingProperty = Option(error.getProperty).map(_.toString)
          val errorMessage = error.getMessage.toString
          val validationError: SchemaValidationError =
            missingProperty match {
              case None =>
                val property = error.getInstanceLocation.toString.split('.').last
                val valueThatCausedError = error.getInstanceNode.toString
                ValueError(property, valueThatCausedError, errorMessage)
              case Some(property) => MissingPropertyError(property, errorMessage)
            }
          val property = validationError.propertyWithError

          jsonMap + (property -> (property, validationError).invalidNel[Obj])
        }
      }
  }

  private def validateAgainstSchema(jsonToValidate: String, schemaInputStream: InputStream): IO[List[ValidationMessage]] = IO {
    val jsonFactory = JsonSchemaFactory.getInstance(VersionFlag.V202012)
    val schema = jsonFactory.getSchema(schemaInputStream)
    schema.validate(jsonToValidate, JSON).asScala.toSet.toList
  }

  def convertUjsonObjectToMap(jsonObject: Obj): Map[String, ValidatedNel[InvalidJsonEntry, Obj]] = {
    val objectAsMap = jsonObject.obj.toMap
    objectAsMap.map { case (property, value) =>
      property -> Validated.Valid[Obj](value.obj).toValidatedNel[InvalidJsonEntry, Obj]
    }
  }

  def convertUjsonObjectToString(jsonObject: Obj): String = write(jsonObject)

  // val input = """[{"series":"A 167","id_Code":"hello","id_URI":"zzz","id":"h","parentId":null,"title":null,"type":"ContentFolder","name":"zzz"},{"originalFiles":["00bc4004-a0dd-426f-abaa-6b846391fd8f"],"originalMetadataFiles":["640f48e1-c587-4440-8962-4a75b05038b5"],"transferringBody":"vlah","transferCompleteDatetime":"2024-08-05T13:58:41Z","upstreamSystem":"aaa","digitalAssetSource":"Born Digital","digitalAssetSubtype":"FCL","id_BornDigitalRef":"ZHPL","id_ConsignmentReference":"sfsdf","id_RecordID":"f5d6c25c-e586-4e63-a45b-9c175b095c48","id":"f5d6c25c-e586-4e63-a45b-9c175b095c48","parentId":"64ad1ab1-cb26-4ae5-b50b-c1926af688ff","title":"fhfghfgh","type":"Asset","name":"f5d6c25c-e586-4e63-a45b-9c175b095c48"}, {"id":"00bc4004-a0dd-426f-abaa-6b846391fd8f","parentId":"f5d6c25c-e586-4e63-a45b-9c175b095c48","title":"ttt","type":"File","name":"xcvxcvx","sortOrder":1,"fileSize":15613,"representationType":"Preservation","representationSuffix":1,"location":"s3://ghfgh/00bc4004-a0dd-426f-abaa-6b846391fd8f","checksum_sha256":"111"}, {"id":"00bc4004-a0dd-426f-abaa-6b846391fd8f","parentId":"f5d6c25c-e586-4e63-a45b-9c175b095c48","title":"ttt","type":"File","name":"xcvxcvx","sortOrder":1,"fileSize":15613,"representationType":"Preservation","representationSuffix":1,"location":"s3://ghfgh/00bc4004-a0dd-426f-abaa-6b846391fd8f","checksum_sha256":"111"}, {"id":"640f48e1-c587-4440-8962-4a75b05038b5","parentId":"f5d6c25c-e586-4e63-a45b-9c175b095c48","title":"hjkhjk","type":"File","name":"jjj","sortOrder":2,"fileSize":1159,"representationType":"Preservation","representationSuffix":1,"location":"s3://ghfgh/640f48e1-c587-4440-8962-4a75b05038b5"}]""".stripMargin

}

object MetadataJsonValidator:
  type InvalidJsonEntry = (String, ValidationError)
  sealed trait ValidationError:
    val errorMessage: String

  sealed trait SchemaValidationError extends ValidationError:
    val propertyWithError: String

  case class MinimumAssetsAndFilesError(errorMessage: String) extends ValidationError
  case class MissingPropertyError(propertyWithError: String, errorMessage: String) extends SchemaValidationError
  case class ValueError(propertyWithError: String, valueThatCausedError: String, errorMessage: String) extends SchemaValidationError
  case class MissingFileExtensionError(errorMessage: String) extends ValidationError
  case class NoFileAtS3LocationError(errorMessage: String) extends ValidationError
  case class IdIsNotAUuidError(errorMessage: String) extends ValidationError
  case class IdIsNotUniqueError(errorMessage: String) extends ValidationError
  case class HierarchyLinkingError(errorMessage: String) extends ValidationError

  enum SchemaType:
    case MinimumAssetsAndFiles, File, Asset, ArchiveFolder, ContentFolder
