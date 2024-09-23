package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.effect.IO
import cats.implicits.*
import fs2.interop.reactivestreams.*
import fs2.{Chunk, Stream, text}
import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.reactivestreams.FlowAdapters
import org.scanamo.generic.semiauto.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import ujson.*
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.EntryValidationError.{MissingPropertyError, SchemaValidationError, ValidationError, ValueError}
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.EntryTypeSchema.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.ValidatedUtils.ValidatedEntry
import uk.gov.nationalarchives.utils.LambdaRunner

import java.net.URI
import java.nio.ByteBuffer

class Lambda extends LambdaRunner[Input, StateOutput, Config, Dependencies] {
  lazy private val bufferSize = 1024 * 5

  given Encoder[Value] = value => Json.fromString(value.toString)

  given Encoder[Map[String, ValidValidationResult | InvalidValidationResult]] =
    entry =>
      entry.map { case (fieldName, value) =>
        value match {
          case valid: ValidValidationResult => (fieldName, Json.obj("Valid" -> Json.fromString(valid.result)))
          case invalid: InvalidValidationResult =>
            fieldName -> Json.obj {
              "Invalid" ->
                invalid.result.map {
                  case valueError: ValueError =>
                    Json.obj(
                      ("errorType", Json.fromString(valueError.getClass.getSimpleName)),
                      ("valueThatCausedError", Json.fromString(valueError.valueThatCausedError)),
                      ("errorMessage", Json.fromString(valueError.errorMessage))
                    )
                  case missingPropertyError: MissingPropertyError =>
                    Json.obj(
                      ("errorType", Json.fromString(missingPropertyError.getClass.getSimpleName)),
                      ("propertyWithError", Json.fromString(missingPropertyError.propertyWithError)),
                      ("errorMessage", Json.fromString(missingPropertyError.errorMessage))
                    )
                  case validationError: ValidationError =>
                    Json.obj(
                      ("errorType", Json.fromString(validationError.getClass.getSimpleName)),
                      ("errorMessage", Json.fromString(validationError.errorMessage))
                    )
                }.asJson
            }
        }
      }.asJson

  override def dependencies(config: Config): IO[Dependencies] =
    IO(Dependencies(DAS3Client[IO]()))

  override def handler: (
      Input,
      Config,
      Dependencies
  ) => IO[StateOutput] = (input, config, dependencies) =>
    for {
      log <- IO(log(Map("batchRef" -> input.batchId)))
      _ <- log(s"Processing batchRef ${input.batchId}")
      s3Client = dependencies.s3
      bucket = input.metadataPackage.getHost
      key = input.metadataPackage.getPath.drop(1)
      metadataJson <- parseFileFromS3(s3Client, bucket, key)
      _ <- log("Retrieving metadata.json from s3 bucket")

      minimumAssetsAndFilesErrors <- MetadataJsonSchemaValidator.checkJsonForMinimumObjects(metadataJson)
      _ <- log("Checking that JSON has at least one Asset and one File")

      atLeastOneEntryWithSeriesAndNullParentErrors <- MetadataJsonSchemaValidator.checkJsonForAtLeastOneEntryWithSeriesAndNullParent(metadataJson)
      _ <- log("Checking that JSON has at least one entry that has both a series and a null parent")

      validatedEntries <- validateAgainstSchema(metadataJson) // all entries validated except for where the 'type' could not be found
      _ <- log("Checking that each entry type in JSON matches a schema")

      valueValidator = new MetadataJsonValueValidator
      fileEntries = validatedEntries("File")
      fileEntriesWithValidatedLocation <- valueValidator.checkFileIsInCorrectS3Location(s3Client, fileEntries)

      idsOfMetadataFiles = getIdsOfMetadataFiles(validatedEntries("Asset"))
      (metadataEntries, nonMetadataEntries) =
        separateMetadataFilesFromNonMetadataFiles(fileEntriesWithValidatedLocation, idsOfMetadataFiles)
      metadataEntriesWithValidatedExtensions = valueValidator.checkFileNamesHaveExtensions(metadataEntries)

      fileEntriesWithValidatedExtensions = metadataEntriesWithValidatedExtensions ::: nonMetadataEntries

      updatedEntries = validatedEntries ++ Map("File" -> fileEntriesWithValidatedExtensions)
      allEntryIds = valueValidator.getIdsOfAllEntries(updatedEntries, idsOfMetadataFiles)
      entriesWithValidatedUniqueIds = valueValidator.checkIfAllIdsAreUnique(updatedEntries, allEntryIds)
      entriesWithValidatedUuids = valueValidator.checkIfAllIdsAreUuids(entriesWithValidatedUniqueIds)

      entryTypesGrouped = allEntryIds.groupBy { case (_, entryType) => entryType }
      entriesWithValidatedParentIds = valueValidator.checkIfEntriesHaveCorrectParentIds(entriesWithValidatedUuids, allEntryIds.toMap, entryTypesGrouped)
      entriesCompletelyValidated = valueValidator.checkForCircularDependenciesInFolders(entriesWithValidatedParentIds)

      jsonStructuralErrors = {
        toListOfInvalidValidationResult(minimumAssetsAndFilesErrors) :::
          toListOfInvalidValidationResult(atLeastOneEntryWithSeriesAndNullParentErrors)
      }.toMap

      entriesThatFailedValidation = entriesCompletelyValidated.flatMap { case (_, entries) =>
        entries.collect {
          case entry if entry.exists { case (_, value) => value.isInvalid } =>
            entry.map { case (entryName, value) =>
              val transformedValue: ValidValidationResult | InvalidValidationResult =
                value match {
                  case Validated.Valid(value)                  => ValidValidationResult(value.toString)
                  case Validated.Invalid(nonEmptyListOfErrors) => InvalidValidationResult(nonEmptyListOfErrors.toList)
                }
              entryName -> transformedValue
            }
        }
      }.toList
      numOfJsonStructureErrors = jsonStructuralErrors.size
      numOfEntriesWithFailures = entriesThatFailedValidation.length

      _ <- IO.whenA((numOfJsonStructureErrors + numOfEntriesWithFailures) > 0) {
        lazy val keyWithoutOriginalFileName = key.split('/').dropRight(1).mkString("/")
        lazy val keyOfResultsFile = s"$keyWithoutOriginalFileName/metadata-entries-with-errors.json"
        lazy val fullLocationOfResultsFile = s"$bucket/$keyOfResultsFile"

        val jsonStructureErrorMessage =
          if numOfJsonStructureErrors > 0 then
            val s = if numOfJsonStructureErrors == 1 then "" else "s"
            s"$numOfJsonStructureErrors thing$s wrong with the structure of the metadata.json for batchId '${input.batchId}'; the results can be found here: $fullLocationOfResultsFile\n\n"
          else ""

        val entriesErrorMessage =
          if numOfEntriesWithFailures > 0 then
            val (entryOrEntries, hasOrHave) = if numOfEntriesWithFailures == 1 then ("entry", "has") else ("entries", "have")
            s"$numOfEntriesWithFailures $entryOrEntries (objects) in the metadata.json for batchId '${input.batchId}' $hasOrHave failed validation; the results can be found here: $fullLocationOfResultsFile"
          else ""

        val allJsonErrors = entriesThatFailedValidation.+:(jsonStructuralErrors)
        logErrorMessages(jsonStructureErrorMessage, entriesErrorMessage, log, entriesThatFailedValidation) >>
          uploadResults(s3Client, bucket, keyOfResultsFile, allJsonErrors) >>
          IO.raiseError(JsonValidationException(jsonStructureErrorMessage + entriesErrorMessage))
      }
    } yield StateOutput(input.batchId, input.metadataPackage)

  private def validateAgainstSchema(metadataJson: String) =
    for {
      metadataJsonAsUjson <- IO(read(metadataJson).arr.toList)
      entriesGroupedByType = metadataJsonAsUjson.groupBy {
        case value if value.obj.contains("type") && List("ArchiveFolder", "ContentFolder", "Asset", "File").contains(value("type").str) => value("type").str
        case value                                                                                                                      => "UnknownType"
      }
      validateJsonObjects = validateJsonPerType(entriesGroupedByType)
      fileEntries <- validateJsonObjects(File)
      assetEntries <- validateJsonObjects(Asset)
      archivedFolderEntries <- validateJsonObjects(ArchiveFolder)
      contentFolderEntries <- validateJsonObjects(ContentFolder)
      unknownTypeEntries <- validateJsonObjects(UnknownType)

      allValidatedEntries = Map(
        "File" -> fileEntries,
        "Asset" -> assetEntries,
        "ArchiveFolder" -> archivedFolderEntries,
        "ContentFolder" -> contentFolderEntries,
        "UnknownType" -> unknownTypeEntries
      )
    } yield allValidatedEntries

  private def validateJsonPerType(
      entriesGroupedByType: Map[String, List[Value]]
  )(entryType: EntryTypeSchema): IO[List[Map[FieldName, ValidatedNel[ValidationError, Value]]]] = {
    val entryObjectValidator = MetadataJsonSchemaValidator(entryType)
    val entriesOfSpecifiedType = entriesGroupedByType.getOrElse(entryType.toString, Nil)
    entriesOfSpecifiedType.parTraverse { entryOfSpecifiedType => entryObjectValidator.validateMetadataJsonObject(entryOfSpecifiedType.obj) }
  }

  private def parseFileFromS3(s3: DAS3Client[IO], bucket: String, key: String): IO[String] =
    for {
      pub <- s3.download(bucket, key)
      s3FileString <- pub
        .toStreamBuffered[IO](bufferSize)
        .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
        .through(text.utf8.decode)
        .compile
        .string
    } yield s3FileString

  private def toListOfInvalidValidationResult(listOfErrors: List[ValidatedNel[SchemaValidationError, String]]) =
    listOfErrors.zipWithIndex.map { (error, errorNum) =>
      val nelOfErrors = error.swap.toOption.get
      val errorName = nelOfErrors.head.getClass.getSimpleName
      s"$errorName $errorNum:" -> InvalidValidationResult(nelOfErrors.toList)
    }

  private def getIdsOfMetadataFiles(assets: List[ValidatedEntry]) = assets.flatMap { assetEntry =>
    assetEntry.collect {
      case (fieldName, validatedValue) if fieldName == "originalMetadataFiles" =>
        val listOfValues = validatedValue.getOrElse(Arr()).arr.toList
        listOfValues.map(_.str)
    }.flatten
  }

  private def separateMetadataFilesFromNonMetadataFiles(fileEntries: List[ValidatedEntry], idsOfMetadataFiles: List[String]) =
    fileEntries.partition { fileEntry =>
      val fileEntryId = fileEntry(id).getOrElse(Str("idFieldHasErrorInIt")).str
      idsOfMetadataFiles.contains(fileEntryId)
    }

  private def logErrorMessages(
      jsonStructureErrorMessage: String,
      entriesErrorMessage: String,
      log: String => IO[Unit],
      entriesThatFailedValidation: List[Map[String, ValidValidationResult | InvalidValidationResult]]
  ) =
    for {
      _ <- if jsonStructureErrorMessage.isEmpty then IO.unit else log(jsonStructureErrorMessage)
      _ <-
        if entriesErrorMessage.isEmpty then IO.unit
        else
          log(entriesErrorMessage) >> entriesThatFailedValidation.parTraverse { entry =>
            val entryId = entry("id") match {
              case ValidValidationResult(id) => id
              case InvalidValidationResult(listOfErrors) =>
                val potentialIdValues = listOfErrors.collect { case valueError: ValueError => valueError.valueThatCausedError }
                potentialIdValues.headOption.getOrElse("idUnknown")
            }

            val fieldNamesWithErrors = entry.collect { case (name, value) if value.getClass.getSimpleName == "InvalidValidationResult" => name }.mkString(", ")
            val mapToSendAsJson = Map("id" -> entryId, "fieldNamesWithErrors" -> fieldNamesWithErrors)

            log(mapToSendAsJson.asJson.noSpaces)
          }
    } yield ()

  private def uploadResults(s3Client: DAS3Client[IO], bucket: String, keyOfResultsFile: String, allJsonErrors: List[Map[String, ValidValidationResult | InvalidValidationResult]]) =
    Stream
      .eval(IO.pure(allJsonErrors.asJson.noSpaces))
      .map(s => ByteBuffer.wrap(s.getBytes()))
      .toPublisherResource
      .use(pub => s3Client.upload(bucket, keyOfResultsFile, FlowAdapters.toPublisher(pub)))
      .void
}

object Lambda {
  sealed trait ValidationResult:
    val result: String | List[ValidationError | MissingPropertyError | ValueError]

  case class StateOutput(batchId: String, metadataPackage: URI)

  case class Input(batchId: String, metadataPackage: URI)

  case class Config() derives ConfigReader

  case class Dependencies(s3: DAS3Client[IO])

  case class ValidValidationResult(result: String) extends ValidationResult
  case class InvalidValidationResult(result: List[ValidationError | MissingPropertyError | ValueError]) extends ValidationResult

  case class JsonValidationException(message: String) extends Exception(message)

}
