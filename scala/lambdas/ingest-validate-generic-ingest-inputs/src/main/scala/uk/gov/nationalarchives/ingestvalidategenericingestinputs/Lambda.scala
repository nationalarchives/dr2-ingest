package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.data.{Validated, ValidatedNel}
import cats.effect.IO
import cats.implicits.*
import cats.kernel.Semigroup
import fs2.interop.reactivestreams.*
import fs2.{Chunk, Stream, text}
import io.circe.*
import io.circe.generic.auto.*
import org.scanamo.*
import org.scanamo.generic.semiauto.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import ujson.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.EntryTypeSchema.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonValueValidator
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.{DAS3Client, DASFNClient}

import java.net.URI
import java.util.UUID
import scala.util.Try

class Lambda extends LambdaRunner[Input, Unit, Config, Dependencies] {
  lazy private val bufferSize = 1024 * 5

  override def dependencies(config: Config): IO[Dependencies] =
    IO(Dependencies(DAS3Client[IO](), DASFNClient[IO]()))

  override def handler: (
      Input,
      Config,
      Dependencies
  ) => IO[Unit] = (input, config, dependencies) =>
    for {
      log <- IO(log(Map("batchRef" -> input.batchId)))
      _ <- log(s"Processing batchRef ${input.batchId}")
      s3Client = dependencies.s3
      bucket = input.metadataPackage.getHost
      key = input.metadataPackage.getPath.drop(1)

      metadataJson <- parseFileFromS3(s3Client, input)
      _ <- log("Retrieving metadata.json from s3 bucket")

      minimumAssetsAndFilesErrors <- MetadataJsonSchemaValidator.checkJsonForMinimumObjects(metadataJson)

      errorsPreventingIngest <- // start step function OR log error
        if minimumAssetsAndFilesErrors.isEmpty then
          for {
            result <- d(metadataJson, s3Client)
            _ <- dependencies.sfn.startExecution(config.sfnArn, Output(input.batchId, input.metadataPackage), Option(input.batchId))
          } yield ()
        else IO.pure(minimumAssetsAndFilesErrors)
// check batchId exists
    } yield ()

  private def d(metadataJson: String, s3Client: DAS3Client[IO]) =
    for {
      metadataJsonAsUjson <- IO(read(metadataJson).arr.toList)
      entriesGroupedByType = metadataJsonAsUjson.foldLeft(Map[String, List[Value]]()) { (typesGrouped, entry) =>
        val entryType = Try(entry(typeField).str).getOrElse("UnknownType")
        val entriesBelongingToType = typesGrouped.getOrElse(entryType, Nil)
        typesGrouped + (entryType -> (entry :: entriesBelongingToType))
      }
      validateJsonObjects = validateJsonPerType(entriesGroupedByType)
      fileEntries <- validateJsonObjects(File)
      assetEntries <- validateJsonObjects(Asset)
      archivedFolderEntries <- validateJsonObjects(ArchiveFolder)
      contentFolderEntries <- validateJsonObjects(ContentFolder)

      valueValidator = new MetadataJsonValueValidator
      validatedLocations <- valueValidator.checkFileIsInCorrectS3Location(s3Client, fileEntries)
      fileEntriesWithValidatedLocation = fileEntries.zip(validatedLocations).map { case (entries, locationNel) =>
        entries + (location -> locationNel)
      }

      fileEntriesWithValidatedFileExtensions = valueValidator.checkFileNamesHaveExtensions(fileEntriesWithValidatedLocation)

      allEntries = Map(
        "File" -> fileEntriesWithValidatedFileExtensions,
        "Asset" -> assetEntries,
        "ArchiveFolder" -> archivedFolderEntries,
        "ContentFolder" -> contentFolderEntries
      )

      allEntryIds = valueValidator.getIdsOfAllEntries(allEntries)
      entriesWithValidatedUniqueIds = valueValidator.checkIfAllIdsAreUnique(allEntries, allEntryIds)
      entriesWithValidatedUuids = valueValidator.checkIfAllIdsAreUuids(entriesWithValidatedUniqueIds)
      entryTypesGrouped = allEntryIds.groupBy { case (_, entryType) => entryType }
      entriesWithValidatedParentIds = valueValidator.checkIfEntriesHaveCorrectParentIds(allEntries, allEntryIds.toMap, entryTypesGrouped)
    } yield entriesWithValidatedParentIds

  private def validateJsonPerType(entriesGroupedByType: Map[String, List[Value]])(entryType: EntryTypeSchema): IO[List[Map[FieldName, ValidatedNel[ValidationError, Value]]]] = {
    val entryObjectValidator = MetadataJsonSchemaValidator(entryType)
    val entriesOfSpecifiedType = entriesGroupedByType.getOrElse(entryType.toString, Nil)
    entriesOfSpecifiedType.map { entryOfSpecifiedType => entryObjectValidator.validateMetadataJsonObject(entryOfSpecifiedType.obj) }.sequence
  }

  private def parseFileFromS3(s3: DAS3Client[IO], input: Input): IO[String] =
    for {
      pub <- s3.download(input.metadataPackage.getHost, input.metadataPackage.getPath.drop(1))
      s3FileString <- pub
        .toStreamBuffered[IO](bufferSize)
        .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
        .through(text.utf8.decode)
        .compile
        .string
    } yield s3FileString
}
object Lambda {
  // Not expecting this 'given' to be used since we are only combining errors, but Validate's 'combine' method needs it
  given combineValues: Semigroup[Value] =
    new Semigroup[Value] {
      override def combine(val1: Value, val2: Value): Value = val1
    }
  type Entry = Map[FieldName, ValidatedNel[ValidationError, Value]]
  trait ValidationError:
    val errorMessage: String

  sealed trait EntryTypeAndParent:
    val potentialParentId: Option[String]

  case class FileEntry(potentialParentId: Option[String]) extends EntryTypeAndParent
  case class MetadataFileEntry(potentialParentId: Option[String]) extends EntryTypeAndParent
  case class UnknownFileTypeEntry(potentialParentId: Option[String]) extends EntryTypeAndParent
  case class AssetEntry(potentialParentId: Option[String]) extends EntryTypeAndParent
  case class ArchiveFolderEntry(potentialParentId: Option[String]) extends EntryTypeAndParent
  case class ContentFolderEntry(potentialParentId: Option[String]) extends EntryTypeAndParent

  case class StateOutput(batchId: String, metadataPackage: URI, archiveHierarchyFolders: List[UUID], contentFolders: List[UUID], contentAssets: List[UUID])
  case class Input(batchId: String, metadataPackage: URI)
  case class Output(batchId: String, metadataPackage: URI)
  case class Config(sfnArn: String) derives ConfigReader
  case class Dependencies(s3: DAS3Client[IO], sfn: DASFNClient[IO])
}
