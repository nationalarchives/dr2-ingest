package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.data.{Validated, ValidatedNel}
import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.global
import cats.implicits.*
import com.networknt.schema.SpecVersion.VersionFlag
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.InputFormat.JSON
import fs2.interop.reactivestreams.*
import fs2.{Chunk, Pipe, Stream, text}
import org.scanamo.*
import org.scanamo.generic.semiauto.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import ujson.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonValidator.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonValidator.SchemaType.*
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*

import java.net.URI
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.io.circe.*
import scala.io.circe.generic.auto.*
import scala.util.{Failure, Success, Try}

class Lambda extends LambdaRunner[Input, StateOutput, Config, Dependencies] {
  type UnexepectedContent = (String, ValidationError)
  lazy private val bufferSize = 1024 * 5

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

      metadataJson <- parseFileFromS3(s3Client, input)
      _ <- log("Retrieving metadata.json from s3 bucket")

      minimumAssetsAndFilesValidator = new MetadataJsonValidator(MinimumAssetsAndFiles)
      minimumAssetsAndFilesValidated <- minimumAssetsAndFilesValidator.checkJsonForMinimumObjects(metadataJson)

      q = minimumAssetsAndFilesValidated match {
        case Validated.Valid(_) =>
          for {
            metadataJsonAsUjson <- IO(read(metadataJson).arr.toList)
            entriesGroupedByType = metadataJsonAsUjson.foldLeft(Map[String, List[Value]]()) { (entryTypesGrouped, entry) =>
              val entryType = Try(entry(typeField).str).getOrElse("UnknownType")
              val entriesBelongingToType = entryTypesGrouped.getOrElse(entryType, Nil)
              entryTypesGrouped + (entryType -> entry :: entriesBelongingToType)
            }
            validateJsonObjects = validateJsonPerType(entriesGroupedByType)
            fileEntries <- validateJsonObjects(File)
            assetEntries <- validateJsonObjects(Asset)
            archivedFolderEntries <- validateJsonObjects(ArchiveFolder)
            contentFolderEntries <- validateJsonObjects(ContentFolder)

            fileEntriesWithValidatedFileExtensions <- IO(checkFileNamesHaveExtensions(fileEntries))
            validatedLocations <- checkFileIsInCorrectS3Location(s3Client, fileEntriesWithValidatedFileExtensions)
            fileEntriesWithValidatedLocation = fileEntriesWithValidatedFileExtensions.zip(validatedLocations).map { case (fileEntries, locationNel) =>
              fileEntries + (location -> locationNel)
            }

            allEntries = Map(
              "File" -> fileEntriesWithValidatedLocation,
              "Asset" -> assetEntries,
              "ArchiveFolder" -> archivedFolderEntries,
              "ContentFolder" -> contentFolderEntries
            )

            allEntryIds = getIdsOfAllEntries(allEntries)
            entriesWithValidatedUniqueIds = checkIfAllIdsAreUnique(allEntries, allEntryIds)
            entriesWithValidatedUuids = checkIfAllIdsAreUuids(entriesWithValidatedUniqueIds)
            entryTypesGrouped = allEntryIds.groupBy(_._2)
            entriesWithValidatedParentIds = checkIfEntriesHaveCorrectParentIds(allEntries, allEntryIds, entryTypesGrouped)
          } yield ()
        case invalidatedNel => invalidatedNel
      }
//      q <- mandatoryFieldsReportForFiles.map {
//        idAndFields =>
//          idAndFields._1 -> checkFileIsInCorrectS3Location()
//      }.sequence

      // a = checkIfAllIdsAreUuids(metadataJson)
      // obj <- s3Client.headObject(bucket, key)

      // _ <- dependencies.sfn.startExecution(config.sfnArn, Output(batchRef, metadataPackage), Option(batchRef))

    } yield {
      ??? // start step function OR send message to Slack and log error
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

  private def checkFileIsInCorrectS3Location(s3Client: DAS3Client[IO], fileEntries: List[Map[FieldName, ValidatedNel[InvalidJsonEntry, Obj]]]) =
    fileEntries.map { fileEntry =>
      val locationNel = fileEntry(location)
      locationNel match {
        case Validated.Valid(locationObject) =>
          val fileUri = URI.create(location)
          s3Client
            .headObject(fileUri.getHost, fileUri.getPath.drop(1))
            .redeem(
              exception => (location -> NoFileAtS3LocationError(exception.getMessage)).invalidNel[Obj],
              headResponse =>
                val statusCode = headResponse.sdkHttpResponse().statusCode()
                if statusCode == 200 then Validated.Valid[Obj](locationObject).toValidatedNel[UnexepectedContent, Obj]
                else (location -> NoFileAtS3LocationError(s"Head Object request returned Status code $statusCode")).invalidNel[Obj]
            )

        // compare checksums if file there but checksums don't match, Failure
        case locationInvalidNel => IO.pure(locationInvalidNel)
      }
    }.sequence

  private def checkFileNamesHaveExtensions(fileEntries: List[Map[FieldName, ValidatedNel[InvalidJsonEntry, Obj]]]) =
    fileEntries.map { fileEntry =>
      val nameNel = fileEntry(name)
      val nameValidationResult =
        nameNel match {
          case Validated.Valid(nameObject) =>
            nameObject.str.split('.').toList.reverse match {
              case ext :: _ :: _ => Validated.Valid[Obj](nameObject).toValidatedNel[UnexepectedContent, Obj]
              case _ =>
                (name -> MissingFileExtensionError(s"The file name of does not have an extension")).invalidNel[Obj]
            }
          case invalidatedNel => invalidatedNel
        }
      fileEntry + (name -> nameValidationResult)
    }

  private def validateJsonPerType(entriesGroupedByType: Map[String, List[Value]])(entryType: SchemaType): IO[List[Map[FieldName, ValidatedNel[InvalidJsonEntry, Obj]]]] = {
    val entryObjectValidator = new MetadataJsonValidator(entryType)
    val entries = entriesGroupedByType.getOrElse(entryType.toString, Nil)
    entries.map { entry =>
      val entryJsonObject = entryObjectValidator.convertUjsonObjectToString(entry)
      entryObjectValidator.validateMetadataJsonObject(entryJsonObject)
    }.sequence
  }

  private def checkIfAllIdsAreUuids(allEntries: Map[String, List[Map[FieldName, ValidatedNel[InvalidJsonEntry, Obj]]]]) =
    allEntries.map { case (entryType, entries) =>
      val updatedEntries =
        entries.map { entry =>
          val idNel = entry(id)
          val validatedId = idNel match {
            case Validated.Valid(idObject) =>
              val idAsString = idObject.str
              Try(UUID.fromString(idAsString)) match {
                case Success(uuid) => Validated.Valid[Obj](idObject).toValidatedNel[UnexepectedContent, Obj]
                case _ =>
                  (id -> IdIsNotAUuidError(s"The id $idAsString is not a valid UUID")).invalidNel[Obj]
              }
            case invalidNel => invalidNel
          }
          entry + (id -> validatedId)
        }
      entryType -> updatedEntries
    }

  private def getIdsOfAllEntries(allEntries: Map[String, List[Map[FieldName, ValidatedNel[InvalidJsonEntry, Obj]]]]) = {
    def getEntryType(entryTypeAsString: String, potentialParentId: Option[String]): EntryType =
      entryTypeAsString match {
        case "File"            => FileEntry(potentialParentId)
        case "MetadataFile"    => MetadataFileEntry(potentialParentId)
        case "UnknownFileType" => UnknownFileTypeEntry(potentialParentId)
        case "Asset"           => AssetEntry(potentialParentId)
        case "ArchiveFolder"   => ArchiveFolderEntry(potentialParentId)
        case "ContentFolder"   => ContentFolderEntry(potentialParentId)
      }

    allEntries.flatMap { case (entryType, entries) =>
      entries.flatMap { entry =>
        val idNel = entry(id)
        val parentIdNel = entry("parentId")
        val entryTypeUpdated =
          if entryType == "File" then
            val nameNel = entry("name")
            nameNel match {
              case Validated.Valid(nameObject) =>
                val nameAsString = nameObject.str
                if nameAsString.endsWith("-metadata.json") then "MetadataFile" else entryType
              case invalidNel => "UnknownFileType"
            }
          else entryType

        idNel match {
          case Validated.Valid(idObject) =>
            val idAsString = idObject.str
            parentIdNel match {
              case Validated.Valid(parentIdObject) =>
                val parentIdAsString = parentIdObject.strOpt
                val entryObject = getEntryType(entryTypeUpdated, parentIdAsString)
                List((idAsString, entryObject))
              case invalidNel => List((idAsString, getEntryType(entryTypeUpdated, None)))
            }
          case invalidNel => Nil
        }
      }
    }.toList
  }

  private def checkIfAllIdsAreUnique(allEntries: Map[String, List[Map[FieldName, ValidatedNel[InvalidJsonEntry, Obj]]]], allEntryIds: List[(String, EntryType)]) = {
    val idAndNumberOfEntries = allEntryIds.groupBy(_._1).view.mapValues(_.map(_._2))
    val idsThatAppearMoreThanOnce = idAndNumberOfEntries.filter { case (_, entries) => entries.length > 1 }

    allEntries.map { case (entryType, entries) =>
      val updatedEntries = entries.map { entry =>
        val idNel = entry(id)

        idNel match {
          case Validated.Valid(idObject) =>
            val idAsString = idObject.str
            val numOfIdOccurrences = idsThatAppearMoreThanOnce.getOrElse(idAsString, Nil).length

            if numOfIdOccurrences > 1 then entry + (id -> (id -> IdIsNotUniqueError(s"This id occurs $numOfIdOccurrences times")).invalidNel[Obj])
            else entry

          case invalidNel => entry
        }
      }
      entryType -> updatedEntries
    }
  }

  override def dependencies(config: Config): IO[Dependencies] =
    IO(Dependencies(DAS3Client[IO]()))

  private def checkIfEntriesOfSameTypePointToSameParent(entryTypesGrouped: Map[EntryType, Map[String, EntryType]]) = {
    val entryLinkingErrors = Map()
    val fileParentIds =
      entryTypesGrouped.collect {
        case (FileEntry(Some(parentId)), _)            => parentId
        case (MetadataFileEntry(Some(parentId)), _)    => parentId
        case (UnknownFileTypeEntry(Some(parentId)), _) => parentId
      }.toSet
    lazy val assetParentIds = entryTypesGrouped.collect { case (AssetEntry(Some(parentId)), _) => parentId }.toSet

    lazy val folderEntries =
      entryTypesGrouped.collect {
        case (archiveEntry @ ArchiveFolderEntry(_), ids) => (ids.keys, archiveEntry)
        case (contentEntry @ ContentFolderEntry(_), ids) => (ids.keys, contentEntry)
        case _                                           => false
      }

    val numOfFileParentIds = fileParentIds.length
    val fileLinkingErrors =
      if fileParentIds.length != 1 then
        entryLinkingErrors + ("ErrorType" -> "Files", "Error" -> s"files should all be linked to one parent Asset but $numOfFileParentIds have been found: \n $fileParentIds")
      else entryLinkingErrors

    val numOfAssetParentIds = assetParentIds.length
    val assetLinkingErrors =
      if numOfAassetParentIds != 1 then
        fileLinkingErrors + ("ErrorType" -> "Assets", "Error" -> s"assets should all be linked to one parent Folder but $numOfAssetParentIds have been found: \n $assetParentIds")
      else fileLinkingErrors
  }

  private def checkIfEntriesHaveCorrectParentIds(
      allEntries: Map[String, List[Map[FieldName, ValidatedNel[InvalidJsonEntry, Obj]]]],
      allEntryIds: Map[String, EntryType],
      entryTypesGrouped: Map[EntryType, Map[String, EntryType]]
  ) = {
    val parentId = "parentId"
    allEntries.map { case (entryType, entries) =>
      val updatedEntries =
        entries.map { entry =>
          val idNel = entry(id)
          idNel match {
            case Validated.Valid(idObject) =>
              val idAsString = idObject.str
              val entryTypeInfo = allEntryIds(idAsString)

              val validatedParentId = entryTypeInfo.potentialParentId match {
                case None =>
                  entryTypeInfo match {
                    case FileEntry(_) || AssetEntry(_) =>
                      (parentId -> HierarchyLinkingError(s"The parentId value is null")).invalidNel[Obj]
                    case _ =>
                      Validated.Valid[Obj](parentIdObject).toValidatedNel[UnexepectedContent, Obj]
                  }
                case Some(parentIdThatMightBelongToAnEntry) =>
                  val potentialParentEntryType = allEntryIds.get(parentIdThatMightBelongToAnEntry)
                  potentialParentEntryType match {
                    case None => (parentId -> HierarchyLinkingError(s"The object that this parentId refers to can not be found")).invalidNel[Obj]
                    case Some(parentEntryType) =>
                      lazy val typeOfEntryAsString = entryTypeInfo.getClass.getSimpleName
                      parentEntryType match {
                        case FileEntry(_) =>
                          (parentId -> HierarchyLinkingError("The parentId is for an object of type 'File'")).invalidNel[Obj]
                        case AssetEntry(_) | ArchiveFolderEntry(_) | ContentFolderEntry(_) if typeOfEntryAsString == "AssetEntry" =>
                          (parentId -> HierarchyLinkingError("The parentId is for an object of type 'Asset'")).invalidNel[Obj]
                        case ArchiveFolderEntry(_) | ContentFolderEntry(_) if typeOfEntryAsString == "FileEntry" =>
                          (parentId -> HierarchyLinkingError("The parentId is for an object of type 'ArchiveFolderEntry' or 'ContentFolderEntry'")).invalidNel[Obj]
                        case _ => Validated.Valid[Obj](parentIdObject).toValidatedNel[UnexepectedContent, Obj]
                      }
                  }
              }
              val entryWithUpdatedParentId = entry + (parentId -> validatedParentId)

              if entryType == "Asset" then checkIfAssetReferencesCorrectFiles(entryWithUpdatedParentId, idAsString, entryTypesGrouped)
              else entryWithUpdatedParentId

            case invalidNel => entry
          }
        }
      entryType -> updatedEntries
    }

    val fileEntries = allEntries("File")
    val fileIdsGroupedByParentId = fileEntries.foldLeft(Map[String, List[String]]()) { (idsGroupedByParentId, fileEntry) =>
      val idNel = fileEntry(id)
      val validatedId = (idNel, parentIdNel) match {
        case Validated.Valid(idObject) =>
          val idAsString = idObject.str
          val entryInfo = allEntryIds(idAsString)

        case invalidNel => invalidNel
      }

      val parentId = fileEntry("parentId").str
      val idsBelongingToParentId = idsGroupedByParentId.getOrElse(parentId, Nil)
      idsGroupedByParentId + (parentId -> idAsString :: idsBelongingToParentId)
    }
    val parentAssetIds = fileIdsGroupedByParentId.keySet
    IO.raiseWhen(parentAssetIds.size != 1) {
      new Exception(s"Not all files entries are under the same parent Asset. Here is the grouping: \n $fileIdsGroupedByParentId")
    }
    val fileParentId = parentAssetIds.head
    val assetEntries = allEntries("Asset")
    val assetEntry = assetEntries.head
    val assetId = assetEntry(id).str

    IO.raiseWhen(fileParentId != assetId) {
      new Exception(s"The parentId of the files $fileParentId is not the same as the assetId $assetId")
    }

    val fileIdsBelongingToAsset = fileIdsGroupedByParentId(assetId)
    // val fileIdsAndMetadataFileIds = fileIdsBelongingToAsset(parentAssetId).groupBy{fileId => fileId ==}

    val originalMetadataFileIds = assetEntry("originalMetadataFiles").arr.toList.map(_.str)
    val idsForMetadataEntries = originalMetadataFileIds.diff(fileIdsBelongingToAsset)

    val originalFilesNel = assetEntry("originalFiles")

    val validatedId =
      originalFilesNel match {
        case Validated.Valid(originalFilesObject) =>
          val originalFiles = originalFilesObject.arr.toList.map(_.str)
          originalFiles match {
            case Success(uuid) => Validated.Valid[Obj](idObject).toValidatedNel[UnexepectedContent, Obj]
            case _ =>
              val idAsString = idObject.str
              (id -> IdIsNotAUuidError(s"The id $idAsString is not a valid UUID")).invalidNel[Obj]
          }
        case invalidNel => invalidNel
      }

    val idsForFilesEntries = fileIdsBelongingToAsset.filterNot(idsForMetadataEntries.contains)

    IO.raiseWhen(originalMetadataFileIds != idsForMetadataEntries) {
      new Exception(s"Either an id found in 'originalMetadataFiles' can not be found in the list of entries or vice-versa")
    }

    val assetWithParentId = metadataJson.filter(metadataEntry => metadataEntry("id").str == parentAssetId)
    IO.raiseWhen(assetWithParentId.length != 1) {
      new Exception(s"The files have a parentId of '$parentAssetId' but no Asset with this id exists")
    }
    // def e() =
//    val fileEntries = metadataJson.filter(metadataEntry => metadataEntry("type")).contains("File"))
  }

  private def checkIfAssetReferencesCorrectFiles(
      entryWithUpdatedParentId: Map[FieldName, ValidatedNel[InvalidJsonEntry, Obj]],
      idAsString: String,
      entryTypesGrouped: Map[EntryType, Map[String, EntryType]]
  ) = {
    val fileTypes =
      entryTypesGrouped.collect {
        case (FileEntry(Some(`idAsString`)), ids)            => "expectedOriginalFiles" -> ids.keys.toList
        case (MetadataFileEntry(Some(`idAsString`)), ids)    => "expectedOriginalMetadataFiles" -> ids.keys.toList
        case (UnknownFileTypeEntry(Some(`idAsString`)), ids) => "unexpectedFilesWithNoType" -> ids.keys.toList
      }
    val entryWithUpdatedOriginalFiles = verifyAssetHasCorrectFiles(entryWithUpdatedParentId, fileTypes, "Files")
    val entryWithUpdatedOriginalMetadataFiles = verifyAssetHasCorrectFiles(entryWithUpdatedParentId, fileTypes, "MetadataFiles")
    entryWithUpdatedOriginalMetadataFiles
  }

  private def verifyAssetHasCorrectFiles(
      entryWithUpdatedParentId: Map[FieldName, ValidatedNel[InvalidJsonEntry, Obj]],
      allFileTypes: Map[String, List[String]],
      fileTypeToCheck: "Files" | "MetadataFiles"
  ) = {
    val filesFieldName = s"original$fileTypeToCheck"
    val filesNel = entryWithUpdatedParentId(filesFieldName)
    filesNel match {
      case Validated.Valid(filesObject) =>
        val actualFilesAsList = filesObject.arr.toList.map(_.str)
        val expectedFiles = allFileTypes(s"expectedOriginal$fileTypeToCheck")
        val idsMissingFromFilesList = expectedFiles.diff(actualFilesAsList)
        val idsInFilesListButNotInJson = actualFilesAsList.diff(expectedFiles)
        val idsMissingFromFilesListError =
          if idsMissingFromFilesList.isEmpty then ""
          else s"There are files in the JSON that have the parentId $idAsString but do not appear in '$filesFieldName', they are: $idsMissingFromFilesList\n\n"
        val idsInFilesListButNotInJsonError =
          if idsInFilesListButNotInJson.isEmpty then "" else s"There are files in this array that aren't in the JSON, they are: $idsInFilesListButNotInJson"
        val filesErrorMessage = idsMissingFromFilesListError + idsInFilesListButNotInJsonError

        val filesValidatedNel =
          if filesErrorMessage.isEmpty then Validated.Valid[Obj](filesObject).toValidatedNel[UnexepectedContent, Obj]
          else (s"original$fileTypeToCheck" -> HierarchyLinkingError(filesErrorMessage)).invalidNel[Obj]

        entryWithUpdatedParentId + (filesFieldName -> filesValidatedNel)
      case invalidNel => entryWithUpdatedParentId
    }
  }
}
object Lambda {
  sealed trait EntryType:
    val potentialParentId: Option[String]

  case class FileEntry(potentialParentId: Option[String]) extends EntryType
  case class MetadataFileEntry(potentialParentId: Option[String]) extends EntryType
  case class UnknownFileTypeEntry(potentialParentId: Option[String]) extends EntryType
  case class AssetEntry(potentialParentId: Option[String]) extends EntryType
  case class ArchiveFolderEntry(potentialParentId: Option[String]) extends EntryType
  case class ContentFolderEntry(potentialParentId: Option[String]) extends EntryType

  case class StateOutput(batchId: String, metadataPackage: URI, archiveHierarchyFolders: List[UUID], contentFolders: List[UUID], contentAssets: List[UUID])
  case class Input(batchId: String, metadataPackage: URI)
  case class Config(dynamoTableName: String, discoveryApiUrl: String) derives ConfigReader
  case class Dependencies(s3: DAS3Client[IO])
}
