package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.data.*
import cats.effect.IO
import cats.implicits.*
import org.scanamo.generic.semiauto.*
import ujson.*
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.EntryType.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.EntryValidationError.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonValueValidator.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.ValidatedUtils.{ValidatedEntry, given}

import java.net.URI
import java.util.UUID
import scala.annotation.tailrec
import scala.collection.immutable.Map

class MetadataJsonValueValidator {
  private val parentId = "parentId"
  private val series = "series"

  def checkFileIsInCorrectS3Location(s3Client: DAS3Client[IO], fileEntries: List[ValidatedEntry]): IO[List[ValidatedEntry]] =
    fileEntries.map { fileEntry =>
      val locationNel = fileEntry(location)
      (locationNel match {
        case Validated.Valid(locationObject) =>
          val fileLocation = locationObject.str
          IO(URI.create(fileLocation))
            .flatMap { fileUri =>
              if fileUri.getHost != null then
                s3Client
                  .headObject(fileUri.getHost, fileUri.getPath.drop(1))
                  .redeem(
                    exception => NoFileAtS3LocationError(fileLocation, exception.getMessage).invalidNel[Value],
                    headResponse =>
                      val statusCode = headResponse.sdkHttpResponse().statusCode()
                      if statusCode == 200 then locationNel
                      else NoFileAtS3LocationError(fileLocation, s"Head Object request returned a Status code of $statusCode").invalidNel[Value]
                  )
              else IO.pure(UriIsNotValid(fileLocation, s"'$location' could not be transformed into a URI").invalidNel[Value])
            }
            .recoverWith(err => IO.pure(UriIsNotValid(fileLocation, err.getMessage).invalidNel[Value]))

        case locationInvalidNel => IO.pure(locationInvalidNel)
      }).map(nel => fileEntry + (location -> nel))
    }.sequence

  def checkMetadataFileNamesHaveJsonExtensions(metadataFileEntries: List[ValidatedEntry]): List[ValidatedEntry] =
    metadataFileEntries.map { metadataFileEntry =>
      val nameNel = metadataFileEntry(name)
      val nameValidationResult =
        nameNel match {
          case Validated.Valid(nameValue) =>
            val fileName = nameValue.str
            if fileName.endsWith(".json") then nameNel
            else MissingFileExtensionError(fileName, "The metadata file name does not end with a '.json'").invalidNel[Value]
          case invalidatedNel => invalidatedNel
        }
      metadataFileEntry + (name -> nameValidationResult)
    }

  def checkIfAllIdsAreUuids(allEntries: Map[String, List[ValidatedEntry]]): Map[FieldName, List[ValidatedEntry]] =
    allEntries.map { case (entryType, entries) =>
      val updatedEntries =
        entries.map { entry =>
          val idNel = entry(id)
          val validatedId = idNel match {
            case Validated.Valid(idValue) =>
              val idAsString = idValue.str
              Validated.catchNonFatal(UUID.fromString(idAsString)) match
                case Validated.Valid(_)   => idNel
                case Validated.Invalid(_) => IdIsNotAUuidError(idAsString, s"The id is not a valid UUID").invalidNel[Value]
            case invalidNel => invalidNel
          }
          entry + (id -> validatedId)
        }
      entryType -> updatedEntries
    }

  def checkIfAllIdsAreUnique(allEntries: Map[String, List[ValidatedEntry]], allEntryIds: List[(String, EntryTypeAndParent)]): Map[FieldName, List[ValidatedEntry]] = {
    val idsAndEntriesWithId = allEntryIds
      .groupBy { case (id, _) => id }
      .view
      .mapValues(_.map { case (_, entryType) => entryType })
      .toMap

    val idsThatAppearMoreThanOnce = idsAndEntriesWithId.filter { case (_, entries) => entries.length > 1 }

    if idsThatAppearMoreThanOnce.nonEmpty then
      allEntries.map { case (entryType, entries) =>
        val updatedEntries = entries.map { entry =>
          val idNel = entry(id)

          idNel match {
            case Validated.Valid(idValue) =>
              val idAsString = idValue.str
              val numOfIdOccurrences = idsThatAppearMoreThanOnce.getOrElse(idAsString, Nil).length

              if numOfIdOccurrences > 1 then entry + (id -> IdIsNotUniqueError(idAsString, s"This id occurs $numOfIdOccurrences times").invalidNel[Value])
              else entry
            case invalidNel => entry
          }
        }
        entryType -> updatedEntries
      }
    else allEntries
  }

  def checkIfEntriesHaveCorrectParentIds(
      allEntries: Map[String, List[ValidatedEntry]],
      allEntryIds: Map[String, EntryTypeAndParent],
      entryTypesGrouped: Map[EntryTypeAndParent, List[(String, EntryTypeAndParent)]]
  ): Map[FieldName, List[ValidatedEntry]] = {
    def checkIfParentTypeExists(numOfParentTypesToRemove: Int) = {
      val parentTypes = List("Asset", "ContentFolder", "ArchiveFolder")
      val allEntryTypes = allEntries.keys
      val entriesWithAParentType = allEntryTypes.filter(parentTypes.drop(numOfParentTypesToRemove).contains(_))
      entriesWithAParentType.toSet
    }

    def getErrorMessageIfASeriesExists(entry: ValidatedEntry) =
      if entry.contains(series) then
        Map(
          series -> SeriesExistsError(
            "This entry has a series but has a parentId that's not null; only a top-level entry can have this"
          ).invalidNel[Value]
        )
      else Map.empty

    allEntries.map { case (entryType, entriesOfSpecificType) =>
      val updatedEntries =
        entriesOfSpecificType.map { entry =>
          val idNel = entry(id)
          idNel match {
            case Validated.Valid(idValue) =>
              val idAsString = idValue.str
              val entryTypeAndParent = allEntryIds(idAsString)
              val parentIdNel = entry(parentId)
              val getErrorIfFileHasSeries =
                (entry: ValidatedEntry) => if entry.contains("series") then Map(series -> SeriesExistsError("A file can not have a Series").invalidNel[Value]) else Map.empty
              lazy val defaultParentResult = Map(parentId -> parentIdNel)

              val validatedParentIdAndPotentialSeriesError =
                entryTypeAndParent.potentialParentId match {
                  case None =>
                    entryTypeAndParent match {
                      case FileEntry(_) | MetadataFileEntry(_) | UnknownFileTypeEntry(_) =>
                        val updatedParentIdNel = HierarchyLinkingError("null", "The parentId value is 'null'").invalidNel[Value]
                        Map(parentId -> updatedParentIdNel) ++ getErrorIfFileHasSeries(entry)
                      case _ if !entry.contains("series") =>
                        val updatedParentIdNel = HierarchyLinkingError("null", "The parentId value is 'null' but there is no series").invalidNel[Value]
                        val updatedSeriesNel = SeriesDoesNotExistError("A series cannot be missing if parentId is 'null'").invalidNel[Value]
                        Map(parentId -> updatedParentIdNel, series -> updatedSeriesNel)
                      case _ => defaultParentResult
                    }
                  case Some("parentId undetermined, due to validation error") =>
                    entryTypeAndParent match {
                      case FileEntry(_) | MetadataFileEntry(_) | UnknownFileTypeEntry(_) => defaultParentResult ++ getErrorIfFileHasSeries(entry)
                      case _                                                             => defaultParentResult
                    }
                  case Some(idThatMightBelongToAnEntry) => // check if parentId refers to an entry that is actually in the JSON
                    if idThatMightBelongToAnEntry == idAsString then
                      Map(parentId -> HierarchyLinkingError(idThatMightBelongToAnEntry, "The parentId is the same as the id").invalidNel[Value])
                    else
                      val potentialEntryType = allEntryIds.get(idThatMightBelongToAnEntry)
                      val parentIdValidatedNel = potentialEntryType match {
                        case None =>
                          HierarchyLinkingError(idThatMightBelongToAnEntry, "The object that this parentId refers to, can not be found in the JSON").invalidNel[Value]
                        case Some(parentEntryType) =>
                          lazy val typeOfEntryAsString = entryTypeAndParent.getClass.getSimpleName
                          parentEntryType match {
                            case FileEntry(_) | MetadataFileEntry(_) | UnknownFileTypeEntry(_) =>
                              HierarchyLinkingError(idThatMightBelongToAnEntry, "The parentId is for an object of type 'File'").invalidNel[Value]
                            case AssetEntry(_) if List("ArchiveFolderEntry", "ContentFolderEntry", "AssetEntry").contains(typeOfEntryAsString) =>
                              HierarchyLinkingError(idThatMightBelongToAnEntry, "The parentId is for an object of type 'Asset'").invalidNel[Value]
                            case ContentFolderEntry(_) if List("ArchiveFolderEntry", "FileEntry", "MetadataFileEntry", "UnknownFileTypeEntry").contains(typeOfEntryAsString) =>
                              HierarchyLinkingError(idThatMightBelongToAnEntry, "The parentId is for an object of type 'ContentFolder'").invalidNel[Value]
                            case ArchiveFolderEntry(_) if List("AssetEntry", "FileEntry", "MetadataFileEntry", "UnknownFileTypeEntry").contains(typeOfEntryAsString) =>
                              HierarchyLinkingError(idThatMightBelongToAnEntry, "The parentId is for an object of type 'ArchiveFolder'").invalidNel[Value]
                            case _ => parentIdNel
                          }
                      }
                      Map(parentId -> parentIdValidatedNel) ++ getErrorMessageIfASeriesExists(entry)
                }
              val entryWithUpdatedParentId = entry ++ validatedParentIdAndPotentialSeriesError

              if entryType == "Asset" then checkIfAssetReferencesCorrectFiles(entryWithUpdatedParentId, idAsString, entryTypesGrouped)
              else entryWithUpdatedParentId
            case invalidNel => entry
          }
        }
      entryType -> updatedEntries
    }
  }

  private def checkIfAssetReferencesCorrectFiles(assetEntry: ValidatedEntry, assetId: String, entryTypesGrouped: Map[EntryTypeAndParent, List[(String, EntryTypeAndParent)]]) = {
    val fileArrayNamesAndExpectedFileIds: Map[FieldName, List[FieldName]] =
      entryTypesGrouped.collect {
        case (FileEntry(Some(`assetId`)), idsAndEntryType)            => "expectedOriginalFiles" -> idsAndEntryType.map(_._1)
        case (MetadataFileEntry(Some(`assetId`)), idsAndEntryType)    => "expectedOriginalMetadataFiles" -> idsAndEntryType.map(_._1)
        case (UnknownFileTypeEntry(Some(`assetId`)), idsAndEntryType) => "unexpectedFilesWithNoCategory" -> idsAndEntryType.map(_._1)
      }
    val originalFilesUpdate = verifyAssetHasCorrectFiles(assetId, assetEntry, fileArrayNamesAndExpectedFileIds, "Files")
    val originalMetadataFilesUpdate = verifyAssetHasCorrectFiles(assetId, assetEntry, fileArrayNamesAndExpectedFileIds, "MetadataFiles")
    assetEntry ++ originalFilesUpdate ++ originalMetadataFilesUpdate
  }

  private def verifyAssetHasCorrectFiles(
      assetId: String,
      assetEntry: ValidatedEntry,
      expectedFileArrayNamesAndFileIds: Map[String, List[String]],
      fileTypeToCheck: "Files" | "MetadataFiles"
  ) = {
    val filesArrayFieldName = s"original$fileTypeToCheck"
    val filesArrayNel = assetEntry(filesArrayFieldName)
    filesArrayNel match {
      case Validated.Valid(filesArrayAsValue) =>
        val actualFilesAsList = filesArrayAsValue.arr.toList.map(_.str)
        val expectedFileIds = expectedFileArrayNamesAndFileIds.getOrElse(s"expectedOriginal$fileTypeToCheck", Nil)
        val idsMissingFromFilesList = expectedFileIds.diff(actualFilesAsList)
        val idsInFilesListButNotInJson = actualFilesAsList.diff(expectedFileIds)

        val idsMissingFromFilesListPotentialError =
          if idsMissingFromFilesList.isEmpty then filesArrayNel
          else
            HierarchyLinkingError(
              idsMissingFromFilesList.mkString(", "),
              s"There are files in the JSON that have the parentId of this Asset ($assetId) but do not appear in '$filesArrayFieldName'"
            ).invalidNel[Value]

        val idsInFilesListButNotInJsonPotentialError =
          if idsInFilesListButNotInJson.isEmpty then filesArrayNel
          else
            val idsForUncategorisedFileType = expectedFileArrayNamesAndFileIds.getOrElse("unexpectedFilesWithNoCategory", Nil)
            val additionalErrorMessage =
              if idsForUncategorisedFileType.isEmpty then ""
              else
                s"\n\nIt's also possible that the files are in the JSON but whether they were $filesArrayFieldName or not, " +
                  s"could not be determined, these files are: $idsForUncategorisedFileType"
            HierarchyLinkingError(
              idsInFilesListButNotInJson.mkString(", "),
              s"There are files in the '$filesArrayFieldName' array that don't appear in the JSON or their parentId is not the same as this Asset's ('$assetId')" +
                additionalErrorMessage
            ).invalidNel[Value]

        val filesValidatedNel = idsMissingFromFilesListPotentialError.combine(idsInFilesListButNotInJsonPotentialError)

        Map(filesArrayFieldName -> filesValidatedNel)
      case invalidNel => Map(filesArrayFieldName -> filesArrayNel)
    }
  }

  def checkForCircularDependenciesInFolders(allEntries: Map[String, List[ValidatedEntry]]): Map[FieldName, List[ValidatedEntry]] = {
    // It's entirely possible that a folder (by mistake) references its own child entry, so this needs to be checked
    val entriesOfAllFolderTypes = allEntries.filter { case (entryType, _) => List("ArchiveFolder", "ContentFolder").contains(entryType) }

    val allFolderTypeEntryIds = getIdsOfAllEntries(entriesOfAllFolderTypes).toMap
    val updatedFolderEntries =
      entriesOfAllFolderTypes.map { case (folderEntryType, entriesOfSpecificFolderType) =>
        val updatedEntries =
          entriesOfSpecificFolderType.map { entry =>
            val idNel = entry(id)
            val updatedEntry =
              idNel match {
                case Validated.Valid(idValue) =>
                  val idAsString = idValue.str
                  val parentIdNel = entry(parentId)
                  val updatedParentId = getCircularDependencyErrors(allFolderTypeEntryIds, idAsString, idAsString, Nil, parentIdNel)
                  entry ++ Map(parentId -> updatedParentId)
                case invalidNel => entry
              }
            updatedEntry
          }
        folderEntryType -> updatedEntries
      }
    allEntries ++ updatedFolderEntries
  }

  def getIdsOfAllEntries(allEntries: Map[String, List[ValidatedEntry]], metadataFileIds: List[String] = Nil): List[(FieldName, EntryTypeAndParent)] = {
    def getEntryType(entryTypeAsString: String, potentialParentId: Option[String]): EntryTypeAndParent =
      entryTypeAsString match {
        case "File"            => FileEntry(potentialParentId)
        case "MetadataFile"    => MetadataFileEntry(potentialParentId)
        case "UnknownFileType" => UnknownFileTypeEntry(potentialParentId)
        case "Asset"           => AssetEntry(potentialParentId)
        case "ArchiveFolder"   => ArchiveFolderEntry(potentialParentId)
        case "ContentFolder"   => ContentFolderEntry(potentialParentId)
        case "UnknownType"     => UnknownTypeEntry(potentialParentId)
      }

    allEntries.toList.flatMap { case (entryType, entries) =>
      entries.flatMap { entry =>
        val idNel = entry(id)
        val parentIdNel = entry("parentId")
        val entryTypeUpdated =
          if entryType == "File" then
            val nameNel = entry("name")
            nameNel match {
              case Validated.Valid(nameValue) =>
                val idOfFile = idNel.getOrElse(Str("idFieldHasErrorInIt")).str
                val nameAsString = nameValue.str
                if nameAsString.endsWith("-metadata.json") || metadataFileIds.contains(idOfFile) then "MetadataFile" else entryType
              case invalidNel => "UnknownFileType"
            }
          else entryType

        idNel match {
          case Validated.Valid(idValue) =>
            val idAsString = idValue.str
            parentIdNel match {
              case Validated.Valid(parentIdValue) =>
                val potentialParentIdAsString = parentIdValue.strOpt
                val entryValue = getEntryType(entryTypeUpdated, potentialParentIdAsString)
                List((idAsString, entryValue))
              case invalidNel => List((idAsString, getEntryType(entryTypeUpdated, Some("parentId undetermined, due to validation error"))))
            }
          case invalidNel => Nil
        }
      }
    }
  }

  private def generateParentIdErrorMessage(parentIdNel: ValidatedNel[ValidationError, Value], parentType: Set[String]) = {
    val parentTypeMessage =
      if parentType.isEmpty then "and no parent entry exists in the JSON to refer to"
      else s"despite at least one ${parentType.mkString(" and ")} existing"
    parentIdNel match {
      case Validated.Valid(parentIdThatIsNull) =>
        HierarchyLinkingError(
          parentIdThatIsNull.strOpt.getOrElse("null"),
          s"The parentId value is 'null' $parentTypeMessage"
        ).invalidNel[Value]
      case invalidNel => invalidNel
    }
  }

  @tailrec
  private def getCircularDependencyErrors(
      allFolderTypeEntryIds: Map[FieldName, EntryTypeAndParent],
      originalId: String,
      idToLookUpNext: String,
      idsOfParents: List[String],
      parentIdNel: ValidatedNel[ValidationError, Value]
  ): ValidatedNel[ValidationError, Value] = {
    val entryTypeAndParent = allFolderTypeEntryIds(idToLookUpNext)
    entryTypeAndParent.potentialParentId match {
      case None | Some("parentId undetermined, due to validation error") => parentIdNel
      // E.g. (if starting folder is 'f1' and its parents increment by 1) if f2's parentId is f3 and f3's is f2,
      // don't return error as it will be caught when its f2/f3's turn
      case Some(parentId) if idsOfParents.contains(parentId) => parentIdNel
      case Some(parentId) if parentId == originalId =>
        val parentIdThatReferencesEntry = idsOfParents.last
        val idsOfParentsWithId = List(parentId) ::: idsOfParents ::: List(parentId)
        val breadcrumbOfIds = idsOfParentsWithId.mkString(" > ")
        HierarchyLinkingError(
          parentIdThatReferencesEntry,
          s"Circular dependency! A parent entry (id '$parentIdThatReferencesEntry') references this entry as its parentId.\n\n" +
            s"The breadcrumb trail looks like this $breadcrumbOfIds"
        ).invalidNel[Value]
      case Some(parentId) => getCircularDependencyErrors(allFolderTypeEntryIds, originalId, parentId, idsOfParents ::: List(parentId), parentIdNel)
    }
  }
}

object MetadataJsonValueValidator:
  case class MissingFileExtensionError(valueThatCausedError: String, errorMessage: String) extends ValueError
  case class UriIsNotValid(valueThatCausedError: String, errorMessage: String) extends ValueError
  case class NoFileAtS3LocationError(valueThatCausedError: String, errorMessage: String) extends ValueError
  case class IdIsNotAUuidError(valueThatCausedError: String, errorMessage: String) extends ValueError
  case class IdIsNotUniqueError(valueThatCausedError: String, errorMessage: String) extends ValueError
  case class HierarchyLinkingError(valueThatCausedError: String, errorMessage: String) extends ValueError
  case class SeriesDoesNotExistError(errorMessage: String) extends ValidationError
  case class SeriesExistsError(errorMessage: String) extends ValidationError
