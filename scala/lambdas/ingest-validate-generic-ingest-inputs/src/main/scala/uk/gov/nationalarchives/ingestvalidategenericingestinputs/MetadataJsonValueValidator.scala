package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.data.*
import cats.effect.IO
import cats.implicits.*
import org.scanamo.*
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
import scala.util.{Failure, Success, Try}

class MetadataJsonValueValidator {
  private val parentId = "parentId"
  private val series = "series"

  def checkFileIsInCorrectS3Location(s3Client: DAS3Client[IO], fileEntries: List[ValidatedEntry]): IO[List[ValidatedNel[ValidationError, Value]]] =
    fileEntries.map { fileEntry =>
      val locationNel = fileEntry(location)
      locationNel match {
        case Validated.Valid(locationObject) =>
          val potentialFileUri = Try(URI.create(locationObject.str))
          potentialFileUri match {
            case Success(fileUri) if fileUri.getHost != null =>
              s3Client
                .headObject(fileUri.getHost, fileUri.getPath.drop(1))
                .redeem(
                  exception => NoFileAtS3LocationError(exception.getMessage).invalidNel[Value],
                  headResponse =>
                    val statusCode = headResponse.sdkHttpResponse().statusCode()
                    if statusCode == 200 then locationNel
                    else NoFileAtS3LocationError(s"Head Object request returned a Status code of $statusCode").invalidNel[Value]
                )
            case Success(_) => IO.pure(ValueError(location, locationObject.str, s"'$location' could not be transformed into a URI").invalidNel[Value])
            case Failure(e) => IO.pure(UriIsNotValid(e.getMessage).invalidNel[Value])
          }
        case locationInvalidNel => IO.pure(locationInvalidNel)
      }
    }.sequence

  def checkFileNamesHaveExtensions(fileEntries: List[ValidatedEntry]): List[ValidatedEntry] =
    fileEntries.map { fileEntry =>
      val nameNel = fileEntry(name)
      val nameValidationResult =
        nameNel match {
          case Validated.Valid(nameValue) =>
            nameValue.str.split('.').toList.reverse match {
              case ext :: _ :: _ => nameNel
              case _ =>
                MissingFileExtensionError(s"The file name does not have an extension at the end of it").invalidNel[Value]
            }
          case invalidatedNel => invalidatedNel
        }
      fileEntry + (name -> nameValidationResult)
    }

  def checkIfAllIdsAreUuids(allEntries: Map[String, List[ValidatedEntry]]): Map[FieldName, List[ValidatedEntry]] =
    allEntries.map { case (entryType, entries) =>
      val updatedEntries =
        entries.map { entry =>
          val idNel = entry(id)
          val validatedId = idNel match {
            case Validated.Valid(idValue) =>
              val idAsString = idValue.str
              Try(UUID.fromString(idAsString)) match {
                case Success(uuid) => idNel
                case _             => IdIsNotAUuidError(s"The id $idAsString is not a valid UUID").invalidNel[Value]
              }
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

              if numOfIdOccurrences > 1 then entry + (id -> IdIsNotUniqueError(s"This id occurs $numOfIdOccurrences times").invalidNel[Value])
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

    def getErrorMessageIfSeriesDoesNotExist(entry: ValidatedEntry, entryType: String) =
      if entry.contains(series) && entryType == "File" then Map(series -> SeriesExistsError("A file can not have a Series").invalidNel[Value])
      else if entry.contains(series) || entryType == "File" then Map()
      else
        Map(
          series -> SeriesDoesNotExistError(
            "The parentId is null and since only top-level entries can have null parentIds, " +
              "and series, this entry should have a 'series' (if it is indeed top-level)"
          ).invalidNel[Value]
        )

    def getErrorMessageIfASeriesExists(entry: ValidatedEntry) =
      if entry.contains(series) then
        Map(
          series -> SeriesExistsError(
            "This entry has a series but has a parentId that's not null; only a top-level entry can have this"
          ).invalidNel[Value]
        )
      else Map()

    allEntries.map { case (entryType, entriesOfSpecificType) =>
      val updatedEntries =
        entriesOfSpecificType.map { entry =>
          val idNel = entry(id)
          idNel match {
            case Validated.Valid(idValue) =>
              val idAsString = idValue.str
              val entryTypeAndParent = allEntryIds(idAsString)
              val parentIdNel = entry(parentId)

              val (validatedParentId, potentialSeriesError) =
                entryTypeAndParent.potentialParentId match {
                  case None =>
                    val parentIdValidatedNel = entryTypeAndParent match {
                      // It's hard to determine whether an ArchiveFolderEntry's parent is null/None because it's a top-level entry
                      // or it's a mistake so further checking needs to be done (in the circular dependencies method).
                      case ArchiveFolderEntry(_) => parentIdNel
                      case ContentFolderEntry(_) =>
                        val potentialParent = checkIfParentTypeExists(2)
                        if potentialParent.nonEmpty then generateParentIdErrorMessage(parentIdNel, potentialParent) else parentIdNel
                      case AssetEntry(_) =>
                        val potentialParent = checkIfParentTypeExists(1)
                        if potentialParent.nonEmpty then generateParentIdErrorMessage(parentIdNel, potentialParent) else parentIdNel
                      case _ => // For a file, a parentId could be 'None', either because its value in the JSON is 'null' or
                        // it failed an earlier validation and 'getIdsOfAllEntries' made the parent 'None' because the value was unavailable
                        val potentialParent = checkIfParentTypeExists(0)
                        generateParentIdErrorMessage(parentIdNel, potentialParent)
                    }
                    val potentialSeriesErrorMessage = getErrorMessageIfSeriesDoesNotExist(entry, entryType)
                    (parentIdValidatedNel, potentialSeriesErrorMessage)
                  case Some("parentId undetermined, due to validation error") => (parentIdNel, Map())
                  case Some(idThatMightBelongToAnEntry) => // check if parentId refers to an entry that is actually in the JSON
                    if idThatMightBelongToAnEntry == idAsString then
                      (HierarchyLinkingError(idThatMightBelongToAnEntry, "The parentId is the same as the id").invalidNel[Value], Map())
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
                      val potentialSeriesErrorMessage = getErrorMessageIfASeriesExists(entry)
                      (parentIdValidatedNel, potentialSeriesErrorMessage)
                }
              val entryWithUpdatedParentId = entry ++ potentialSeriesError + (parentId -> validatedParentId)

              if entryType == "Asset" then checkIfAssetReferencesCorrectFiles(entryWithUpdatedParentId, idAsString, entryTypesGrouped)
              else entryWithUpdatedParentId
            case invalidNel => entry
          }
        }
      entryType -> updatedEntries
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

  private def checkIfAssetReferencesCorrectFiles(
      entryWithUpdatedParentId: ValidatedEntry,
      assetId: String,
      entryTypesGrouped: Map[EntryTypeAndParent, List[(String, EntryTypeAndParent)]]
  ) = {
    val fileTypesAndTheirIds: Map[FieldName, List[FieldName]] =
      entryTypesGrouped.collect {
        case (FileEntry(Some(`assetId`)), ids)            => "expectedOriginalFiles" -> ids.map(_._1)
        case (MetadataFileEntry(Some(`assetId`)), ids)    => "expectedOriginalMetadataFiles" -> ids.map(_._1)
        case (UnknownFileTypeEntry(Some(`assetId`)), ids) => "unexpectedFilesWithNoCategory" -> ids.map(_._1)
      }
    val originalFilesUpdate = verifyAssetHasCorrectFiles(assetId, entryWithUpdatedParentId, fileTypesAndTheirIds, "Files")
    val originalMetadataFilesUpdate = verifyAssetHasCorrectFiles(assetId, entryWithUpdatedParentId, fileTypesAndTheirIds, "MetadataFiles")
    entryWithUpdatedParentId ++ originalFilesUpdate ++ originalMetadataFilesUpdate
  }

  private def verifyAssetHasCorrectFiles(
      assetId: String,
      entryWithUpdatedParentId: ValidatedEntry,
      allFileTypesAndTheirIds: Map[String, List[String]],
      fileTypeToCheck: "Files" | "MetadataFiles"
  ) = {
    val filesFieldName = s"original$fileTypeToCheck"
    val filesNel = entryWithUpdatedParentId(filesFieldName)
    filesNel match {
      case Validated.Valid(filesValue) =>
        val actualFilesAsList = filesValue.arr.toList.map(_.str)
        val expectedFileIds = allFileTypesAndTheirIds.getOrElse(s"expectedOriginal$fileTypeToCheck", Nil)
        val idsMissingFromFilesList = expectedFileIds.diff(actualFilesAsList)
        val idsInFilesListButNotInJson = actualFilesAsList.diff(expectedFileIds)

        val idsMissingFromFilesListPotentialError =
          if idsMissingFromFilesList.isEmpty then filesNel
          else
            HierarchyLinkingError(
              idsMissingFromFilesList.mkString(", "),
              s"There are files in the JSON that have the parentId of this Asset ($assetId) but do not appear in '$filesFieldName'"
            ).invalidNel[Value]

        val idsInFilesListButNotInJsonPotentialError =
          if idsInFilesListButNotInJson.isEmpty then filesNel
          else
            val idsForUncategorisedFileType = allFileTypesAndTheirIds.getOrElse("unexpectedFilesWithNoCategory", Nil)
            val additionalErrorMessage =
              if idsForUncategorisedFileType.isEmpty then ""
              else
                s"\n\nIt's also possible that the files are in the JSON but whether they were $filesFieldName or not, " +
                  s"could not be determined, these files are: $idsForUncategorisedFileType"
            HierarchyLinkingError(
              idsInFilesListButNotInJson.mkString(", "),
              s"There are files in the '$filesFieldName' array that don't appear in the JSON or their parentId is not the same as this Asset's ('$assetId')" +
                additionalErrorMessage
            ).invalidNel[Value]

        val filesValidatedNel = idsMissingFromFilesListPotentialError.combine(idsInFilesListButNotInJsonPotentialError)

        Map(filesFieldName -> filesValidatedNel)
      case invalidNel => Map(filesFieldName -> filesNel)
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

  def getIdsOfAllEntries(allEntries: Map[String, List[ValidatedEntry]]): List[(FieldName, EntryTypeAndParent)] = {
    def getEntryType(entryTypeAsString: String, potentialParentId: Option[String]): EntryTypeAndParent =
      entryTypeAsString match {
        case "File"            => FileEntry(potentialParentId)
        case "MetadataFile"    => MetadataFileEntry(potentialParentId)
        case "UnknownFileType" => UnknownFileTypeEntry(potentialParentId)
        case "Asset"           => AssetEntry(potentialParentId)
        case "ArchiveFolder"   => ArchiveFolderEntry(potentialParentId)
        case "ContentFolder"   => ContentFolderEntry(potentialParentId)
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
                val nameAsString = nameValue.str
                if nameAsString.endsWith("-metadata.json") then "MetadataFile" else entryType
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
}

object MetadataJsonValueValidator:
  case class MissingFileExtensionError(errorMessage: String) extends ValidationError
  case class UriIsNotValid(errorMessage: String) extends ValidationError
  case class NoFileAtS3LocationError(errorMessage: String) extends ValidationError
  case class IdIsNotAUuidError(errorMessage: String) extends ValidationError
  case class IdIsNotUniqueError(errorMessage: String) extends ValidationError
  case class HierarchyLinkingError(valueThatCausedError: String, errorMessage: String) extends ValidationError
  case class SeriesDoesNotExistError(errorMessage: String) extends ValidationError
  case class SeriesExistsError(errorMessage: String) extends ValidationError