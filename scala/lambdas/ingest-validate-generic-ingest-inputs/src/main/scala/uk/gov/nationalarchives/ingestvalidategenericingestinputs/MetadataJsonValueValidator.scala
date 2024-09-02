package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.effect.IO
import cats.data.*
import cats.implicits.*
import org.scanamo.*
import org.scanamo.generic.semiauto.*
import ujson.*
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda.given
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.ValueError
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonValueValidator.*

import java.net.URI
import java.util.UUID
import scala.annotation.tailrec
import scala.collection.immutable.Map
import scala.util.{Failure, Success, Try}

class MetadataJsonValueValidator {
  def checkFileIsInCorrectS3Location(s3Client: DAS3Client[IO], fileEntries: List[Entry]): IO[List[ValidatedNel[ValidationError, Value]]] =
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
                    // compare checksums if file is there and if checksums don't match, Failure?
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

  def checkFileNamesHaveExtensions(fileEntries: List[Entry]): List[Entry] =
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

  def checkIfAllIdsAreUuids(allEntries: Map[String, List[Entry]]): Map[FieldName, List[Entry]] =
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

  def getIdsOfAllEntries(allEntries: Map[String, List[Entry]]): List[(FieldName, EntryTypeAndParent)] = {
    def getEntryType(entryTypeAsString: String, potentialParentId: Option[String]): EntryTypeAndParent =
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
    }.toList
  }

  def checkIfAllIdsAreUnique(allEntries: Map[String, List[Entry]], allEntryIds: List[(String, EntryTypeAndParent)]): Map[FieldName, List[Entry]] = {
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
      allEntries: Map[String, List[Entry]],
      allEntryIds: Map[String, EntryTypeAndParent],
      entryTypesGrouped: Map[EntryTypeAndParent, List[(String, EntryTypeAndParent)]]
  ): Map[FieldName, List[Entry]] = {
    val parentId = "parentId"
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
                    val series = "series"
                    def checkIfParentTypeExistsOrSameTypeMoreThanOnce(numOfParentTypesToRemove: Int, fileType: Boolean=false) = {
                      val parentTypes = List("Asset", "ContentFolder", "ArchiveFolder")
                      val allEntryTypes = allEntries.keys
                      val entriesWithAParentType = allEntryTypes.filter(parentTypes.drop(numOfParentTypesToRemove).contains(_))

                      val entriesWithTheSameTyped =
                        if !fileType then
                          val entriesThatHaveTheSameType = allEntryTypes.filter(_ == parentTypes(numOfParentTypesToRemove - 1)).toList
                          if entriesThatHaveTheSameType.length > 1 then entriesThatHaveTheSameType else Nil
                        else Nil

                      (entriesWithAParentType.toSet, entriesWithTheSameTyped.toSet)
                     }

                    def getErrorMessageIfSeriesDoesNotExist =
                      if entry.contains(series) || entryType == "File" then Map()
                      else Map(series -> SeriesDoesNotExist("The parentId is null and only top-level items have null parentIds, therefore, this entry should have a 'series'").invalidNel[Value])

                    val parentIdValidatedNel = entryTypeAndParent match {
                      // figure out how to store the fact that one of these is the top-level folder
                      case ArchiveFolderEntry(_) => parentIdNel
                      case ContentFolderEntry(_) =>
                        val (potentialParent, potentialEntryOfSameType) = checkIfParentTypeExistsOrSameTypeMoreThanOnce(2)
                        if potentialParent.nonEmpty || (potentialParent.isEmpty && potentialEntryOfSameType.nonEmpty) then
                          generateParentIdErrorMessage(parentIdNel, potentialParent, potentialEntryOfSameType)
                        else parentIdNel
                      case AssetEntry(_) =>
                        val (potentialParent, potentialEntryOfSameType) = checkIfParentTypeExistsOrSameTypeMoreThanOnce(1)
                        if potentialParent.nonEmpty || (potentialParent.isEmpty && potentialEntryOfSameType.nonEmpty) then
                          generateParentIdErrorMessage(parentIdNel, potentialParent, potentialEntryOfSameType)
                        else parentIdNel
                      case _ => // for a file, a parentId could be 'None', either because it's value in the JSON is 'null' or
                        // it failed an earlier validation and 'getIdsOfAllEntries' made the parent 'None' because the value was unavailable
                        val (potentialParent, potentialEntryOfSameType) = checkIfParentTypeExistsOrSameTypeMoreThanOnce(0, true)
                        generateParentIdErrorMessage(parentIdNel, potentialParent, potentialEntryOfSameType)
                    }
                    val potentialSeriesErrorMessage = getErrorMessageIfSeriesDoesNotExist
                    (parentIdValidatedNel, potentialSeriesErrorMessage)
                  case Some("parentId undetermined, due to validation error") => (parentIdNel, Map())
                  case Some(idThatMightBelongToAnEntry) => // check if parentId refers to an entry that is actually in the JSON
                    val potentialEntryType = allEntryIds.get(idThatMightBelongToAnEntry)
                    val parentIdValidatedNel = potentialEntryType match {
                      case None =>
                        HierarchyLinkingError(idThatMightBelongToAnEntry, "The object that this parentId refers to can not be found in the JSON").invalidNel[Value]
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
                    (parentIdValidatedNel, Map())
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

  private def generateParentIdErrorMessage(parentIdNel: ValidatedNel[ValidationError, Value], parentType: Set[String], entryOfSameType: Set[String]) = {
    val parentTypeMessage = if parentType.isEmpty then "and no parent entry exists in the JSON to refer to" else s"despite at least one ${parentType.mkString(" and ")} existing"
    val entryOfSameTypeMessage = if entryOfSameType.isEmpty then "" else "but there are other entries of the same type"
    parentIdNel match {
      case Validated.Valid(parentIdThatIsNull) =>
        HierarchyLinkingError(
          parentIdThatIsNull.strOpt.getOrElse("null"),
          s"The parentId value is 'null' $parentTypeMessage $entryOfSameTypeMessage"
        ).invalidNel[Value]
      case invalidNel => invalidNel
    }
  }

  private def checkIfAssetReferencesCorrectFiles(
      entryWithUpdatedParentId: Entry,
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
      entryWithUpdatedParentId: Entry,
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
                s"\n\nIt's also possible that the files are in the JSON but whether they were $filesFieldName or not, could not be determined, " +
                  s"these files are: $idsForUncategorisedFileType"
            HierarchyLinkingError(
              idsInFilesListButNotInJson.mkString(", "),
              s"There are files in this '$filesFieldName' that don't appear in the JSON or their parentId is not the same as this Asset's ('$assetId')" +
                additionalErrorMessage
            ).invalidNel[Value]

        val filesValidatedNel = idsMissingFromFilesListPotentialError.combine(idsInFilesListButNotInJsonPotentialError)

        Map(filesFieldName -> filesValidatedNel)
      case invalidNel => Map(filesFieldName -> filesNel)
    }
  }

  def checkIfEntriesOfSameTypePointToSameParent(entryTypesGrouped: Map[EntryTypeAndParent, Map[String, EntryTypeAndParent]]) = {
    val entryLinkingErrors: List[Map[String, String]] = Nil
//    val fileParentIds =
//      entryTypesGrouped.collect {
//        case (FileEntry(Some(parentId)), _)            => parentId
//        case (MetadataFileEntry(Some(parentId)), _)    => parentId
//        case (UnknownFileTypeEntry(Some(parentId)), _) => parentId
//      }.toSet
//
//    val numOfFileParentIds = fileParentIds.size
//    val fileLinkingError =
//      if numOfFileParentIds != 1 then
//        entryLinkingErrors :+ Map(
//          "ErrorType" -> "Files",
//          "Error" -> s"Files should all be linked to one parent Asset but $numOfFileParentIds have been found: \n $fileParentIds"
//        )
//      else entryLinkingErrors

    val assetParentIds = entryTypesGrouped.collect { case (AssetEntry(Some(parentId)), _) => parentId }.toSet
    val numOfAssetParentIds = assetParentIds.size
    val assetLinkingError =
      if numOfAssetParentIds != 1 then
        entryLinkingErrors :+ Map(
          "ErrorType" -> "Assets",
          "Error" -> s"Assets should all be linked to one parent Folder but $numOfAssetParentIds have been found: \n $assetParentIds"
        )
      else entryLinkingErrors

    val folderIdsAndParent =
      entryTypesGrouped.toList.collect {
        case (ArchiveFolderEntry(potentialParentId), idsWithSameParent) => (idsWithSameParent.keys.toList, potentialParentId)
        case (ContentFolderEntry(potentialParentId), idsWithSameParent) => (idsWithSameParent.keys.toList, potentialParentId)
      }

    val folderIdsGroupedByParent2 = folderIdsAndParent
      .groupBy { case (_, potentialParentId) => potentialParentId }

    val folderIdsGroupedByParent = folderIdsAndParent
      .groupBy { case (_, potentialParentId) => potentialParentId }
      .view
      .mapValues(_.flatMap { case (ids, _) => ids })
      .toMap

    val foldersWithSameParentError = assetLinkingError ++
      folderIdsGroupedByParent.toList.collect {
        case (parentId, idsWithSameParent) if idsWithSameParent.length > 1 =>
          Map("ErrorType" -> "Folder", "Error" -> s"These folders $idsWithSameParent have the same parentId $parentId")
      }

    @tailrec
    def verifyParentHasChildFolder(potentialParentId: Option[String]): Map[String, String] = {
      val potentialChild = folderIdsGroupedByParent.get(potentialParentId)
      potentialChild match {
        case None if potentialChild.contains(assetParentIds.toList) => Map()
        case Some(List(id))                                         => verifyParentHasChildFolder(Some(id))
        case _ =>
          Map("ErrorType" -> "Folder", "Error" -> s"id $potentialParentId is not the parent of anything")
      }
    }

    foldersWithSameParentError :+ verifyParentHasChildFolder(None)
  }
}

object MetadataJsonValueValidator:
  case class MissingFileExtensionError(errorMessage: String) extends ValidationError
  case class UriIsNotValid(errorMessage: String) extends ValidationError
  case class NoFileAtS3LocationError(errorMessage: String) extends ValidationError
  case class IdIsNotAUuidError(errorMessage: String) extends ValidationError
  case class IdIsNotUniqueError(errorMessage: String) extends ValidationError
  case class HierarchyLinkingError(valueThatCausedError: String, errorMessage: String) extends ValidationError
  case class SeriesDoesNotExist(errorMessage: String) extends ValidationError
