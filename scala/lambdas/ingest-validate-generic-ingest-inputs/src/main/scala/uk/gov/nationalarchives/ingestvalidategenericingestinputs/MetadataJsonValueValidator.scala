package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.data.*
import cats.implicits.*
import org.scanamo.*
import org.scanamo.generic.semiauto.*
import ujson.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda.given
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonValueValidator.*

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.immutable.Map
import scala.util.{Success, Try}

class MetadataJsonValueValidator {
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
                case _ => IdIsNotAUuidError(s"The id $idAsString is not a valid UUID").invalidNel[Value]
              }
            case invalidNel => invalidNel
          }
          entry + (id -> validatedId)
        }
      entryType -> updatedEntries
    }

  def getIdsOfAllEntries(allEntries: Map[String, List[Entry]]) = {
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
              case invalidNel => List((idAsString, getEntryType(entryTypeUpdated, None)))
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
      .mapValues(_.map { case (_, entryType) => entryType }).toMap

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
  ) = {
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

              val validatedParentId = entryTypeAndParent.potentialParentId match {
                case None =>
                  entryTypeAndParent match {
                    case ArchiveFolderEntry(_) => parentIdNel
                    case _ => // parentId is 'None', either...
                      parentIdNel match {
                        case Validated.Valid(parentIdThatIsNull) =>  // ...because it's value in the JSON is 'null'
                          HierarchyLinkingError(parentIdThatIsNull.strOpt.getOrElse("null"), "The parentId value is 'null'").invalidNel[Value]
                        case invalidNel => invalidNel  // ...or it failed a previous validation and 'getIdsOfAllEntries' made the parent 'None'
                      }

                  }
                case Some(idThatMightBelongToAnEntry) => // check if parentId refers to an entry that is actually in the JSON
                  print("\n\n\n\ntypeOfEntryAsString", entryTypeAndParent.getClass.getSimpleName)
                  print("\n\n\n\nidAsString", idAsString)
                  print("\n\n\n\nparent Id ", idThatMightBelongToAnEntry)
                  val potentialEntryType = allEntryIds.get(idThatMightBelongToAnEntry)
                  print("\n\n\n\npotentialEntryType", potentialEntryType)
                  potentialEntryType match {
                    case None =>
                      HierarchyLinkingError(
                        idThatMightBelongToAnEntry,
                        "The object that this parentId refers to can not be found in the JSON").invalidNel[Value]
                    case Some(parentEntryType) =>
                      lazy val typeOfEntryAsString = entryTypeAndParent.getClass.getSimpleName
                      parentEntryType match {
                        case FileEntry(_) | MetadataFileEntry(_) | UnknownFileTypeEntry(_) =>
                          HierarchyLinkingError(idThatMightBelongToAnEntry, "The parentId is for an object of type 'File'").invalidNel[Value]
                        case AssetEntry(_) if List("ArchiveFolderEntry", "ContentFolderEntry", "AssetEntry").contains(typeOfEntryAsString) =>
                          HierarchyLinkingError(idThatMightBelongToAnEntry, "The parentId is for an object of type 'Asset'").invalidNel[Value]
                        case ContentFolderEntry(_) if List("ArchiveFolderEntry", "ContentFolderEntry", "FileEntry", "MetadataFileEntry", "UnknownFileTypeEntry").contains(typeOfEntryAsString) =>
                          HierarchyLinkingError(idThatMightBelongToAnEntry, "The parentId is for an object of type 'ContentFolder'").invalidNel[Value]
                        case ArchiveFolderEntry(_) if List("AssetEntry", "FileEntry", "MetadataFileEntry", "UnknownFileTypeEntry").contains(typeOfEntryAsString) =>
                          HierarchyLinkingError(idThatMightBelongToAnEntry, "The parentId is for an object of type 'ArchiveFolder'").invalidNel[Value]
                        case _ => parentIdNel
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
  }

  private def checkIfAssetReferencesCorrectFiles(
      entryWithUpdatedParentId: Entry,
      assetId: String,
      entryTypesGrouped: Map[EntryTypeAndParent, List[(String, EntryTypeAndParent)]]
  ) = {
    val fileTypesAndTheirIds: Map[FieldName, List[FieldName]] =
      entryTypesGrouped.collect {
        // This assumes that there will only be one Asset, if it changes in future, 'Some(`assetId`)' (with backticks) needs to replace '_'
        case (FileEntry(Some(`assetId`)), ids)            => "expectedOriginalFiles" -> ids.map(_._1)
        case (MetadataFileEntry(Some(`assetId`)), ids)    => "expectedOriginalMetadataFiles" -> ids.map(_._1)
        case (UnknownFileTypeEntry(Some(`assetId`)), ids) => "unexpectedFilesWithNoCategory" -> ids.map(_._1)
      }
    val entryWithUpdatedOriginalFiles = verifyAssetHasCorrectFiles(assetId, entryWithUpdatedParentId, fileTypesAndTheirIds, "Files")
    val entryWithUpdatedOriginalMetadataFiles = verifyAssetHasCorrectFiles(assetId, entryWithUpdatedParentId, fileTypesAndTheirIds, "MetadataFiles")
    entryWithUpdatedParentId ++ entryWithUpdatedOriginalFiles ++ entryWithUpdatedOriginalMetadataFiles
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
          else HierarchyLinkingError(idsMissingFromFilesList.mkString(", "),
            s"There are files in the JSON that have the parentId of this asset ($assetId) but do not appear in '$filesFieldName'"
          ).invalidNel[Value]

        val idsInFilesListButNotInJsonPotentialError =
          if idsInFilesListButNotInJson.isEmpty then filesNel
          else
            val idsForUncategorisedFileType = allFileTypesAndTheirIds.getOrElse("unexpectedFilesWithNoCategory", Nil)
            val additionalErrorMessage =
              if idsForUncategorisedFileType.isEmpty then ""
              else
                s"\n\nIt's also possible that the files are in the JSON but whether they were $filesFieldName or not, could not be determined;\n\n" +
                s"these files are: $idsForUncategorisedFileType"
            HierarchyLinkingError(
              idsInFilesListButNotInJson.mkString(", "),
              s"There are files in this '$filesFieldName' that don't appear in the JSON or their parentId is not the same as this Asset's ('$assetId')"
            ).invalidNel[Value]

        val filesValidatedNel = idsMissingFromFilesListPotentialError.combine(idsInFilesListButNotInJsonPotentialError)

//        val idsMissingFromFilesListError =
//          if idsMissingFromFilesList.isEmpty then ""
//          else s"There are files in the JSON that have the parentId $assetId but do not appear in '$filesFieldName', they are: $idsMissingFromFilesList\n\n"
//        val idsInFilesListButNotInJsonError =
//          if idsInFilesListButNotInJson.isEmpty then ""
//          else s"There are files in this array that aren't in the JSON, they are: $idsInFilesListButNotInJson"
//        val filesErrorMessage = idsMissingFromFilesListError + idsInFilesListButNotInJsonError
//
//        val filesValidatedNel =
//          if filesErrorMessage.isEmpty then filesNel else HierarchyLinkingError(filesErrorMessage).invalidNel[Value]

        Map(filesFieldName -> filesValidatedNel)
      case invalidNel => Map(filesFieldName -> filesNel)
    }
  }

  def checkIfEntriesOfSameTypePointToSameParent(entryTypesGrouped: Map[EntryTypeAndParent, Map[String, EntryTypeAndParent]]) = {
    val entryLinkingErrors: List[Map[String, String]] = Nil
    val fileParentIds =
      entryTypesGrouped.collect {
        case (FileEntry(Some(parentId)), _)            => parentId
        case (MetadataFileEntry(Some(parentId)), _)    => parentId
        case (UnknownFileTypeEntry(Some(parentId)), _) => parentId
      }.toSet

    val numOfFileParentIds = fileParentIds.size
    val fileLinkingError =
      if numOfFileParentIds != 1 then
        entryLinkingErrors :+ Map(
          "ErrorType" -> "Files",
          "Error" -> s"Files should all be linked to one parent Asset but $numOfFileParentIds have been found: \n $fileParentIds"
        )
      else entryLinkingErrors

    val assetParentIds = entryTypesGrouped.collect { case (AssetEntry(Some(parentId)), _) => parentId }.toSet
    val numOfAssetParentIds = assetParentIds.size
    val assetLinkingError =
      if numOfAssetParentIds != 1 then
        fileLinkingError :+ Map(
          "ErrorType" -> "Assets",
          "Error" -> s"Assets should all be linked to one parent Folder but $numOfAssetParentIds have been found: \n $assetParentIds"
        )
      else fileLinkingError

    val folderIdsAndParent =
      entryTypesGrouped.toList.collect {
        case (ArchiveFolderEntry(potentialParentId), idsWithSameParent) => (idsWithSameParent.keys.toList, potentialParentId)
        case (ContentFolderEntry(potentialParentId), idsWithSameParent) => (idsWithSameParent.keys.toList, potentialParentId)
      }

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
  case class NoFileAtS3LocationError(errorMessage: String) extends ValidationError
  case class IdIsNotAUuidError(errorMessage: String) extends ValidationError
  case class IdIsNotUniqueError(errorMessage: String) extends ValidationError
  case class HierarchyLinkingError(valueThatCausedError: String, errorMessage: String) extends ValidationError
