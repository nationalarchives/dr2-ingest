package uk.gov.nationalarchives.ingestvalidategenericingestinputs.testUtils

import cats.data.*
import cats.implicits.*
import ujson.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.EntryType.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.EntryValidationError.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonValueValidator.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.ValidatedUtils.ValidatedEntry

object ExternalServicesTestUtils {
  val id: "id" = "id"
  val parentId: "parentId" = "parentId"
  val name: "name" = "name"
  val entryType: "type" = "type"
  val atLeastOnAssetAndFileErrorMessages: Map[String, String] =
    Map(
      "File" -> atLeastOneAssetAndFileErrorMessage("File"),
      "Asset" -> atLeastOneAssetAndFileErrorMessage("Asset")
    )
  private val rand = new scala.util.Random

  def testValidMetadataJson(newObjectsToAdd: List[Obj] = Nil): List[Obj] =
    List(
      Obj(
        "series" -> randomSeries,
        "id_Code" -> "idcode",
        "id_URI" -> "https://example.com/id/abcde/2023/1537",
        "id" -> "b7329714-4753-4bf5-a802-1c126bad1ad6",
        "parentId" -> Null,
        "title" -> Null,
        "type" -> "ArchiveFolder",
        "name" -> "https://example.com/id/abcde/2023/1537"
      ),
      Obj(
        "id_Code" -> "idcode2",
        "id_URI" -> "https://example.com/id/abcde/2023/1537",
        "id" -> "27354aa8-975f-48d1-af79-121b9a349cbe",
        "parentId" -> "b7329714-4753-4bf5-a802-1c126bad1ad6",
        "title" -> "folder title",
        "type" -> "ContentFolder",
        "name" -> "https://example.com/id/abcde/2023/1537"
      ),
      Obj(
        "originalFiles" -> Arr("b0147dea-878b-4a25-891f-66eba66194ca"),
        "originalMetadataFiles" -> Arr("d4f8613d-2d2a-420d-a729-700c841244f3"),
        "transferringBody" -> "tbody",
        "transferCompleteDatetime" -> "2024-01-01T12:38:41Z",
        "upstreamSystem" -> "TDD",
        "digitalAssetSource" -> "Asset Source",
        "digitalAssetSubtype" -> "SUBTYPE",
        "id_BornDigitalRef" -> "BDR",
        "id_ConsignmentReference" -> "sfsdf",
        "id_RecordID" -> "f5d6c25c-e586-4e63-a45b-9c175b095c48",
        "id" -> "b3bcfd9b-3fe6-41eb-8620-0cb3c40655d6",
        "parentId" -> "27354aa8-975f-48d1-af79-121b9a349cbe",
        "title" -> "fhfghfgh",
        "type" -> "Asset",
        "correlationId" -> "gxcvsdfkna",
        "description" -> ""
      ),
      Obj(
        "id" -> "b0147dea-878b-4a25-891f-66eba66194ca",
        "parentId" -> "b3bcfd9b-3fe6-41eb-8620-0cb3c40655d6",
        "title" -> "test name.docx",
        "type" -> "File",
        "name" -> "test name.docx",
        "sortOrder" -> 1,
        "fileSize" -> 15613,
        "representationType" -> "Preservation",
        "representationSuffix" -> 1,
        "location" -> "s3://test-source-bucket/b0147dea-878b-4a25-891f-66eba66194ca",
        "checksum_sha256" -> "ab41c540b192c7cd58d044527e2a849a6206fe95974910fe855bb92bc69c75a5",
        "metadataFile" -> false
      ),
      Obj(
        "id" -> "d4f8613d-2d2a-420d-a729-700c841244f3",
        "parentId" -> "b3bcfd9b-3fe6-41eb-8620-0cb3c40655d6",
        "title" -> "",
        "type" -> "File",
        "name" -> "TDD-2023-ABC-metadata.json",
        "sortOrder" -> 2,
        "fileSize" -> 1159,
        "representationType" -> "Preservation",
        "representationSuffix" -> 1,
        "location" -> "s3://test-source-bucket/d4f8613d-2d2a-420d-a729-700c841244f3",
        "checksum_sha256" -> "05fdca35f031b6d3246becd5888b2e2a538305fe48183fb3bf0dd6cdc7d6f7f5",
        "metadataFile" -> true
      )
    ) ::: newObjectsToAdd

  def randomSeries: String = {
    val oneToFourRandomLetters =
      (1 to rand.between(0, 4)).foldLeft("") { (letters, _) =>
        letters + rand.between(65, 90).toChar
      }

    if oneToFourRandomLetters.nonEmpty then s"$oneToFourRandomLetters ${rand.between(1, 10000)}" else "Unknown"
  }

  def convertAllUjsonObjsToSchemaValidatedMaps(entriesGroupedByType: Map[String, List[Obj]]): Map[String, List[Map[String, ValidatedNel[ValidationError, Value]]]] =
    entriesGroupedByType.map { case (entryType, entries) =>
      (entryType, entries.map(entry => convertUjsonObjToSchemaValidatedMap(entry)))
    }

  def convertUjsonObjToSchemaValidatedMap(entry: Obj): Map[String, ValidatedNel[ValidationError, Value]] =
    entry.obj.toMap.map { case (property, value) =>
      if (property == "type" && value.str == "UnknownType") then
        property -> SchemaValueError("UnknownType", """$.type: does not have a value in the enumeration ["ArchiveFolder", "ContentFolder", "Asset", "File"]""").invalidNel[Value]
      else property -> Validated.Valid(value)
    }

  def convertUjsonObjToGenericValidatedMap(entry: Obj): ValidatedEntry =
    entry.obj.toMap.map { case (property, value) => property -> Validated.Valid(value) }

  def generateListOfIdsAndEntries(allEntries: List[Obj] = testValidMetadataJson()): List[(String, EntryTypeAndParent)] =
    allEntries.map(entry => (entry(id).str, generateEntry(entry)))

  private def generateEntry(entry: Obj) = {
    val typeOfEntry = entry(entryType).str
    val potentialParentId = entry(parentId).strOpt
    typeOfEntry match {
      case "ArchiveFolder"                      => ArchiveFolderEntry(potentialParentId)
      case "ContentFolder"                      => ContentFolderEntry(potentialParentId)
      case "Asset"                              => AssetEntry(potentialParentId)
      case "File" if entry("metadataFile").bool => MetadataFileEntry(potentialParentId)
      case "File"                               => FileEntry(potentialParentId)
    }
  }

  def parentIdError(parentIdMessage: String = ""): Map[String, ValidatedNel[HierarchyLinkingError, Value]] =
    Map(parentId -> HierarchyLinkingError("null", s"The parentId value is 'null'$parentIdMessage").invalidNel[Value])

  def seriesError(seriesCaseClassName: String): ValidatedEntry =
    if seriesCaseClassName.isEmpty then Map()
    else if seriesCaseClassName == "SeriesExistsError" then Map("series" -> SeriesExistsError("A file can not have a Series").invalidNel[Value])
    else Map("series" -> SeriesDoesNotExistError("A series cannot be missing if parentId is 'null'").invalidNel[Value])

  def circularDependencyError(incorrectBreadcrumbTrail: List[String]): ValidatedNel[HierarchyLinkingError, Value] = {
    val parentThatReferencesChild = incorrectBreadcrumbTrail.dropRight(1).last
    HierarchyLinkingError(
      parentThatReferencesChild,
      s"Circular dependency! A parent entry (id '$parentThatReferencesChild') references this entry as its parentId.\n\n" +
        s"The breadcrumb trail looks like this ${incorrectBreadcrumbTrail.mkString(" > ")}"
    ).invalidNel[Value]
  }

  private def atLeastOneAssetAndFileErrorMessage(entryType: "File" | "Asset") =
    s"$$: must contain at least 1 element(s) that passes these validations: " +
      s"""{"title":"$entryType","description":"JSON must contain at least one object with all and only these properties, """ +
      s"""one of which is 'type': '$entryType'","properties":{"type":{"type":"string","const":"$entryType"}}}"""
}
