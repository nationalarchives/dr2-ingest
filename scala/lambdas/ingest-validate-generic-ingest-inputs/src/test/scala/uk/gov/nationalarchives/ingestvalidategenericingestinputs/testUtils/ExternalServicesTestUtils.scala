package uk.gov.nationalarchives.ingestvalidategenericingestinputs.testUtils

import cats.data.*
import ujson.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.SchemaValidationError

object ExternalServicesTestUtils {
  private val rand = new scala.util.Random
  val id: "id" = "id"
  val parentId: "parentId" = "parentId"
  val name: "name" = "name"
  val entryType: "type" = "type"

  def randomSeries: String = {
    val oneToFourRandomLetters =
      (1 to rand.between(0, 4)).foldLeft("") { (letters, _) =>
        letters + rand.between(65, 90).toChar
      }

    if oneToFourRandomLetters.nonEmpty then s"$oneToFourRandomLetters ${rand.between(1, 10000)}" else "Unknown"
  }

  def testValidMetadataJson(incorrectValue: String = "default"): List[Obj] =
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
        "name" -> "b3bcfd9b-3fe6-41eb-8620-0cb3c40655d6"
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
        "checksum_sha256" -> "ab41c540b192c7cd58d044527e2a849a6206fe95974910fe855bb92bc69c75a5"
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
        "checksum_sha256" -> "05fdca35f031b6d3246becd5888b2e2a538305fe48183fb3bf0dd6cdc7d6f7f5"
      )
    )

  def testAllEntryIds(allEntries: List[Obj] = testValidMetadataJson()): List[(String, EntryTypeAndParent)] = {
    val topFolder = allEntries.head
    val contentFolder = allEntries(1)
    val asset = allEntries(2)
    val file = allEntries(3)
    val metadataFile = allEntries(4)
    List(
      (topFolder(id).str, ArchiveFolderEntry(topFolder(parentId).strOpt)),
      (contentFolder(id).str, ContentFolderEntry(contentFolder(parentId).strOpt)),
      (asset(id).str, AssetEntry(asset(parentId).strOpt)),
      (file(id).str, FileEntry(file(parentId).strOpt)),
      (metadataFile(id).str, MetadataFileEntry(metadataFile(parentId).strOpt))
    )
  }

  val atLeastOnAssetAndFileErrorMessages: Map[String, String] =
    Map(
      "File" -> atLeastOneAssetAndFileErrorMessage("File"),
      "Asset" -> atLeastOneAssetAndFileErrorMessage("Asset")
    )

  def convertUjsonObjToSchemaValidatedMap(entry: Obj): Map[String, ValidatedNel[SchemaValidationError, Value]] =
    entry.obj.toMap.map { case (property, value) => property -> Validated.Valid(value) }

  def convertAllUjsonObjsToSchemaValidatedMaps(entriesGroupedByType: Map[String, List[Obj]]) =
    entriesGroupedByType.map { case (entryType, entries) =>
      (entryType, entries.map(entry => convertUjsonObjToSchemaValidatedMap(entry)))
    }

  def convertUjsonObjToGenericValidatedMap(entry: Obj): Map[String, ValidatedNel[ValidationError, Value]] =
    entry.obj.toMap.map { case (property, value) => property -> Validated.Valid(value) }

  private def atLeastOneAssetAndFileErrorMessage(entryType: "File" | "Asset") = {
    val exactlyOrAtLeastWording = Map("File" -> "at least", "Asset" -> "exactly")
    s"$$: must contain at least 1 element(s) that passes these validations: " +
      s"""{"title":"$entryType","description":"JSON must contain ${exactlyOrAtLeastWording(entryType)} one object with all and only these properties, """ +
      s"""one of which is 'type': '$entryType'","properties":{"type":{"type":"string","const":"$entryType"}}}"""
  }
}
