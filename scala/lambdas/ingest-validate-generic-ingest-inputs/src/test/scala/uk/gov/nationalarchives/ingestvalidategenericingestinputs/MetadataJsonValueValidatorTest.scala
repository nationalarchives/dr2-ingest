package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.data.ValidatedNel
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.*
import org.mockito.ArgumentMatchers.*
import org.mockito.Mockito.{times, verify, when}
import org.mockito.ArgumentMatchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.*
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.http.SdkHttpResponse
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import ujson.*
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonValueValidator.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.testUtils.ExternalServicesTestUtils.*

class MetadataJsonValueValidatorTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {
  val incorrectUrls: TableFor2[String, ValidatedNel[ValidationError, Value]] = Table(
    ("IncorrectUrl", "Expected Error"),
    ("AnIncorrectUrl", ValueError("location", "AnIncorrectUrl", "'location' could not be transformed into a URI").invalidNel[Value]),
    ("An IncorrectUrlWithASpace", UriIsNotValid("Illegal character in path at index 2: An IncorrectUrlWithASpace").invalidNel[Value]),
  )

  val entriesReferencingANonExistentParent: TableFor2[String, String] = Table(
    ("Entry referencing a non-existent parent", "Parent missing"),
    ("File", "Asset"),
    ("Asset", "ContentFolder"),
    ("ContentFolder", "ArchiveFolder")
  )

  val entriesReferencingTheWrongParentType: TableFor4[String, Map[Int, Int], String, Map[Int, EntryTypeAndParent]] = Table(
    ("Types with wrong parentId", "Indices of entries and Index of the wrong parentId", "Wrong parent type", "index and EntryType"),
    ("ArchiveFolder, ContentFolder, Asset and Files", Map(0 -> 4, 1 -> 4, 2 -> 4, 3 -> 4, 4 -> 3), "File", Map(4 -> MetadataFileEntry(None), 3 -> FileEntry(None))),
    ("ArchiveFolder, ContentFolder, Asset", Map(0 -> 2, 1 -> 2, 2 -> 2), "Asset", Map(2 -> AssetEntry(None))),
    ("ArchiveFolder and Files", Map(0 -> 1, 1 -> 1, 3 -> 1, 4 -> 1), "ContentFolder", Map(1 -> ContentFolderEntry(None))),
    ("Asset and Files", Map(0 -> 0, 1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0), "ArchiveFolder", Map(0 -> ArchiveFolderEntry(None)))
  )

  private val validator = new MetadataJsonValueValidator
  private val allEntries = testValidMetadataJson().map {
    entry => if entry(entryType).str != "ArchiveFolder" then Obj.from(entry.value.toMap - "series") else entry
  }

  private def mockS3Client(fileEntries: List[Obj], statusCode: Int=200, throwError: Boolean=false): DAS3Client[IO] = {
    val s3 = mock[DAS3Client[IO]]
    fileEntries.foreach { fileEntry =>
      val key = fileEntry(id).str
      lazy val response = SdkHttpResponse.builder().statusCode(statusCode).build()
      lazy val headObjectResponse = if throwError then IO(throw new Exception("Key could not be found")) else IO(HeadObjectResponse.builder().sdkHttpResponse(response).build)
      when(s3.headObject(ArgumentMatchers.eq("test-source-bucket"), ArgumentMatchers.eq(key)))
        .thenReturn(headObjectResponse)
    }

    s3
  }

  "checkFileIsInCorrectS3Location" should "return 0 s3 errors if the URI is in the correct format and 'headObject returns a valid response'" in {
    val fileEntries = allEntries.filter(_(entryType).str == "File")
    val s3Client = mockS3Client(fileEntries)
    val entriesAsValidatedMap = fileEntries.map(convertUjsonObjToSchemaValidatedMap)
    val validatedLocations = validator.checkFileIsInCorrectS3Location(s3Client, entriesAsValidatedMap).unsafeRunSync()

    val fileEntriesWithValidatedLocation = entriesAsValidatedMap.zip(validatedLocations).map{
      case (entryAsValidatedMap, validatedLocation) => entryAsValidatedMap + ("location" -> validatedLocation)
    }

    fileEntriesWithValidatedLocation should equal(entriesAsValidatedMap)
    verify(s3Client, times(entriesAsValidatedMap.length)).headObject(any[String], any[String])
  }

  "checkFileIsInCorrectS3Location" should "not check (validate) the location field if it already has an error on it" in {
    val fileEntries = allEntries.filter(_(entryType).str == "File")
    val s3Client = mock[DAS3Client[IO]]

    val entriesAsValidatedMap = fileEntries.map { entry =>
      convertUjsonObjToSchemaValidatedMap(entry) ++ Map(
        "location" -> MissingPropertyError("location", "$: required property 'location' not found").invalidNel[Value]
      )
    }
    val validatedLocations = validator.checkFileIsInCorrectS3Location(s3Client, entriesAsValidatedMap).unsafeRunSync()

    val fileEntriesWithValidatedLocation = entriesAsValidatedMap.zip(validatedLocations).map {
      case (entryAsValidatedMap, validatedLocation) => entryAsValidatedMap + ("location" -> validatedLocation)
    }

    fileEntriesWithValidatedLocation should equal(entriesAsValidatedMap)
    verify(s3Client, times(0)).headObject(any[String], any[String])
  }

  forAll(incorrectUrls) { (incorrectUrl, expectedError) =>
  "checkFileIsInCorrectS3Location" should "not call the s3 client and instead return an error if a URI could not be extracted " +
    s"from a location with a value of '$incorrectUrl'" in {
    val fileEntries = allEntries.collect{
      case entry if entry(entryType).str == "File" => Obj.from(entry.value ++ Map("location" -> Str(incorrectUrl)))
    }

    val s3Client = mock[DAS3Client[IO]]

    val entriesAsValidatedMap = fileEntries.map(convertUjsonObjToSchemaValidatedMap)
    val validatedLocations = validator.checkFileIsInCorrectS3Location(s3Client, entriesAsValidatedMap).unsafeRunSync()

    val fileEntriesWithValidatedLocation = entriesAsValidatedMap.zip(validatedLocations).map {
      case (entryAsValidatedMap, validatedLocation) => entryAsValidatedMap + ("location" -> validatedLocation)
    }

    val expectedEntriesAsValidatedMap = fileEntries.map { entry =>
      convertUjsonObjToSchemaValidatedMap(entry) ++ Map("location" -> expectedError )
    }

    fileEntriesWithValidatedLocation should equal(expectedEntriesAsValidatedMap)
    verify(s3Client, times(0)).headObject(any[String], any[String])
  }
  }

  "checkFileIsInCorrectS3Location" should "return a NoFileAtS3LocationError if the status code from the response of 'headObject' is not a 200" in {
    val fileEntries = allEntries.filter(_(entryType).str == "File")
    val s3Client = mockS3Client(fileEntries, 404)
    val entriesAsValidatedMap = fileEntries.map(convertUjsonObjToSchemaValidatedMap)
    val validatedLocations = validator.checkFileIsInCorrectS3Location(s3Client, entriesAsValidatedMap).unsafeRunSync()

    val fileEntriesWithValidatedLocation = entriesAsValidatedMap.zip(validatedLocations).map {
      case (entryAsValidatedMap, validatedLocation) => entryAsValidatedMap + ("location" -> validatedLocation)
    }

    val expectedFileEntriesWithValidatedLocation = entriesAsValidatedMap.zip(validatedLocations).map {
      case (entryAsValidatedMap, validatedLocation) =>
        entryAsValidatedMap + ("location" -> NoFileAtS3LocationError("Head Object request returned a Status code of 404").invalidNel[Value])
    }

    fileEntriesWithValidatedLocation should equal(expectedFileEntriesWithValidatedLocation)
    verify(s3Client, times(entriesAsValidatedMap.length)).headObject(any[String], any[String])
  }

  "checkFileIsInCorrectS3Location" should "return a NoFileAtS3LocationError if the call to 'headObject' yields an Exception" in {
    val fileEntries = allEntries.filter(_(entryType).str == "File")
    val s3Client = mockS3Client(fileEntries, throwError =  true)
    val entriesAsValidatedMap = fileEntries.map(convertUjsonObjToSchemaValidatedMap)
    val validatedLocations = validator.checkFileIsInCorrectS3Location(s3Client, entriesAsValidatedMap).unsafeRunSync()

    val fileEntriesWithValidatedLocation = entriesAsValidatedMap.zip(validatedLocations).map {
      case (entryAsValidatedMap, validatedLocation) => entryAsValidatedMap + ("location" -> validatedLocation)
    }

    val expectedFileEntriesWithValidatedLocation = entriesAsValidatedMap.map {
      entryAsValidatedMap => entryAsValidatedMap + ("location" ->
        NoFileAtS3LocationError("Key could not be found").invalidNel[Value])
    }

    fileEntriesWithValidatedLocation should equal(expectedFileEntriesWithValidatedLocation)
    verify(s3Client, times(entriesAsValidatedMap.length)).headObject(any[String], any[String])
  }

  "checkFileNamesHaveExtensions" should "return 0 MissingFileExtensionErrors if the value of the 'name' in the File entries, end with an extension" in {
    val fileEntries = allEntries.filter(_(entryType).str == "File")
    val entriesAsValidatedMap = fileEntries.map(convertUjsonObjToSchemaValidatedMap)
    val fileEntriesWithValidatedName = validator.checkFileNamesHaveExtensions(entriesAsValidatedMap)
    fileEntriesWithValidatedName should equal(entriesAsValidatedMap)
  }

  "checkFileNamesHaveExtensions" should "return a MissingFileExtensionError if the File entries' names, don't end with an extension" in {
    val fileEntries = allEntries.filter(_(entryType).str == "File")
    val fileEntriesWithExtsRemovedFromName = fileEntries.map { fileEntry =>
      val nameVal = fileEntry.value(name)
      val nameValWithExtensionRemoved = nameVal.str.split('.').dropRight(1).mkString
      Obj.from(fileEntry.value ++ Map(name -> Str(nameValWithExtensionRemoved)))
    }
    val entriesAsValidatedMap = fileEntriesWithExtsRemovedFromName.map(convertUjsonObjToSchemaValidatedMap)
    val fileEntriesWithValidatedName = validator.checkFileNamesHaveExtensions(entriesAsValidatedMap)
    val expectedEntriesAsValidatedMap = entriesAsValidatedMap.map {
      _.map { case (property, value) =>
        (
          property,
          if property == name then MissingFileExtensionError("The file name does not have an extension at the end of it").invalidNel[Value]
          else value
        )
      }
    }
    fileEntriesWithValidatedName should equal(expectedEntriesAsValidatedMap)
  }

  "checkFileNamesHaveExtensions" should "not check (validate) the name field if it already has an error on it" in {
    val fileEntries = allEntries.filter(_(entryType).str == "File")
    val fileEntriesWithExtsRemovedFromName = fileEntries.map { fileEntry =>
      val nameVal = fileEntry.value(name)
      val nameValWithExtensionRemoved = nameVal.str.split('.').dropRight(1).mkString
      Obj.from(fileEntry.value ++ Map(name -> Str(nameValWithExtensionRemoved)))
    }
    val entriesAsValidatedMap = fileEntriesWithExtsRemovedFromName.map { entry =>
      convertUjsonObjToSchemaValidatedMap(entry) ++ Map(
        name -> ValueError(name, "123", s"$$.$name: integer found, string expected").invalidNel[Value]
      )
    }
    val fileEntriesWithValidatedName = validator.checkFileNamesHaveExtensions(entriesAsValidatedMap)
    fileEntriesWithValidatedName should equal(entriesAsValidatedMap)
  }

  "checkIfAllIdsAreUuids" should "return 0 IdIsNotAUuidErrors if the value of the 'id' in the entries are UUIDs" in {
    val entriesGroupedByType = allEntries.groupBy(_(entryType).str)
    val allEntriesAsValidatedMaps = convertAllUjsonObjsToSchemaValidatedMaps(entriesGroupedByType)
    val entriesWithValidatedName = validator.checkIfAllIdsAreUuids(allEntriesAsValidatedMaps)
    entriesWithValidatedName should equal(allEntriesAsValidatedMaps)
  }

  "checkIfAllIdsAreUuids" should "return a IdIsNotAUuidError if the entries' ids, are not UUIDs" in {
    val entriesWithIncorrectIds = allEntries.map { entry =>
      Obj.from(entry.value ++ Map(id -> Str("notAUuid")))
    }
    val entriesGroupedByType = entriesWithIncorrectIds.groupBy(_(entryType).str)
    val allEntriesAsValidatedMaps = convertAllUjsonObjsToSchemaValidatedMaps(entriesGroupedByType)

    val fileEntriesWithValidatedIds = validator.checkIfAllIdsAreUuids(allEntriesAsValidatedMaps)

    val convertIdFieldToError = (property: String, value: ValidatedNel[ValidationError, Value]) =>
      if property == id then IdIsNotAUuidError("The id notAUuid is not a valid UUID").invalidNel[Value] else value
    val expectedEntriesAsValidatedMap = transformValuesInAllJsonObjects(allEntriesAsValidatedMaps, convertIdFieldToError)
    fileEntriesWithValidatedIds should equal(expectedEntriesAsValidatedMap)
  }

  "checkIfAllIdsAreUuids" should "not check (validate) the id fields if they already have an error in them" in {
    val entriesWithIncorrectIds = allEntries.map { entry =>
      Obj.from(entry.value ++ Map(id -> Str("notAUuid")))
    }
    val entriesGroupedByType = entriesWithIncorrectIds.groupBy(_(entryType).str)
    val entriesWhereIdsHaveError = entriesGroupedByType.map { case (entryType, entries) =>
      entryType ->
        entries.map { entry =>
          convertUjsonObjToSchemaValidatedMap(entry) ++ Map(
            id -> ValueError(id, "123", s"$$.$id: integer found, string expected").invalidNel[Value]
          )
        }
    }

    val fileEntriesWithValidatedName = validator.checkIfAllIdsAreUuids(entriesWhereIdsHaveError)
    fileEntriesWithValidatedName should equal(entriesWhereIdsHaveError)
  }

  "getIdsOfAllEntries" should "return the ids of the entries, grouped with the entry's type and it's parent" in {
    val entriesGroupedByType = allEntries.groupBy(_(entryType).str)
    val allEntriesAsValidatedMaps = convertAllUjsonObjsToSchemaValidatedMaps(entriesGroupedByType)
    val entriesGroupedById = validator.getIdsOfAllEntries(allEntriesAsValidatedMaps).sortBy(_._1)
    entriesGroupedById should equal(testAllEntryIds(allEntries).sortBy(_._1))
  }

  "getIdsOfAllEntries" should "assign a file to an 'UnknownFileType' if the name field has a error" in {
    val entriesWithIncorrectFileNames = allEntries.map { entry =>
      if entry(entryType).str == "File" then Obj.from(entry.value ++ Map(name -> Num(123))) else entry
    }
    val entriesGroupedByType = entriesWithIncorrectFileNames.groupBy(_(entryType).str)
    val entriesWhereIdsHaveError = entriesGroupedByType.map { case (entryType, entries) =>
      entryType ->
        entries.map { entry =>
          convertUjsonObjToSchemaValidatedMap(entry) ++ Map(
            name -> ValueError(name, "123", s"$$.$name: integer found, string expected").invalidNel[Value]
          )
        }
    }

    val fileEntriesWithValidatedName = validator.getIdsOfAllEntries(entriesWhereIdsHaveError).sortBy(_._1)
    fileEntriesWithValidatedName should equal(
      List(
        ("b7329714-4753-4bf5-a802-1c126bad1ad6", ArchiveFolderEntry(None)),
        ("27354aa8-975f-48d1-af79-121b9a349cbe", ContentFolderEntry(Some("b7329714-4753-4bf5-a802-1c126bad1ad6"))),
        ("b3bcfd9b-3fe6-41eb-8620-0cb3c40655d6", AssetEntry(Some("27354aa8-975f-48d1-af79-121b9a349cbe"))),
        ("d4f8613d-2d2a-420d-a729-700c841244f3", UnknownFileTypeEntry(Some("b3bcfd9b-3fe6-41eb-8620-0cb3c40655d6"))),
        ("b0147dea-878b-4a25-891f-66eba66194ca", UnknownFileTypeEntry(Some("b3bcfd9b-3fe6-41eb-8620-0cb3c40655d6")))
      ).sortBy(_._1)
    )
  }

  "getIdsOfAllEntries" should "not return ids if the id field has a error" in {
    val entriesWithIncorrectIds = allEntries.map { entry =>
      Obj.from(entry.value ++ Map(id -> Num(123)))
    }
    val entriesGroupedByType = entriesWithIncorrectIds.groupBy(_(entryType).str)
    val entriesWhereIdsHaveError = entriesGroupedByType.map { case (entryType, entries) =>
      entryType ->
        entries.map { entry =>
          convertUjsonObjToSchemaValidatedMap(entry) ++ Map(
            id -> ValueError(id, "123", s"$$.$id: integer found, string expected").invalidNel[Value]
          )
        }
    }

    val fileEntriesWithValidatedName = validator.getIdsOfAllEntries(entriesWhereIdsHaveError)
    fileEntriesWithValidatedName should equal(Nil)
  }

  "getIdsOfAllEntries" should "return the ids with an EntryType that has a parentId of 'Some(parentIdErrorMessage)', if " + 
    "the parentId field of each, has some sort of error prior" in {
    val entriesWithIncorrectParentIds = allEntries.map { entry =>
      Obj.from(entry.value ++ Map(parentId -> Num(123)))
    }
    val entriesGroupedByType = entriesWithIncorrectParentIds.groupBy(_(entryType).str)
    val entriesWhereIdsHaveError = entriesGroupedByType.map { case (entryType, entries) =>
      entryType ->
        entries.map { entry =>
          convertUjsonObjToSchemaValidatedMap(entry) ++ Map(
            parentId -> ValueError(parentId, "123", s"$$.$parentId: integer found, string expected").invalidNel[Value]
          )
        }
    }

    val fileEntriesWithValidatedName = validator.getIdsOfAllEntries(entriesWhereIdsHaveError).sortBy(_._1)
    val parentIdErrorMessage = Some("parentId undetermined, due to validation error")
    fileEntriesWithValidatedName should equal(
      List(
        ("b7329714-4753-4bf5-a802-1c126bad1ad6", ArchiveFolderEntry(parentIdErrorMessage)),
        ("27354aa8-975f-48d1-af79-121b9a349cbe", ContentFolderEntry(parentIdErrorMessage)),
        ("b3bcfd9b-3fe6-41eb-8620-0cb3c40655d6", AssetEntry(parentIdErrorMessage)),
        ("b0147dea-878b-4a25-891f-66eba66194ca", FileEntry(parentIdErrorMessage)),
        ("d4f8613d-2d2a-420d-a729-700c841244f3", MetadataFileEntry(parentIdErrorMessage))
      ).sortBy(_._1)
    )
  }

  "checkIfAllIdsAreUnique" should "return 0 IdIsNotUniqueErrors if the values of the 'id' field in the entries are all unique" in {
    val entriesGroupedByType = allEntries.groupBy(_(entryType).str)
    val allEntriesAsValidatedMaps = convertAllUjsonObjsToSchemaValidatedMaps(entriesGroupedByType)
    val entriesWithValidatedName = validator.checkIfAllIdsAreUnique(allEntriesAsValidatedMaps, testAllEntryIds(allEntries))
    entriesWithValidatedName should equal(allEntriesAsValidatedMaps)
  }

  "checkIfAllIdsAreUnique" should "return a IdIsNotUniqueError if any of the entries' ids, are not unique" in {
    val entriesWithIncorrectIds = allEntries.map { entry =>
      Obj.from(entry.value ++ Map(id -> Str("cbf14cb2-1cb3-43a4-8310-2ac295a130c5")))
    }
    val duplicateEntryIdsAndTypes = testAllEntryIds(entriesWithIncorrectIds)
    val entriesGroupedByType = entriesWithIncorrectIds.groupBy(_(entryType).str)
    val allEntriesAsValidatedMaps = convertAllUjsonObjsToSchemaValidatedMaps(entriesGroupedByType)

    val fileEntriesWithValidatedIds = validator.checkIfAllIdsAreUnique(allEntriesAsValidatedMaps, duplicateEntryIdsAndTypes)

    val convertIdFieldToError =
      (property: String, value: ValidatedNel[ValidationError, Value]) => if property == id then IdIsNotUniqueError("This id occurs 5 times").invalidNel[Value] else value
    val expectedEntriesAsValidatedMap = transformValuesInAllJsonObjects(allEntriesAsValidatedMaps, convertIdFieldToError)
    fileEntriesWithValidatedIds should equal(expectedEntriesAsValidatedMap)
  }

  "checkIfAllIdsAreUnique" should "not check (validate) the id fields if they already have an error in them" in {
    val entriesWithIncorrectIds = allEntries.map { entry =>
      Obj.from(entry.value ++ Map(id -> Str("cbf14cb2-1cb3-43a4-8310-2ac295a130c5")))
    }
    val entriesGroupedByType = entriesWithIncorrectIds.groupBy(_(entryType).str)
    val entriesWhereIdsHaveError = entriesGroupedByType.map { case (entryType, entries) =>
      entryType ->
        entries.map { entry =>
          convertUjsonObjToSchemaValidatedMap(entry) ++ Map(
            id -> ValueError(id, "123", s"$$.$id: integer found, string expected").invalidNel[Value]
          )
        }
    }

    val fileEntriesWithValidatedName = validator.checkIfAllIdsAreUuids(entriesWhereIdsHaveError)
    fileEntriesWithValidatedName should equal(entriesWhereIdsHaveError)
  }

  "checkIfEntriesHaveCorrectParentIds" should "return 0 HierarchyLinkingError if the values of the 'id' field in the entries are all unique" in {
    val entriesGroupedByType = allEntries.groupBy(_(entryType).str)
    val allEntriesAsValidatedMaps = convertAllUjsonObjsToSchemaValidatedMaps(entriesGroupedByType)
    val allEntryIds = testAllEntryIds(allEntries)
    val entryTypesGrouped = allEntryIds.groupBy { case (_, entryType) => entryType }
    val entriesWithValidatedParentId = validator.checkIfEntriesHaveCorrectParentIds(allEntriesAsValidatedMaps, allEntryIds.toMap, entryTypesGrouped)
    entriesWithValidatedParentId should equal(allEntriesAsValidatedMaps)
  }

  "checkIfEntriesHaveCorrectParentIds" should "return a HierarchyLinkingError if the parentIds of non-ArchiveFolders are null" in {
    val entriesWithIncorrectParentIds = allEntries.map(entry => Obj.from(entry.value ++ Map(parentId -> Null)))
    val entriesGroupedByType = entriesWithIncorrectParentIds.groupBy(_(entryType).str)
    val allEntriesAsValidatedMaps = convertAllUjsonObjsToSchemaValidatedMaps(entriesGroupedByType)

    val allEntryIds = testAllEntryIds(entriesWithIncorrectParentIds)
    val entryTypesGrouped = allEntryIds.groupBy { case (_, entryType) => entryType }
    val entriesWithValidatedParentId = validator.checkIfEntriesHaveCorrectParentIds(allEntriesAsValidatedMaps, allEntryIds.toMap, entryTypesGrouped)
    val entriesWhereParentIdHasErrors = allEntriesAsValidatedMaps.map { case (entryType, entries) =>
      entryType ->
        entries.map { entry =>
          val updatedParentId =
            if entryType == "ArchiveFolder" then Map()
            else Map(parentId -> HierarchyLinkingError("null", "The parentId value is 'null'").invalidNel[Value])

          val updatedFiles =
            if entryType == "Asset" then assetFilesErrorMessage("originalFiles") ++ assetFilesErrorMessage("originalMetadataFiles")
            else Map()

          entry ++ updatedParentId ++ updatedFiles
        }
    }
    entriesWithValidatedParentId should equal(entriesWhereParentIdHasErrors)
  }

  "checkIfEntriesHaveCorrectParentIds" should "not check (validate) the parentId fields if the parentId fields already " +
    "have errors in them (non-ArchiveFolders)" in {
      val entriesWithIncorrectIds = allEntries.map { entry =>
        Obj.from(entry.value ++ Map(parentId -> Str("cbf14cb2-1cb3-43a4-8310-2ac295a130c5")))
      }
      val entriesGroupedByType = entriesWithIncorrectIds.groupBy(_(entryType).str)
      val allEntriesAsValidatedMaps = entriesGroupedByType.map { case (entryType, entries) =>
        (entryType, entries.map(convertUjsonObjToSchemaValidatedMap))
      }
      val allEntriesWithParentIdsChangedToError = allEntriesAsValidatedMaps.map { case (entryType, entries) =>
        entryType ->
          entries.map { entry =>
            val updatedParentId =
              if "ArchiveFolder" == entryType then Map()
              else Map(parentId -> IdIsNotUniqueError("This id occurs 5 times").invalidNel[Value])

            val updatedFiles =
              if entryType == "Asset" then assetFilesErrorMessage("originalFiles") ++ assetFilesErrorMessage("originalMetadataFiles")
              else Map()

            entry ++ updatedParentId ++ updatedFiles
          }
      }

      // allEntryIds comes from 'getIdsOfAllEntries'; if parentIds failed a previous check, parentIds will now be 'None'
      val allEntryIdsWithParentsThatAreNone = testAllEntryIds(allEntries.map(entry => Obj.from(entry.value ++ Map(parentId -> Null))))
      val entryTypesGrouped = allEntryIdsWithParentsThatAreNone.groupBy { case (_, entryType) => entryType }
      val entriesWithValidatedParentId = validator.checkIfEntriesHaveCorrectParentIds(
        allEntriesWithParentIdsChangedToError,
        allEntryIdsWithParentsThatAreNone.toMap,
        entryTypesGrouped
      )
      entriesWithValidatedParentId should equal(allEntriesWithParentIdsChangedToError)
    }

  forAll(entriesReferencingANonExistentParent) { (entryWithoutParent, parentEntryTypeToRemove) =>
    "checkIfEntriesHaveCorrectParentIds" should s"return a HierarchyLinkingError if the parentId of a $entryWithoutParent doesn't exist in the JSON" in {
      val entriesWithoutSpecifiedEntry = allEntries.filterNot(_(entryType).str == parentEntryTypeToRemove)
      val entriesGroupedByType = entriesWithoutSpecifiedEntry.groupBy(_(entryType).str)
      val allEntriesAsValidatedMaps = entriesGroupedByType.map { case (entryType, entries) =>
        (entryType, entries.map(convertUjsonObjToSchemaValidatedMap))
      }

      val allEntryIds = testAllEntryIds().filterNot { case (id, entryType) => entryType.getClass.getSimpleName == parentEntryTypeToRemove + "Entry" }

      val entryTypesGrouped = allEntryIds.groupBy { case (_, entryType) => entryType }
      val entriesWithValidatedParentId = validator.checkIfEntriesHaveCorrectParentIds(allEntriesAsValidatedMaps, allEntryIds.toMap, entryTypesGrouped)
      val entriesWhereParentIdHasErrors = entriesGroupedByType.map { case (entryType, entries) =>
        entryType ->
          entries.map { entry =>
            convertUjsonObjToSchemaValidatedMap(entry) ++ (
              if entryWithoutParent == entryType then
                Map(parentId -> HierarchyLinkingError(entry(parentId).str, "The object that this parentId refers to can not be found in the JSON").invalidNel[Value])
              else Map()
            )
          }
      }
      entriesWithValidatedParentId should equal(entriesWhereParentIdHasErrors)
    }
  }

  forAll(entriesReferencingTheWrongParentType) { (typesWithWrongParentId, indicesOfEntriesAndWrongParentIndex, wrongEntryType, indexAndEntryType) =>
    "checkIfEntriesHaveCorrectParentIds" should s"return a HierarchyLinkingError if the parentId of $typesWithWrongParentId is of type $wrongEntryType" in {
      val entriesWithoutIncorrectParentType = allEntries.zipWithIndex.map { (entry, index) =>
        val potentialIndexOfWrongParent = indicesOfEntriesAndWrongParentIndex.get(index)
        potentialIndexOfWrongParent.map(indexOfWrongParent => Obj.from(entry.value ++ Map(parentId -> allEntries(indexOfWrongParent)(id)))).getOrElse(entry)
      }
      val entriesGroupedByType = entriesWithoutIncorrectParentType.groupBy(_(entryType).str)
      val allEntriesAsValidatedMaps = entriesGroupedByType.map { case (entryType, entries) =>
        (entryType, entries.map(convertUjsonObjToSchemaValidatedMap))
      }

      val allEntryIds = testAllEntryIds(entriesWithoutIncorrectParentType)
      val entryTypesGrouped = allEntryIds.groupBy { case (_, entryType) => entryType }
      val entriesWithValidatedParentId = validator.checkIfEntriesHaveCorrectParentIds(allEntriesAsValidatedMaps, allEntryIds.toMap, entryTypesGrouped)
      val entriesWhereParentIdHasErrors = entriesGroupedByType.map { case (entryType, entries) =>
        entryType ->
          entries.map { entry =>
            val updatedParentId =
              if typesWithWrongParentId.contains(entryType) then
                Map(parentId -> HierarchyLinkingError(entry(parentId).str, s"The parentId is for an object of type '$wrongEntryType'").invalidNel[Value])
              else Map()

            val updatedFiles =
              if entryType == "Asset" && typesWithWrongParentId.contains("File") then assetFilesErrorMessage("originalFiles") ++ assetFilesErrorMessage("originalMetadataFiles")
              else Map()
            convertUjsonObjToSchemaValidatedMap(entry) ++ updatedParentId ++ updatedFiles
          }
      }

      entriesWithValidatedParentId.toList.sortBy(_._1) should equal(entriesWhereParentIdHasErrors.toList.sortBy(_._1))
    }
  }

  "checkIfEntriesHaveCorrectParentIds" should "return a HierarchyLinkingError if originalFiles and originalMetadataFiles do not have any files in them" in {
    val entriesWithAssetWithNoFiles = allEntries.map { entry =>
      if entry(entryType).str == "Asset" then Obj.from(entry.value ++ Map("originalFiles" -> Arr(), "originalMetadataFiles" -> Arr())) else entry
    }
    val entriesGroupedByType = entriesWithAssetWithNoFiles.groupBy(_(entryType).str)
    val allEntriesAsValidatedMaps = convertAllUjsonObjsToSchemaValidatedMaps(entriesGroupedByType)
    val allEntryIds = testAllEntryIds(entriesWithAssetWithNoFiles)
    val entryTypesGrouped = allEntryIds.groupBy { case (_, entryType) => entryType }

    val entriesWithValidatedName = validator.checkIfEntriesHaveCorrectParentIds(allEntriesAsValidatedMaps, allEntryIds.toMap, entryTypesGrouped)
    val entriesWhereAssetFilesHaveErrors = entriesGroupedByType.map { case (entryType, entries) =>
      entryType ->
        entries.map { entry =>
          convertUjsonObjToSchemaValidatedMap(entry) ++ (
            if entryType == "Asset" then
              Map(
                "originalFiles" -> HierarchyLinkingError(
                  "b0147dea-878b-4a25-891f-66eba66194ca",
                  s"There are files in the JSON that have the parentId of this Asset (b3bcfd9b-3fe6-41eb-8620-0cb3c40655d6) but do not appear in 'originalFiles'"
                ).invalidNel[Value],
                "originalMetadataFiles" -> HierarchyLinkingError(
                  "d4f8613d-2d2a-420d-a729-700c841244f3",
                  s"There are files in the JSON that have the parentId of this Asset (b3bcfd9b-3fe6-41eb-8620-0cb3c40655d6) but do not appear in 'originalMetadataFiles'"
                ).invalidNel[Value]
              )
            else Map()
          )
        }
    }
    entriesWithValidatedName.toList.sortBy(_._1) should equal(entriesWhereAssetFilesHaveErrors.toList.sortBy(_._1))
  }

  "checkIfEntriesHaveCorrectParentIds" should "return a HierarchyLinkingError if the files in originalFiles and originalMetadataFiles do not exist in the JSON" in {
    val entriesWithoutFiles = allEntries.filterNot(_(entryType).str == "File")

    val entriesGroupedByType = entriesWithoutFiles.groupBy(_(entryType).str)
    val allEntriesAsValidatedMaps = convertAllUjsonObjsToSchemaValidatedMaps(entriesGroupedByType)
    val allEntryIds = testAllEntryIds().dropRight(2)
    val entryTypesGrouped = allEntryIds.groupBy { case (_, entryType) => entryType }

    val entriesWithValidatedName = validator.checkIfEntriesHaveCorrectParentIds(allEntriesAsValidatedMaps, allEntryIds.toMap, entryTypesGrouped)
    val entriesWhereAssetFilesHaveErrors = entriesGroupedByType.map { case (entryType, entries) =>
      entryType ->
        entries.map { entry =>
          convertUjsonObjToSchemaValidatedMap(entry) ++ (
            if entryType == "Asset" then assetFilesErrorMessage("originalFiles") ++ assetFilesErrorMessage("originalMetadataFiles")
            else Map()
          )
        }
    }
    entriesWithValidatedName.toList.sortBy(_._1) should equal(entriesWhereAssetFilesHaveErrors.toList.sortBy(_._1))
  }

  "checkIfEntriesHaveCorrectParentIds" should "return a HierarchyLinkingError if the files referred to in originalFiles and originalMetadataFiles, " +
    "don't have an extension, and therefore, it could not be established whether they belong to originalFiles or originalMetadataFiles" in {
    val entriesWithFilesWithNoExtensions = allEntries.map { entry =>
      if entry(entryType).str == "File" then Obj.from(entry.value ++ Map(name -> Str(entry(name).str.drop(5)))) else entry
    }

    val entriesGroupedByType = entriesWithFilesWithNoExtensions.groupBy(_(entryType).str)
    val allEntriesAsValidatedMaps = convertAllUjsonObjsToSchemaValidatedMaps(entriesGroupedByType)
    val allEntryIds = testAllEntryIds(entriesWithFilesWithNoExtensions).map { case (id, entryTypeAndParent) =>
      id -> (
        if entryTypeAndParent.getClass.getSimpleName.endsWith("FileEntry") then UnknownFileTypeEntry(entryTypeAndParent.potentialParentId)
        else entryTypeAndParent
      )
    }
    val entryTypesGrouped = allEntryIds.groupBy { case (_, entryType) => entryType }

    val entriesWithValidatedName = validator.checkIfEntriesHaveCorrectParentIds(allEntriesAsValidatedMaps, allEntryIds.toMap, entryTypesGrouped)
    val entriesWhereAssetFilesHaveErrors = entriesGroupedByType.map { case (entryType, entries) =>
      entryType ->
        entries.map { entry =>
          convertUjsonObjToSchemaValidatedMap(entry) ++ (
            if entryType == "Asset" then
              assetFilesErrorMessage("originalFiles", true) ++
                assetFilesErrorMessage("originalMetadataFiles", true)
            else Map()
          )
        }
    }
    entriesWithValidatedName.toList.sortBy(_._1) should equal(entriesWhereAssetFilesHaveErrors.toList.sortBy(_._1))
  }

  "checkIfEntriesHaveCorrectParentIds" should "not check (validate) the originalFiles nor originalMetadata fields if the fields already have errors in them" in {
    val entriesGroupedByType = allEntries.groupBy(_(entryType).str)
    val allEntriesAsValidatedMaps = entriesGroupedByType.map { case (entryType, entries) =>
      entryType ->
        entries.map { entry =>
          convertUjsonObjToSchemaValidatedMap(entry) ++ (
            if entryType == "Asset" then
              Map(
                "originalFiles" -> ValueError("originalFiles", "true", "$.originalFiles: boolean found, array expected").invalidNel[Value],
                "originalMetadataFiles" -> ValueError("originalMetadataFiles", "true", "$.originalMetadataFiles: boolean found, array expected").invalidNel[Value]
              )
            else Map()
          )
        }
    }
    val allEntryIds = testAllEntryIds()
    val entryTypesGrouped = allEntryIds.groupBy { case (_, entryType) => entryType }
    val entriesWithValidatedName = validator.checkIfEntriesHaveCorrectParentIds(allEntriesAsValidatedMaps, allEntryIds.toMap, entryTypesGrouped)

    entriesWithValidatedName should equal(allEntriesAsValidatedMaps)
  }

  "checkIfEntriesHaveCorrectParentIds" should "not check (validate) the parentId fields if the id fields already have errors in them" in {
    val entriesWithIncorrectIds = allEntries.map { entry =>
      Obj.from(entry.value ++ Map(id -> Str("cbf14cb2-1cb3-43a4-8310-2ac295a130c5")))
    }
    val entriesGroupedByType = entriesWithIncorrectIds.groupBy(_(entryType).str)
    val allEntriesAsValidatedMaps = entriesGroupedByType.map { case (entryType, entries) =>
      entryType ->
        entries.map { entry =>
          convertUjsonObjToSchemaValidatedMap(entry) ++ Map(id -> IdIsNotUniqueError("This id occurs 5 times").invalidNel[Value])
        }
    }
    val allEntryIds = testAllEntryIds(entriesWithIncorrectIds)
    val entryTypesGrouped = allEntryIds.groupBy { case (_, entryType) => entryType }
    val entriesWithValidatedName = validator.checkIfEntriesHaveCorrectParentIds(allEntriesAsValidatedMaps, allEntryIds.toMap, entryTypesGrouped)

    entriesWithValidatedName should equal(allEntriesAsValidatedMaps)
  }

  private def transformValuesInAllJsonObjects(
      entriesGroupedByType: Map[String, List[Entry]],
      valueTransformer: (String, ValidatedNel[ValidationError, Value]) => ValidatedNel[ValidationError, Value]
  ) = entriesGroupedByType.map { case (entryType, entries) =>
    entryType -> entries.map(_.map { case (property, value) => (property, valueTransformer(property, value)) })
  }

  private def assetFilesErrorMessage(files: String, addUncategorisedFileErrorMessage: Boolean = false) = {
    val getfileIds = Map("originalFiles" -> "b0147dea-878b-4a25-891f-66eba66194ca", "originalMetadataFiles" -> "d4f8613d-2d2a-420d-a729-700c841244f3")
    val uncategorisedFileErrorMessage =
      if addUncategorisedFileErrorMessage then
        s"\n\nIt's also possible that the files are in the JSON but whether they were $files or not, could not be determined, " +
          s"these files are: ${getfileIds.values.toList}"
      else ""
    Map(
      files -> HierarchyLinkingError(
        getfileIds(files),
        s"There are files in this '$files' that don't appear in the JSON or their parentId is not the same as this Asset's ('b3bcfd9b-3fe6-41eb-8620-0cb3c40655d6')" +
          uncategorisedFileErrorMessage
      ).invalidNel[Value]
    )
  }
}
