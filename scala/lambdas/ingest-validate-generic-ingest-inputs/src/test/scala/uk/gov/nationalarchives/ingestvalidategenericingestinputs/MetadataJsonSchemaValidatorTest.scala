package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.data.*
import cats.effect.unsafe.implicits.global
import cats.implicits.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.*
import org.scalatestplus.mockito.MockitoSugar
import ujson.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.EntryValidationError.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.MetadataJsonSchemaValidator.EntryTypeSchema.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.testUtils.ExternalServicesTestUtils.*

class MetadataJsonSchemaValidatorTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {
  private val entryTypeSchemas: TableFor1[EntryTypeSchema] = Table("entryType", ArchiveFolder, ContentFolder, Asset, File, UnknownType)
  private lazy val unknownTypeEntry = List(Obj.from(testValidMetadataJson().head.value ++ Map("type" -> Str("UnknownType"))))

  val propertiesWithArrayType: TableFor4[String, String, Value, SchemaValidationEntryError] = Table(
    ("entryField", "Expected array type", "Incorrect array value", "Expected error message"),
    ("originalFiles", "String", Bool(true), ValueError("originalFiles", "true", "$.originalFiles: boolean found, array expected")),
    ("originalMetadataFiles", "String", Num(123), ValueError("originalMetadataFiles", "123", "$.originalMetadataFiles: integer found, array expected"))
  )

  private val minimumTypesAllowedInJson: TableFor2[String, List[String]] = Table(
    ("title", "Types to remove"),
    ("contains a file but not an Asset", List("Asset")),
    ("contains an Asset but not a File", List("File")),
    ("does not contain an Asset nor a File", List("File", "Asset"))
  )

  private val someValidSeries: TableFor1[Value] = Table(
    "Some Valid Series",
    Str("A 1"),
    Str("U 10"),
    Str("M 110"),
    Str("Y 1100"),
    Str("AB 1"),
    Str("BC 10"),
    Str("CD 110"),
    Str("DE 1100"),
    Str("ABC 1"),
    Str("BAA 25"),
    Str("CAJ 378"),
    Str("DZB 3793"),
    Str("WQYZ 2"),
    Str("JFYE 98"),
    Str("XVFA 921"),
    Str("KFLS 7524"),
    Str("Unknown")
  )

  private val someInvalidSeries: TableFor1[Value] = Table(
    "Some (valid-like) Invalid Series",
    Str("A"),
    Str("abc 1"),
    Str("A1"),
    Str("1"),
    Str("1KFL 7524"),
    Str("XVFAE 9212"),
    Str("DZB 0793"),
    Str("WQYZ 28576"),
    Str("JFYE-98"),
    Str(" DE 1100"),
    Str("ABC 1 "),
    Str("unknown"),
    Str(" Unknown"),
    Str("Unknown ")
  )

  private val invalidSha256Checksums: TableFor2[Value, String] = Table(
    ("hash", "At most/least 64 characters long"),
    (Str("ab41c540b192c7cd58d044527e2a849a6206fe95974910fe855bb92bc69c75a5b"), "most"),
    (Str("ab41c540b192c7cd58d044527e2a849a6206fe95974910fe855bb92bc69c75a"), "least")
  )

  private val nonSha256ChecksumsShorterThan32Chars: TableFor2[String, Value] = Table(
    ("checksum_", "hash shorter than 32 chars"),
    ("checksum_shaMD5", Str("9e107d9d372bb6826bd81d3542a419d")),
    ("checksum_sha1", Str("2fd4e1c67a2d28fced849ee1bb76e73"))
  )

  private val incorrectSeriesAndParentPermutations: TableFor2[String, List[Map[? >: String <: String, Str | Null.type | Value]]] = Table(
    ("Series and null parent state", "Items to add to the entries"),
    ("only one entry with a series and none with a null parent", List(Map("series" -> Str(randomSeries)), Map(), Map(), Map(), Map())),
    ("only one entry with a null parent and none with a series", List(Map(), Map("parentId" -> Null), Map(), Map(), Map())),
    ("only one entry with a series and another one with a null parent", List(Map("series" -> Str(randomSeries)), Map("parentId" -> Null), Map(), Map(), Map()))
  )

  private lazy val entriesWithoutASeriesOrNullParent = testValidMetadataJson().map { entry =>
    val entryWithoutASeries = Obj.from(entry.value.toMap - "series")
    if entry(entryType).str == "ArchiveFolder" then Obj.from(entryWithoutASeries.value.toMap ++ Map("parentId" -> Str("a499be5a-26b9-45ec-9f8e-2b81fd21dbf8")))
    else entryWithoutASeries
  }

  "checkJsonForAtLeastOneEntryWithSeriesAndNullParent" should "return 0 errors if the JSON contains at least one entry with a series and a parentId with a null value" in {
    val jsonString = convertUjsonToString(testValidMetadataJson())
    val errors = MetadataJsonSchemaValidator.checkJsonForAtLeastOneEntryWithSeriesAndNullParent(jsonString).unsafeRunSync()
    errors should equal(Nil)
  }

  forAll(incorrectSeriesAndParentPermutations) { case (incorrectSeriesAndNullParentState, itemsToAddToEntries) =>
    "checkJsonForAtLeastOneEntryWithSeriesAndNullParent" should s"return an error if the JSON contains $incorrectSeriesAndNullParentState" in {
      val entries = entriesWithoutASeriesOrNullParent.zip(itemsToAddToEntries).map { case (entry, itemToAddToEntry) =>
        Obj.from(entry.value.toMap ++ itemToAddToEntry)
      }
      val jsonString = convertUjsonToString(entries)
      val errors = MetadataJsonSchemaValidator.checkJsonForAtLeastOneEntryWithSeriesAndNullParent(jsonString).unsafeRunSync()
      errors should equal(
        List(
          AtLeastOneEntryWithSeriesAndNullParentError(
            "$: must contain at least 1 element(s) that passes these validations: " +
              """{"title":"At least one top-level entry in the JSON","description":"There must be at least one object in the JSON that has both a series and a parent that is null",""" +
              """"properties":{"parentId":{"type":"null","const":null},"series":true},"required":["series","parentId"]}"""
          ).invalidNel[Value]
        )
      )
    }
  }

  "checkJsonForMinimumObjects" should "return 0 errors if the JSON contains at least one Asset and one File" in {
    val jsonString = convertUjsonToString(testValidMetadataJson())
    val errors = MetadataJsonSchemaValidator.checkJsonForMinimumObjects(jsonString).unsafeRunSync()
    errors should equal(Nil)
  }

  forAll(minimumTypesAllowedInJson) { case (entryTypeTest, typesToRemove) =>
    val numOfEntriesToRemove = typesToRemove.length
    val s = if numOfEntriesToRemove > 1 then "s" else ""
    "checkJsonForMinimumObjects" should s"should return $numOfEntriesToRemove error message$s if the JSON $entryTypeTest" in {
      val jsonWithoutSpecifiedType = testValidMetadataJson().filterNot(entry => typesToRemove.contains(entry(entryType).str))
      val jsonString = convertUjsonToString(jsonWithoutSpecifiedType)
      val validatedNelErrors = MetadataJsonSchemaValidator.checkJsonForMinimumObjects(jsonString).unsafeRunSync()
      val errorMessages = validatedNelErrors.map(_.swap.map(_.head.errorMessage).getOrElse(""))
      errorMessages should equal(typesToRemove.map(atLeastOnAssetAndFileErrorMessages))
    }
  }

  forAll(entryTypeSchemas) { entryTypeSchema =>
    "validateMetadataJsonObject" should s", for an entry of type '$entryTypeSchema', return the JSON object as a Map with all " +
      " properties as 'Valid' if all properties are present and the values are valid" in {
        val entry = testValidMetadataJson(unknownTypeEntry).filter(_(entryType).str == entryTypeSchema.toString).head
        val validatedJsonObjectAsMap = MetadataJsonSchemaValidator(entryTypeSchema).validateMetadataJsonObject(entry).unsafeRunSync()
        validatedJsonObjectAsMap should equal(convertUjsonObjToSchemaValidatedMap(entry))
      }
  }

  forAll(entryTypeSchemas) { entryTypeSchema =>
    "validateMetadataJsonObject" should s", for an entry of type '$entryTypeSchema', return the JSON object as a Map with all " +
      "properties as 'Valid' if additional properties are present, so long as all properties are present and the values are valid" in {
        val entry = testValidMetadataJson(unknownTypeEntry).filter(_(entryType).str == entryTypeSchema.toString).head
        val entryWithAdditionalProperty = Obj.from(entry.value ++ Map("additionalProperty" -> Str("additionalPropertyVal")))
        val validatedJsonObjectAsMap = MetadataJsonSchemaValidator(entryTypeSchema).validateMetadataJsonObject(entryWithAdditionalProperty).unsafeRunSync()
        validatedJsonObjectAsMap should equal(convertUjsonObjToSchemaValidatedMap(entryWithAdditionalProperty))
      }
  }

  forAll(entryTypeSchemas) { entryTypeSchema =>
    "validateMetadataJsonObject" should s", for an entry of type '$entryTypeSchema', return ValueErrors for all of the additional " +
      "properties that are empty" in {
        val entry = testValidMetadataJson(unknownTypeEntry).filter(_(entryType).str == entryTypeSchema.toString).head
        val entryWithAdditionalProperty = Obj.from(entry.value ++ Map("additionalProperty" -> Str(""), "additionalProperty2" -> Arr()))
        val validatedJsonObjectAsMap = MetadataJsonSchemaValidator(entryTypeSchema).validateMetadataJsonObject(entryWithAdditionalProperty).unsafeRunSync()
        validatedJsonObjectAsMap should equal(
          convertUjsonObjToSchemaValidatedMap(entryWithAdditionalProperty) ++ Map(
            "additionalProperty" -> ValueError("additionalProperty", "", "$.additionalProperty: must be at least 1 characters long").invalidNel[Value],
            "additionalProperty2" -> ValueError("additionalProperty2", "", "$.additionalProperty2: must have at least 1 items but found 0").invalidNel[Value]
          )
        )
      }
  }

  forAll(entryTypeSchemas) { entryTypeSchema =>
    "validateMetadataJsonObject" should s", for an entry of type '$entryTypeSchema', return ValueErrors for all of the additional " +
      "property names that don't contain at least one ASCII letter or number" in {
        val entry = testValidMetadataJson(unknownTypeEntry).filter(_(entryType).str == entryTypeSchema.toString).head
        val entryWithAdditionalProperty = Obj.from(entry.value ++ Map("!$%^&*()@~'#,./<>?" -> Str("symbols"), "āďđìťîōņàĺƤŗõƥēŕƭŷ²" -> Arr("non-ascii letter and number")))
        val validatedJsonObjectAsMap = MetadataJsonSchemaValidator(entryTypeSchema).validateMetadataJsonObject(entryWithAdditionalProperty).unsafeRunSync()
        validatedJsonObjectAsMap should equal(
          convertUjsonObjToSchemaValidatedMap(entryWithAdditionalProperty) ++ Map(
            "āďđìťîōņàĺƤŗõƥēŕƭŷ²" -> MissingPropertyError(
              "āďđìťîōņàĺƤŗõƥēŕƭŷ²",
              "$: property 'āďđìťîōņàĺƤŗõƥēŕƭŷ²' name is not valid: does not match the regex pattern [A-Za-z0-9]"
            ).invalidNel[Value],
            "!$%^&*()@~'#,./<>?" -> MissingPropertyError("!$%^&*()@~'#,./<>?", "$: property '!$%^&*()@~'#,./<>?' name is not valid: does not match the regex pattern [A-Za-z0-9]")
              .invalidNel[Value]
          )
        )
      }
  }

  forAll(entryTypeSchemas.filter(_.toString != "UnknownType")) { entryTypeSchema =>
    "validateMetadataJsonObject" should s", for an entry of type '$entryTypeSchema', return ValueErrors for all of the properties " +
      "with incorrect types and multiple ValueErrors for properties with multiple criteria" in {
        val schemaType = entryTypeSchema.toString
        val entry = testValidMetadataJson().filter(_(entryType).str == schemaType).head
        val entriesWithWrongTypes = Obj.from(
          entry.value.map { case (name, value) =>
            val wrongType = name match {
              case "sortOrder" | "fileSize" | "representationSuffix" | "originalFiles" | "originalMetadataFiles" => Str("stringInsteadOfInt")
              case _                                                                                             => Num(123)
            }
            name -> wrongType
          }
        )
        val validatedJsonObjectAsMap = MetadataJsonSchemaValidator(entryTypeSchema).validateMetadataJsonObject(entriesWithWrongTypes).unsafeRunSync()
        val expectedValidatedJsonObject = validatedJsonObjectWithAllValueErrors(entry, schemaType)
        val typePropertyErrors = validatedJsonObjectAsMap("type")

        validatedJsonObjectAsMap should equal(sortJsonObjectByFieldName(expectedValidatedJsonObject))
        typePropertyErrors.swap.toList.flatMap(_.toList).length should equal(2)
      }
  }

  forAll(propertiesWithArrayType) { (arrayTypeField, expectedType, incorrectValue, expectedErrorMessage) =>
    "validateMetadataJsonObject" should s", for an entry with an array type like '$arrayTypeField', return a 'required property key not found' " +
      "error message if the type is an array but the inner type is of an unexpected type" in {
        val entryWithArrayTypes = testValidMetadataJson(unknownTypeEntry).filter(_(entryType).str == "Asset").head
        val entryWithInnerTypeChanged = Obj.from(entryWithArrayTypes.value ++ Map(arrayTypeField -> incorrectValue))
        val validatedJsonObjectAsMap = MetadataJsonSchemaValidator(Asset).validateMetadataJsonObject(entryWithInnerTypeChanged).unsafeRunSync()
        val expectedValidatedJsonObjectAsMap = convertUjsonObjToSchemaValidatedMap(entryWithInnerTypeChanged) ++ Map(arrayTypeField -> expectedErrorMessage.invalidNel[Value])
        validatedJsonObjectAsMap should equal(sortJsonObjectByFieldName(expectedValidatedJsonObjectAsMap))
      }
  }

  forAll(entryTypeSchemas.filter(_.toString != "UnknownType")) { entryTypeSchema =>
    "validateMetadataJsonObject" should s", for an entry of type '$entryTypeSchema', return a 'required property key not found' " +
      "error message for all of the missing 'required' properties" in {
        val entryWithoutAnyRequiredFields = Obj()
        val validatedJsonObjectAsMap = MetadataJsonSchemaValidator(entryTypeSchema).validateMetadataJsonObject(entryWithoutAnyRequiredFields).unsafeRunSync()
        val entryKeys = testValidMetadataJson().collect { case entry if entry(entryType).str == entryTypeSchema.toString => entry.value.keys }.head
        val expectedValidatedJsonObject = entryKeys.collect {
          case key if !List("checksum_sha256", "series").contains(key) =>
            key -> MissingPropertyError(key, s"$$: required property '$key' not found").invalidNel[Value]
        }.toMap
        validatedJsonObjectAsMap should equal(sortJsonObjectByFieldName(expectedValidatedJsonObject))
      }
  }

  forAll(entryTypeSchemas.filter(_.toString != "File")) { entryTypeSchema =>
    forAll(someValidSeries) { series =>
      "validateMetadataJsonObject" should s", for an entry of type '$entryTypeSchema', return 0 errors if a valid Series $series matches the Regex pattern" in {
        val entry = testValidMetadataJson(unknownTypeEntry).filter(_(entryType).str == entryTypeSchema.toString).head
        val entryWithSeriesChanged = Obj.from(entry.value ++ Map("series" -> series))
        val validatedJsonObjectAsMap = MetadataJsonSchemaValidator(entryTypeSchema).validateMetadataJsonObject(entryWithSeriesChanged).unsafeRunSync()
        validatedJsonObjectAsMap should equal(convertUjsonObjToSchemaValidatedMap(entryWithSeriesChanged))
      }
    }
  }

  forAll(entryTypeSchemas.filter(_.toString != "File")) { entryTypeSchema =>
    forAll(someInvalidSeries) { invalidSeries =>
      val series = "series"
      "validateMetadataJsonObject" should s", for an entry of type '$entryTypeSchema', return a ValueError if an invalid Series $invalidSeries does not match the Regex pattern" in {
        val entry = testValidMetadataJson(unknownTypeEntry).filter(_(entryType).str == entryTypeSchema.toString).head
        val entryWithSeriesChanged = Obj.from(entry.value ++ Map(series -> invalidSeries))
        val validatedJsonObjectAsMap = MetadataJsonSchemaValidator(entryTypeSchema).validateMetadataJsonObject(entryWithSeriesChanged).unsafeRunSync()

        val expectedValidatedJson = convertUjsonObjToSchemaValidatedMap(entryWithSeriesChanged) ++ Map(
          series -> ValueError(
            series,
            invalidSeries.str,
            s"$$.$series: does not match the regex pattern ^([A-Z]{1,4} [1-9][0-9]{0,3}|Unknown)$$"
          ).invalidNel[Value]
        )

        validatedJsonObjectAsMap should equal(expectedValidatedJson)
      }
    }
  }

  "validateMetadataJsonObject" should s", for an entry of type 'File', return a ValueError if it contains a series" in {
    val series = "series"
    val fileEntry = testValidMetadataJson().filter(_(entryType).str == "File").head
    val fileEntryWithSeries = Obj.from(fileEntry.value ++ Map(series -> Str("A 1")))
    val validatedJsonObjectAsMap = MetadataJsonSchemaValidator(File).validateMetadataJsonObject(fileEntryWithSeries).unsafeRunSync()
    val expectedValidatedJson = convertUjsonObjToSchemaValidatedMap(fileEntryWithSeries) ++ Map(
      series -> ValueError(
        series,
        "A 1",
        s"$$.$series: schema for 'series' is false"
      ).invalidNel[Value]
    )
    validatedJsonObjectAsMap should equal(expectedValidatedJson)
  }

  forAll(invalidSha256Checksums) { (invalidSha256Checksum, atMostOrLeast) =>
    val checksum256: String = "checksum_sha256"
    "validateMetadataJsonObject" should s"return a ValueError if the $checksum256 value is not at $atMostOrLeast 64 characters long" in {
      val fileEntry = testValidMetadataJson().filter(_(entryType).str == "File").head
      val entryWithChecksumChanged = Obj.from(fileEntry.value ++ Map(checksum256 -> invalidSha256Checksum))
      val validatedJsonObjectAsMap = MetadataJsonSchemaValidator(File).validateMetadataJsonObject(entryWithChecksumChanged).unsafeRunSync()

      val expectedValidatedJson = convertUjsonObjToSchemaValidatedMap(entryWithChecksumChanged) ++ Map(
        checksum256 -> ValueError(
          checksum256,
          invalidSha256Checksum.str,
          s"$$.$checksum256: must be at $atMostOrLeast 64 characters long"
        ).invalidNel[Value]
      )

      validatedJsonObjectAsMap should equal(sortJsonObjectByFieldName(expectedValidatedJson))
    }
  }

  forAll(nonSha256ChecksumsShorterThan32Chars) { (checksumSha, hashShorterThan32Chars) =>
    "validateMetadataJsonObject" should s"return a ValueError if the $checksumSha value is shorter than 32 characters" in {
      val fileEntry = testValidMetadataJson().filter(_(entryType).str == "File").head
      val entryWithChecksumChanged = Obj.from(fileEntry.value ++ Map(checksumSha -> hashShorterThan32Chars))
      val validatedJsonObjectAsMap = MetadataJsonSchemaValidator(File).validateMetadataJsonObject(entryWithChecksumChanged).unsafeRunSync()

      val expectedValidatedJson = convertUjsonObjToSchemaValidatedMap(entryWithChecksumChanged) ++ Map(
        checksumSha -> ValueError(
          checksumSha,
          hashShorterThan32Chars.str,
          s"$$.$checksumSha: must be at least 32 characters long"
        ).invalidNel[Value]
      )

      validatedJsonObjectAsMap should equal(sortJsonObjectByFieldName(expectedValidatedJson))
    }
  }

  "validateMetadataJsonObject" should s"return a ValueError if the representationSuffix value is less than 1" in {
    val representationSuffix = "representationSuffix"
    val fileEntry = testValidMetadataJson().filter(_(entryType).str == "File").head
    val invalidRepresentationSuffix = Num(0)
    val entryWithRepresentationSuffixChanged = Obj.from(fileEntry.value ++ Map(representationSuffix -> invalidRepresentationSuffix))
    val validatedJsonObjectAsMap = MetadataJsonSchemaValidator(File).validateMetadataJsonObject(entryWithRepresentationSuffixChanged).unsafeRunSync()

    val expectedValidatedJson = convertUjsonObjToSchemaValidatedMap(entryWithRepresentationSuffixChanged) ++ Map(
      representationSuffix -> ValueError(
        "representationSuffix",
        invalidRepresentationSuffix.value.toInt.toString,
        s"$$.$representationSuffix: must have a minimum value of 1"
      ).invalidNel[Value]
    )

    validatedJsonObjectAsMap should equal(sortJsonObjectByFieldName(expectedValidatedJson))
  }

  private def convertUjsonToString(ujsonArray: Value) = write(ujsonArray)

  private def validatedJsonObjectWithAllValueErrors(entry: Obj, schemaType: String) =
    entry.value.map { case (name, value) =>
      val invalidNel = name match {
        case "sortOrder" | "fileSize" | "representationSuffix" =>
          ValueError(name, "stringInsteadOfInt", s"$$.$name: string found, integer expected").invalidNel[Value]
        case "originalFiles" | "originalMetadataFiles" =>
          ValueError(name, "stringInsteadOfInt", s"$$.$name: string found, array expected").invalidNel[Value]
        case "parentId" | "title" if List("ArchiveFolder", "ContentFolder").contains(schemaType) =>
          ValueError(name, "123", s"$$.$name: integer found, [string, null] expected").invalidNel[Value]
        case "type" =>
          if List("ArchiveFolder", "ContentFolder").contains(value.str) then
            NonEmptyList
              .of(
                ValueError(name, "123", s"$$.$name: integer found, string expected"),
                ValueError(name, "123", s"""$$.$name: does not have a value in the enumeration ["ArchiveFolder", "ContentFolder"]""")
              )
              .sortBy(_.propertyWithError)
              .invalid
          else
            NonEmptyList
              .of(
                ValueError(name, "123", s"$$.$name: integer found, string expected"),
                ValueError(name, "123", s"""$$.$name: must be the constant value '$schemaType'""")
              )
              .sortBy(_.propertyWithError)
              .invalid
        case _ =>
          ValueError(name, "123", s"$$.$name: integer found, string expected").invalidNel[Value]
      }
      name -> invalidNel
    }.toMap
}
