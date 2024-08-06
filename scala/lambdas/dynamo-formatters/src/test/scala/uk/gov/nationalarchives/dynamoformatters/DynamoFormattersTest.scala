package uk.gov.nationalarchives.dynamoformatters

import cats.implicits.*
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1, TableFor3}
import org.scanamo.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.{fromBool, fromL, fromM, fromN, fromS}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{*, given}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.FileRepresentationType.*

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID
import scala.jdk.CollectionConverters.*

class DynamoFormattersTest extends AnyFlatSpec with TableDrivenPropertyChecks with EitherValues {

  val allFolderFieldsPopulated: Map[String, AttributeValue] =
    Map(
      batchId -> fromS("testBatchId"),
      id -> fromS(UUID.randomUUID().toString),
      parentPath -> fromS("testParentPath"),
      name -> fromS("testName"),
      typeField -> fromS("ArchiveFolder"),
      title -> fromS("title"),
      description -> fromS("description"),
      "id_Test" -> fromS("testIdentifier"),
      childCount -> fromN("1")
    )

  val allFileFieldsPopulated: Map[String, AttributeValue] =
    Map(
      batchId -> fromS("testBatchId"),
      id -> fromS(UUID.randomUUID().toString),
      parentPath -> fromS("testParentPath"),
      name -> fromS("testName"),
      typeField -> fromS("File"),
      title -> fromS("testTitle"),
      description -> fromS("testDescription"),
      sortOrder -> fromN("2"),
      fileSize -> fromN("1"),
      checksumSha256 -> fromS("testChecksumSha256"),
      fileExtension -> fromS("testFileExtension"),
      representationType -> fromS("Preservation"),
      ingestedPreservica -> fromS("true"),
      ingestedCustodialCopy -> fromS("true"),
      representationSuffix -> fromN("1"),
      "id_Test" -> fromS("testIdentifier"),
      childCount -> fromN("1"),
      location -> fromS("s3://bucket/key")
    )
  val allAssetFieldsPopulated: Map[String, AttributeValue] = {
    Map(
      batchId -> fromS("testBatchId"),
      id -> fromS(UUID.randomUUID().toString),
      parentPath -> fromS("testParentPath"),
      name -> fromS("testName"),
      typeField -> fromS("Asset"),
      transferringBody -> fromS("testTransferringBody"),
      transferCompleteDatetime -> fromS("2023-06-01T00:00Z"),
      upstreamSystem -> fromS("testUpstreamSystem"),
      digitalAssetSource -> fromS("testDigitalAssetSource"),
      digitalAssetSubtype -> fromS("testDigitalAssetSubtype"),
      originalFiles -> generateListAttributeValue("dec2b921-20e3-41e8-a299-f3cbc13131a2"),
      originalMetadataFiles -> generateListAttributeValue("3f42e3f2-fffe-4fe9-87f7-262e95b86d75"),
      title -> fromS("testTitle"),
      ingestedPreservica -> fromS("true"),
      ingestedCustodialCopy -> fromS("true"),
      description -> fromS("testDescription"),
      "id_Test" -> fromS("testIdentifier"),
      "id_Test2" -> fromS("testIdentifier2"),
      childCount -> fromN("1"),
      skipIngest -> fromBool(false),
      correlationId -> fromS("correlationId")
    )
  }

  def buildAttributeValue(map: Map[String, AttributeValue]): AttributeValue =
    AttributeValue.builder().m(map.asJava).build()

  def allMandatoryFieldsMap(typeValue: String, representationTypeValue: String): Map[String, AttributeValue] = {
    val baseFields = List(
      (id, UUID.randomUUID().toString),
      (batchId, "batchId"),
      (name, "name"),
      (typeField, typeValue),
      (childCount, "1")
    )
    val fields = typeValue match {
      case "ArchiveFolder" => baseFields
      case "ContentFolder" => baseFields
      case "Asset" =>
        List(
          (transferringBody, "testTransferringBody"),
          (transferCompleteDatetime, "2023-06-01T00:00Z"),
          (upstreamSystem, "testUpstreamSystem"),
          (digitalAssetSource, "testDigitalAssetSource"),
          (digitalAssetSubtype, "testDigitalAssetSubtype"),
          (originalFiles, "dec2b921-20e3-41e8-a299-f3cbc13131a2"),
          (originalMetadataFiles, "3f42e3f2-fffe-4fe9-87f7-262e95b86d75")
        ) ++ baseFields
      case "File" =>
        List(
          (fileSize, "1"),
          (fileExtension, "testFileExtension"),
          (checksumSha256, "checksum"),
          (sortOrder, "2"),
          (representationType, representationTypeValue),
          (representationSuffix, "1"),
          (location, "s3://bucket/key")
        ) ++ baseFields
      case _ => Nil
    }
    fields.map { case (name, value) =>
      if (name.endsWith("Files")) name -> generateListAttributeValue(value)
      else name -> (if (value.forall(_.isDigit)) fromN(value) else fromS(value))
    }.toMap
  }

  def populatedFields(rowType: Type): Map[String, AttributeValue] = rowType match {
    case ArchiveFolder | ContentFolder => allFolderFieldsPopulated
    case Asset                         => allAssetFieldsPopulated
    case File                          => allFileFieldsPopulated
  }

  def invalidNumericField(fieldName: String, rowType: Type): AttributeValue = {
    buildAttributeValue(populatedFields(rowType) + (fieldName -> fromS("1")))
  }

  def invalidBooleanField(fieldName: String, rowType: Type): AttributeValue = {
    buildAttributeValue(populatedFields(rowType) + (fieldName -> fromS("true")))
  }

  def invalidNumericValue(fieldName: String, rowType: Type): AttributeValue = buildAttributeValue(
    populatedFields(rowType) + (fieldName -> fromN("NaN"))
  )

  def invalidListOfStringsValue(fieldName: String, rowType: Type): AttributeValue = buildAttributeValue(
    populatedFields(rowType) + (
      fieldName -> generateListAttributeValue("1", "dec2b921-20e3-41e8-a299-f3cbc13131a2")
    )
  )

  def stringValueInListIsNotConvertable(fieldName: String, rowType: Type): AttributeValue =
    buildAttributeValue(
      populatedFields(rowType) + (fieldName -> generateListAttributeValue(
        "dec2b921-20e3-41e8-a299-f3cbc13131a2",
        "notAUuid"
      ))
    )

  def stringValueIsNotConvertible(fieldName: String, rowType: Type): AttributeValue = buildAttributeValue(
    populatedFields(rowType) + (fieldName -> fromS("notAConvertibleString"))
  )

  def invalidTypeAttributeValue: AttributeValue =
    AttributeValue.builder().m(allMandatoryFieldsMap("Invalid", "Preservation").asJava).build()

  def invalidRepresentationTypeAttributeValue: AttributeValue =
    AttributeValue.builder().m(allMandatoryFieldsMap("File", "Invalid").asJava).build()

  def missingFieldsInvalidNumericField(
      rowType: Type,
      invalidNumericField: String,
      missingFields: String*
  ): AttributeValue =
    buildAttributeValue(missingFieldsMap(rowType, missingFields: _*) + (invalidNumericField -> fromS("1")))

  def missingFieldsMap(rowType: Type, fieldsToExclude: String*): Map[String, AttributeValue] =
    allMandatoryFieldsMap(rowType.toString, "Preservation")
      .filterNot(fields => fieldsToExclude.contains(fields._1))

  def missingFieldsAttributeValue(rowType: Type, fieldsToExclude: String*): AttributeValue = {
    val fieldMap = missingFieldsMap(rowType, fieldsToExclude: _*)
    AttributeValue.builder().m(fieldMap.asJava).build()
  }

  val invalidDynamoAttributeValues: TableFor3[AttributeValue, String, Type] = Table(
    ("attributeValue", "expectedErrorMessage", "rowType"),
    (missingFieldsAttributeValue(ArchiveFolder, id), "'id': missing", ArchiveFolder),
    (missingFieldsAttributeValue(ArchiveFolder, batchId), "'batchId': missing", ArchiveFolder),
    (missingFieldsAttributeValue(ArchiveFolder, name), "'name': missing", ArchiveFolder),
    (missingFieldsAttributeValue(ArchiveFolder, typeField), "'type': missing", ArchiveFolder),
    (missingFieldsAttributeValue(File, location), "'location': missing", File),
    (
      missingFieldsAttributeValue(ArchiveFolder, typeField, batchId, name),
      "'batchId': missing, 'name': missing, 'type': missing",
      ArchiveFolder
    ),
    (
      missingFieldsAttributeValue(Asset, transferCompleteDatetime),
      "'transferCompleteDatetime': missing",
      Asset
    ),
    (
      missingFieldsAttributeValue(Asset, childCount),
      "'childCount': missing",
      Asset
    ),
    (
      invalidNumericField(childCount, File),
      "'childCount': not of type: 'Number' was 'DynString(1)'",
      File
    ),
    (
      invalidBooleanField(skipIngest, Asset),
      "'skipIngest': not of type: 'Boolean' was 'DynString(true)'",
      Asset
    ),
    (
      invalidNumericValue(childCount, File),
      "'childCount': could not be converted to desired type: java.lang.RuntimeException: Cannot parse NaN for field childCount into int",
      File
    ),
    (
      invalidNumericField(fileSize, File),
      "'fileSize': not of type: 'Number' was 'DynString(1)'",
      File
    ),
    (
      invalidNumericField(representationSuffix, File),
      "'representationSuffix': not of type: 'Number' was 'DynString(1)'",
      File
    ),
    (
      invalidNumericValue(fileSize, File),
      "'fileSize': could not be converted to desired type: java.lang.RuntimeException: Cannot parse NaN for field fileSize into long",
      File
    ),
    (
      invalidNumericField(sortOrder, File),
      "'sortOrder': not of type: 'Number' was 'DynString(1)'",
      File
    ),
    (
      invalidNumericValue(sortOrder, File),
      "'sortOrder': could not be converted to desired type: java.lang.RuntimeException: Cannot parse NaN for field sortOrder into int",
      File
    ),
    (
      invalidTypeAttributeValue,
      "'batchId': missing, 'id': missing, 'name': missing, 'sortOrder': missing, 'fileSize': missing, 'checksum_sha256': missing, 'fileExtension': missing, 'type': missing, 'representationType': missing, 'representationSuffix': missing, 'childCount': missing, 'location': missing",
      File
    ),
    (
      invalidRepresentationTypeAttributeValue,
      "'representationType': could not be converted to desired type: java.lang.Exception: Representation type Invalid not found",
      File
    ),
    (
      invalidListOfStringsValue(originalFiles, Asset),
      "'originalFiles': missing",
      Asset
    ),
    (
      stringValueInListIsNotConvertable(originalMetadataFiles, Asset),
      "'originalMetadataFiles': could not be converted to desired type: java.lang.RuntimeException: Cannot parse " +
        "notAUuid for field originalMetadataFiles into class java.util.UUID",
      Asset
    ),
    (
      stringValueIsNotConvertible(transferCompleteDatetime, Asset),
      "'transferCompleteDatetime': could not be converted to desired type: java.lang.RuntimeException: Cannot parse " +
        "notAConvertibleString for field transferCompleteDatetime into class java.time.OffsetDateTime",
      Asset
    ),
    (
      missingFieldsInvalidNumericField(File, fileSize, id, batchId),
      "'batchId': missing, 'id': missing, 'fileSize': not of type: 'Number' was 'DynString(1)'",
      File
    )
  )

  forAll(invalidDynamoAttributeValues) { (attributeValue, expectedErrors, rowType) =>
    "dynamoTableFormat read" should s"return an error $expectedErrors for row type $rowType" in {
      val dynamoTableFormat = rowType match
        case ArchiveFolder => archiveFolderTableFormat
        case ContentFolder => contentFolderTableFormat
        case Asset         => assetTableFormat
        case File          => fileTableFormat

      val res = dynamoTableFormat.read(attributeValue)
      res.isLeft should be(true)
      res.left.value.show should equal(expectedErrors)
    }
  }

  val typeTable: TableFor1[Type] = Table(
    "rowType",
    ArchiveFolder,
    ContentFolder,
    Asset,
    File
  )

  forAll(typeTable) { rowType =>
    s"all${rowType}FieldsPopulated" should "contain all of the fields in DynamoTable" in {
      val tableElementNames = rowType match {
        case ArchiveFolder => generateFolderDynamoTable().productElementNames
        case ContentFolder => generateFolderDynamoTable().productElementNames
        case Asset         => generateAssetDynamoTable().productElementNames
        case File          => generateFileDynamoTable().productElementNames
      }
      val dynamoTableFields = tableElementNames.toList
      val dynamoTableFieldsMapped = dynamoTableFields.map {
        case "identifiers"           => "id_Test"
        case "checksumSha256"        => "checksum_sha256"
        case "ingestedPreservica"    => "ingested_PS"
        case "ingestedCustodialCopy" => "ingested_CC"
        case theRest                 => theRest
      }

      val fieldsPopulated = populatedFields(rowType)

      val dynamoFieldsNotAccountedFor =
        dynamoTableFieldsMapped.filterNot(fieldsPopulated.contains)

      dynamoFieldsNotAccountedFor should equal(Nil)
    }
  }

  "assetTableFormat read" should "return true for ingested_PS if the value is true and false otherwise" in {
    val assetRowIngested = assetTableFormat.read(buildAttributeValue(allAssetFieldsPopulated)).value
    assetRowIngested.ingestedPreservica should equal(true)

    val assetRowNotIngested =
      assetTableFormat.read(buildAttributeValue(allAssetFieldsPopulated + (ingestedPreservica -> fromS("false")))).value
    assetRowNotIngested.ingestedPreservica should equal(false)

    val assetRowInvalidValue =
      assetTableFormat.read(buildAttributeValue(allAssetFieldsPopulated + (ingestedPreservica -> fromS("1")))).value
    assetRowInvalidValue.ingestedPreservica should equal(false)
  }

  "assetTableFormat read" should "return true for ingested_CC if the value is true and false otherwise" in {
    val assetRowIngested = assetTableFormat.read(buildAttributeValue(allAssetFieldsPopulated)).value
    assetRowIngested.ingestedCustodialCopy should equal(true)

    val assetRowIngestedCCMissing = assetTableFormat.read(buildAttributeValue(allAssetFieldsPopulated.filter(_._1 != ingestedCustodialCopy))).value
    assetRowIngestedCCMissing.ingestedCustodialCopy should equal(false)

    val assetRowNotIngested =
      assetTableFormat.read(buildAttributeValue(allAssetFieldsPopulated + (ingestedCustodialCopy -> fromS("false")))).value
    assetRowNotIngested.ingestedCustodialCopy should equal(false)

    val assetRowInvalidValue =
      assetTableFormat.read(buildAttributeValue(allAssetFieldsPopulated + (ingestedCustodialCopy -> fromS("1")))).value
    assetRowInvalidValue.ingestedCustodialCopy should equal(false)
  }

  "assetTableFormat read" should "return the value if skipIngest is present and false otherwise" in {
    val skipIngestPresentFalse = assetTableFormat.read(buildAttributeValue(allAssetFieldsPopulated)).value
    skipIngestPresentFalse.skipIngest should equal(false)

    val skipIngestPresentTrue =
      assetTableFormat.read(buildAttributeValue(allAssetFieldsPopulated + (skipIngest -> fromBool(true)))).value

    skipIngestPresentTrue.skipIngest should equal(true)

    val skipIngestMissing =
      assetTableFormat.read(buildAttributeValue(allAssetFieldsPopulated.filter { case (field, _) => field != skipIngest })).value
    skipIngestMissing.skipIngest should equal(false)
  }

  "assetTableFormat read" should "return a valid object when all asset fields are populated" in {
    val assetRow = assetTableFormat.read(buildAttributeValue(allAssetFieldsPopulated)).value

    assetRow.batchId should equal("testBatchId")
    assetRow.id should equal(UUID.fromString(allAssetFieldsPopulated(id).s()))
    assetRow.parentPath.get should equal("testParentPath")
    assetRow.name should equal("testName")
    assetRow.`type` should equal(Asset)
    assetRow.transferringBody should equal("testTransferringBody")
    assetRow.transferCompleteDatetime should equal(OffsetDateTime.parse("2023-06-01T00:00Z"))
    assetRow.upstreamSystem should equal("testUpstreamSystem")
    assetRow.digitalAssetSource should equal("testDigitalAssetSource")
    assetRow.digitalAssetSubtype should equal("testDigitalAssetSubtype")
    assetRow.originalFiles should equal(List(UUID.fromString("dec2b921-20e3-41e8-a299-f3cbc13131a2")))
    assetRow.originalMetadataFiles should equal(List(UUID.fromString("3f42e3f2-fffe-4fe9-87f7-262e95b86d75")))
    assetRow.title.get should equal("testTitle")
    assetRow.description.get should equal("testDescription")
    assetRow.correlationId.get should equal("correlationId")
    assetRow.identifiers.sortBy(_.identifierName) should equal(
      List(Identifier("Test2", "testIdentifier2"), Identifier("Test", "testIdentifier")).sortBy(_.identifierName)
    )
  }

  "fileTableFormat read" should "return true for ingested_PS if the value is true and false otherwise" in {
    val fileRowIngested = fileTableFormat.read(buildAttributeValue(allFileFieldsPopulated)).value
    fileRowIngested.ingestedPreservica should equal(true)

    val fileRowNotIngested =
      fileTableFormat.read(buildAttributeValue(allFileFieldsPopulated + (ingestedPreservica -> fromS("false")))).value
    fileRowNotIngested.ingestedPreservica should equal(false)

    val fileRowInvalidValue =
      fileTableFormat.read(buildAttributeValue(allFileFieldsPopulated + (ingestedPreservica -> fromS("1")))).value
    fileRowInvalidValue.ingestedPreservica should equal(false)
  }

  "fileTableFormat read" should "return a valid object when all file fields are populated" in {
    val fileRow = fileTableFormat.read(buildAttributeValue(allFileFieldsPopulated)).value

    fileRow.batchId should equal("testBatchId")
    fileRow.id should equal(UUID.fromString(allFileFieldsPopulated(id).s()))
    fileRow.parentPath.get should equal("testParentPath")
    fileRow.name should equal("testName")
    fileRow.`type` should equal(File)
    fileRow.title.get should equal("testTitle")
    fileRow.description.get should equal("testDescription")
    fileRow.fileSize should equal(1)
    fileRow.sortOrder should equal(2)
    fileRow.checksumSha256 should equal("testChecksumSha256")
    fileRow.fileExtension should equal("testFileExtension")
    fileRow.location.toString should equal("s3://bucket/key")
  }

  "fileTableFormat write" should "write all mandatory fields and ignore any optional ones" in {
    val uuid = UUID.randomUUID()
    val dynamoTable = generateFileDynamoTable(uuid)
    val res = fileTableFormat.write(dynamoTable)
    val resultMap = res.toAttributeValue.m().asScala
    resultMap(batchId).s() should equal(batchId)
    resultMap(id).s() should equal(uuid.toString)
    resultMap(name).s() should equal(name)
    resultMap(typeField).s() should equal("File")
    resultMap(parentPath).s() should equal("parentPath")
    resultMap(title).s() should equal("title")
    resultMap(description).s() should equal("description")
    resultMap(sortOrder).n() should equal("1")
    resultMap(fileSize).n() should equal("2")
    resultMap(checksumSha256).s() should equal("checksum")
    resultMap(fileExtension).s() should equal("ext")
    resultMap("representationType").s() should equal("Preservation")
    resultMap(ingestedPreservica).s() should equal("true")
    resultMap(ingestedCustodialCopy).s() should equal("true")
    resultMap("representationSuffix").n() should equal("1")
    resultMap("id_FileIdentifier1").s() should equal("FileIdentifier1Value")
    resultMap(location).s() should equal("s3://bucket/key")
  }

  "archiveFolderTableFormat read" should "return a valid object when all folder fields are populated" in {
    val folderRow = archiveFolderTableFormat.read(buildAttributeValue(allFolderFieldsPopulated)).value

    folderRow.batchId should equal("testBatchId")
    folderRow.id should equal(UUID.fromString(allFolderFieldsPopulated(id).s()))
    folderRow.parentPath.get should equal("testParentPath")
    folderRow.name should equal("testName")
    folderRow.`type` should equal(ArchiveFolder)
    folderRow.title.get should equal("title")
    folderRow.description.get should equal("description")
  }

  "archiveFolderTableFormat write" should "write all mandatory fields and ignore any optional ones" in {
    val uuid = UUID.randomUUID()
    val dynamoTable = generateFolderDynamoTable(uuid)
    val res = archiveFolderTableFormat.write(dynamoTable)
    val resultMap = res.toAttributeValue.m().asScala

    resultMap(batchId).s() should equal(batchId)
    resultMap(id).s() should equal(uuid.toString)
    resultMap(name).s() should equal(name)
    resultMap(parentPath).s() should equal("parentPath")
    resultMap(title).s() should equal("title")
    resultMap(description).s() should equal("description")
    resultMap(typeField).s() should equal("ArchiveFolder")
  }

  "assetTableFormat write" should "write all mandatory fields and ignore any optional ones" in {
    val uuid = UUID.randomUUID()
    val originalFilesUuid = UUID.randomUUID()
    val originalMetadataFilesUuid = UUID.randomUUID()
    val dynamoTable = AssetDynamoTable(
      batchId,
      uuid,
      None,
      name,
      ContentFolder,
      None,
      None,
      transferringBody,
      OffsetDateTime.parse("2023-06-01T00:00Z"),
      upstreamSystem,
      digitalAssetSource,
      digitalAssetSubtype,
      List(originalFilesUuid),
      List(originalMetadataFilesUuid),
      true,
      true,
      Nil,
      1,
      false,
      None
    )
    val res = assetTableFormat.write(dynamoTable)
    val resultMap = res.toAttributeValue.m().asScala
    resultMap(batchId).s() should equal(batchId)
    resultMap(id).s() should equal(uuid.toString)
    resultMap(name).s() should equal(name)
    resultMap(typeField).s() should equal("ContentFolder")
    resultMap(transferringBody).s() should equal(transferringBody)
    resultMap(transferCompleteDatetime).s() should equal("2023-06-01T00:00Z")
    resultMap(upstreamSystem).s() should equal(upstreamSystem)
    resultMap(digitalAssetSource).s() should equal(digitalAssetSource)
    resultMap(digitalAssetSubtype).s() should equal(digitalAssetSubtype)
    resultMap(originalFiles).ss().asScala.toList should equal(List(originalFilesUuid.toString))
    resultMap(originalMetadataFiles).ss().asScala.toList should equal(List(originalMetadataFilesUuid.toString))
    resultMap(ingestedPreservica).s() should equal("true")
    resultMap(ingestedCustodialCopy).s() should equal("true")
    List(parentPath, title, description, sortOrder, fileSize, checksumSha256, fileExtension, "identifiers", skipIngest, correlationId)
      .forall(resultMap.contains) should be(false)
  }

  "assetTableFormat write" should "write skipIngest if set to true and not write it otherwise" in {
    def skipIngestValue(skipIngestValue: Boolean): Option[Boolean] = {
      val res = assetTableFormat.write(generateAssetDynamoTable(skipIngest = skipIngestValue))
      val resultMap = res.toAttributeValue.m().asScala
      resultMap.get(skipIngest).map(_.bool().booleanValue())
    }
    skipIngestValue(false).isDefined should be(false)
    skipIngestValue(true).contains(true) should be(true)

  }

  "contentFolderTableFormat read" should "return a valid object when all folder fields are populated" in {
    val folderRow = contentFolderTableFormat.read(buildAttributeValue(allFolderFieldsPopulated)).value

    folderRow.batchId should equal("testBatchId")
    folderRow.id should equal(UUID.fromString(allFolderFieldsPopulated(id).s()))
    folderRow.parentPath.get should equal("testParentPath")
    folderRow.name should equal("testName")
    folderRow.`type` should equal(ArchiveFolder)
    folderRow.title.get should equal("title")
    folderRow.description.get should equal("description")
    folderRow.identifiers should equal(List(Identifier("Test", "testIdentifier")))
  }

  "contentFolderTableFormat write" should "write all mandatory fields and ignore any optional ones" in {
    val uuid = UUID.randomUUID()
    val dynamoTable = generateContentFolderDynamoTable(uuid)
    val res = contentFolderTableFormat.write(dynamoTable)
    val resultMap = res.toAttributeValue.m().asScala

    resultMap(batchId).s() should equal(batchId)
    resultMap(id).s() should equal(uuid.toString)
    resultMap(name).s() should equal(name)
    resultMap(parentPath).s() should equal("parentPath")
    resultMap(title).s() should equal("title")
    resultMap(description).s() should equal("description")
    resultMap(typeField).s() should equal("ContentFolder")
  }

  "filesTablePkFormat read" should "read the correct fields" in {
    val uuid = UUID.randomUUID()
    val batchId = "batchId"
    val input = fromM(Map(id -> fromS(uuid.toString), batchId -> fromS(batchId)).asJava)
    val res = filesTablePkFormat.read(input).value
    res.partitionKey.id should equal(uuid)
    res.sortKey.batchId should equal(batchId)
  }

  "filesTablePkFormat read" should "error if the field is missing" in {
    val uuid = UUID.randomUUID()
    val input = fromM(Map("invalid" -> fromS(uuid.toString)).asJava)
    val res = filesTablePkFormat.read(input)
    res.isLeft should be(true)
    val isMissingPropertyError = res.left.value.asInstanceOf[InvalidPropertiesError].errors.head._2 match {
      case MissingProperty => true
      case _               => false
    }
    isMissingPropertyError should be(true)
  }

  "filesTablePkFormat write" should "write the correct fields" in {
    val uuid = UUID.randomUUID()
    val attributeValueMap = filesTablePkFormat.write(FilesTablePrimaryKey(FilesTablePartitionKey(uuid), FilesTableSortKey(batchId))).toAttributeValue.m().asScala
    UUID.fromString(attributeValueMap(id).s()) should equal(uuid)
    attributeValueMap(batchId).s() should equal(batchId)
  }

  "ingestLockTableFormat read" should "read the correct fields" in {
    val assetId = UUID.randomUUID()
    val groupId = "groupId"
    val message = "{}"

    val input =
      fromM(Map("assetId" -> fromS(assetId.toString), "groupId" -> fromS(groupId), "message" -> fromS(message)).asJava)
    val res = ingestLockTableFormat.read(input).value
    res.assetId should equal(assetId)
    res.groupId should equal(groupId)
    res.message should equal(res.message)
  }

  "ingestLockTableFormat read" should "error if the field is missing" in {
    val assetId = UUID.randomUUID()

    val input = fromM(Map("invalidField" -> fromS(assetId.toString)).asJava)
    val res = ingestLockTableFormat.read(input)
    res.isLeft should be(true)
    val isMissingPropertyError = res.left.value.asInstanceOf[InvalidPropertiesError].errors.head._2 match {
      case MissingProperty => true
      case _               => false
    }
    isMissingPropertyError should be(true)
  }

  "ingestLockTableFormat write" should "write the correct fields" in {
    val assetId = UUID.randomUUID()
    val groupId = "groupId"
    val message = "{}"

    val attributeValueMap =
      ingestLockTableFormat.write(IngestLockTable(assetId, groupId, message)).toAttributeValue.m().asScala
    UUID.fromString(attributeValueMap("assetId").s()) should equal(assetId)
    attributeValueMap("groupId").s() should equal("groupId")
    attributeValueMap("message").s() should equal("{}")
  }

  "lockTablePkFormat read" should "read the correct fields" in {
    val uuid = UUID.randomUUID()
    val input = fromM(Map(assetId -> fromS(uuid.toString)).asJava)
    val res = lockTablePkFormat.read(input).value
    res.assetId should equal(uuid)
  }

  "lockTablePkFormat read" should "error if the field is missing" in {
    val uuid = UUID.randomUUID()
    val input = fromM(Map("invalid" -> fromS(uuid.toString)).asJava)
    val res = lockTablePkFormat.read(input)
    res.isLeft should be(true)
    val isMissingPropertyError = res.left.value.asInstanceOf[InvalidPropertiesError].errors.head._2 match {
      case MissingProperty => true
      case _               => false
    }
    isMissingPropertyError should be(true)
  }

  "lockTablePkFormat write" should "write the correct fields" in {
    val uuid = UUID.randomUUID()
    val attributeValueMap = lockTablePkFormat.write(LockTablePartitionKey(uuid)).toAttributeValue.m().asScala
    UUID.fromString(attributeValueMap(assetId).s()) should equal(uuid)
  }

  private def generateListAttributeValue(values: String*): AttributeValue =
    fromL(
      values.toList
        .map(value => if (value.forall(_.isDigit)) fromN(value) else fromS(value))
        .asJava
    )

  private def generateAssetDynamoTable(
      uuid: UUID = UUID.randomUUID(),
      originalFilesUuid: UUID = UUID.randomUUID(),
      originalMetadataFilesUuid: UUID = UUID.randomUUID(),
      ingestedPreservica: Boolean = true,
      skipIngest: Boolean = true
  ): AssetDynamoTable = {

    val identifiers = List(Identifier("Test1", "Value1"), Identifier("Test2", "Value2"))
    AssetDynamoTable(
      batchId,
      uuid,
      Option(parentPath),
      name,
      Asset,
      Option(title),
      Option(description),
      transferringBody,
      OffsetDateTime.parse("2023-06-01T00:00Z"),
      upstreamSystem,
      digitalAssetSource,
      digitalAssetSubtype,
      List(originalFilesUuid),
      List(originalMetadataFilesUuid),
      ingestedPreservica,
      true,
      identifiers,
      1,
      skipIngest,
      Option("correlationId")
    )
  }

  private def generateFileDynamoTable(
      uuid: UUID = UUID.randomUUID(),
      ingestedPreservica: Boolean = true
  ): FileDynamoTable = {
    FileDynamoTable(
      batchId,
      uuid,
      Option(parentPath),
      name,
      File,
      Option(title),
      Option(description),
      1,
      2,
      "checksum",
      "ext",
      PreservationRepresentationType,
      1,
      ingestedPreservica,
      true,
      List(Identifier("FileIdentifier1", "FileIdentifier1Value")),
      1,
      URI.create("s3://bucket/key")
    )
  }

  private def generateFolderDynamoTable(uuid: UUID = UUID.randomUUID()): ArchiveFolderDynamoTable = {
    ArchiveFolderDynamoTable(
      batchId,
      uuid,
      Option(parentPath),
      name,
      ArchiveFolder,
      Option(title),
      Option(description),
      Nil,
      1
    )
  }

  private def generateContentFolderDynamoTable(uuid: UUID = UUID.randomUUID()): ContentFolderDynamoTable =
    ContentFolderDynamoTable(
      batchId,
      uuid,
      Option(parentPath),
      name,
      ContentFolder,
      Option(title),
      Option(description),
      Nil,
      1
    )
}
