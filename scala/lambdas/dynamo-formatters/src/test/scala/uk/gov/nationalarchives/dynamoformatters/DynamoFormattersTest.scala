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
import java.time.{Instant, OffsetDateTime}
import java.util.UUID
import scala.Console.in
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
      "checksum_Algorithm1" -> fromS("testChecksumAlgo1"),
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
      typeField -> fromS("Asset"),
      transferringBody -> fromS("testTransferringBody"),
      transferCompleteDatetime -> fromS("2023-06-01T00:00Z"),
      upstreamSystem -> fromS("testUpstreamSystem"),
      digitalAssetSource -> fromS("testDigitalAssetSource"),
      digitalAssetSubtype -> fromS("testDigitalAssetSubtype"),
      originalMetadataFiles -> generateListAttributeValue("3f42e3f2-fffe-4fe9-87f7-262e95b86d75"),
      title -> fromS("testTitle"),
      ingestedPreservica -> fromS("true"),
      ingestedCustodialCopy -> fromS("true"),
      description -> fromS("testDescription"),
      "id_Test" -> fromS("testIdentifier"),
      "id_Test2" -> fromS("testIdentifier2"),
      childCount -> fromN("1"),
      skipIngest -> fromBool(false),
      filePath -> fromS("/a/file/path"),
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
          (originalMetadataFiles, "3f42e3f2-fffe-4fe9-87f7-262e95b86d75"),
          (filePath, "/a/file/path")
        ) ++ baseFields
      case "File" =>
        List(
          (fileSize, "1"),
          (fileExtension, "testFileExtension"),
          ("checksum_SHA256", "checksum"),
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
    buildAttributeValue(missingFieldsMap(rowType, missingFields*) + (invalidNumericField -> fromS("1")))

  def missingFieldsMap(rowType: Type, fieldsToExclude: String*): Map[String, AttributeValue] =
    allMandatoryFieldsMap(rowType.toString, "Preservation")
      .filterNot(fields => fieldsToExclude.contains(fields._1))

  def missingFieldsAttributeValue(rowType: Type, fieldsToExclude: String*): AttributeValue = {
    val fieldMap = missingFieldsMap(rowType, fieldsToExclude*)
    AttributeValue.builder().m(fieldMap.asJava).build()
  }

  val invalidDynamoAttributeValues: TableFor3[AttributeValue, String, Type] = Table(
    ("attributeValue", "expectedErrorMessage", "rowType"),
    (missingFieldsAttributeValue(File, "checksum_SHA256"), "'checksum': missing", File),
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
      "'batchId': missing, 'id': missing, 'name': missing, 'sortOrder': missing, 'fileSize': missing, 'checksum': missing, 'type': missing, 'representationType': missing, 'representationSuffix': missing, 'childCount': missing, 'location': missing",
      File
    ),
    (
      invalidRepresentationTypeAttributeValue,
      "'representationType': could not be converted to desired type: java.lang.Exception: Representation type Invalid not found",
      File
    ),
    (
      stringValueInListIsNotConvertable(originalMetadataFiles, Asset),
      "'originalMetadataFiles': could not be converted to desired type: java.lang.RuntimeException: Cannot parse " +
        "notAUuid for field originalMetadataFiles into class java.util.UUID",
      Asset
    ),
    (
      missingFieldsInvalidNumericField(File, fileSize, id, batchId),
      "'batchId': missing, 'id': missing, 'fileSize': not of type: 'Number' was 'DynString(1)'",
      File
    )
  )

  forAll(invalidDynamoAttributeValues) { (attributeValue, expectedErrors, rowType) =>
    "dynamoItemFormat read" should s"return an error $expectedErrors for row type $rowType" in {
      val dynamoItemFormat = rowType match
        case ArchiveFolder => archiveFolderItemFormat
        case ContentFolder => contentFolderItemFormat
        case Asset         => assetItemFormat
        case File          => fileItemFormat

      val res = dynamoItemFormat.read(attributeValue)
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
    s"all${rowType}FieldsPopulated" should "contain all of the attributes in DynamoItem" in {
      val itemAttributeNames = rowType match {
        case ArchiveFolder => generateFolderDynamoItem().productElementNames
        case ContentFolder => generateFolderDynamoItem().productElementNames
        case Asset         => generateAssetDynamoItem().productElementNames
        case File =>
          val fileDynamoItem = generateFileDynamoItem()
          fileDynamoItem.productElementNames.filterNot(_ == "checksums") ++
            fileDynamoItem.checksums.map(checksumPrefix + _.algorithm)
      }
      val dynamoItemAttributeNames = itemAttributeNames.toList
      val dynamoItemAttributeNamesMapped = dynamoItemAttributeNames.map {
        case "potentialParentPath"          => "parentPath"
        case "potentialTitle"               => "title"
        case "potentialDescription"         => "description"
        case "potentialFileExtension"       => "fileExtension"
        case "identifiers"                  => "id_Test"
        case "ingestedPreservica"           => "ingested_PS"
        case "ingestedCustodialCopy"        => "ingested_CC"
        case "potentialDigitalAssetSubtype" => "digitalAssetSubtype"
        case theRest                        => theRest
      }

      val fieldsPopulated = populatedFields(rowType)

      val dynamoItemAttributesNotAccountedFor =
        dynamoItemAttributeNamesMapped.filterNot(fieldsPopulated.contains)

      dynamoItemAttributesNotAccountedFor should equal(Nil)
    }
  }

  "assetItemFormat read" should "return true for ingested_PS if the value is true and false otherwise" in {
    val assetRowIngested = assetItemFormat.read(buildAttributeValue(allAssetFieldsPopulated)).value
    assetRowIngested.ingestedPreservica should equal(true)

    val assetRowNotIngested =
      assetItemFormat.read(buildAttributeValue(allAssetFieldsPopulated + (ingestedPreservica -> fromS("false")))).value
    assetRowNotIngested.ingestedPreservica should equal(false)

    val assetRowInvalidValue =
      assetItemFormat.read(buildAttributeValue(allAssetFieldsPopulated + (ingestedPreservica -> fromS("1")))).value
    assetRowInvalidValue.ingestedPreservica should equal(false)
  }

  "assetItemFormat read" should "return true for ingested_CC if the value is true and false otherwise" in {
    val assetRowIngested = assetItemFormat.read(buildAttributeValue(allAssetFieldsPopulated)).value
    assetRowIngested.ingestedCustodialCopy should equal(true)

    val assetRowIngestedCCMissing = assetItemFormat.read(buildAttributeValue(allAssetFieldsPopulated.filter(_._1 != ingestedCustodialCopy))).value
    assetRowIngestedCCMissing.ingestedCustodialCopy should equal(false)

    val assetRowNotIngested =
      assetItemFormat.read(buildAttributeValue(allAssetFieldsPopulated + (ingestedCustodialCopy -> fromS("false")))).value
    assetRowNotIngested.ingestedCustodialCopy should equal(false)

    val assetRowInvalidValue =
      assetItemFormat.read(buildAttributeValue(allAssetFieldsPopulated + (ingestedCustodialCopy -> fromS("1")))).value
    assetRowInvalidValue.ingestedCustodialCopy should equal(false)
  }

  "assetItemFormat read" should "return the value if skipIngest is present and false otherwise" in {
    val skipIngestPresentFalse = assetItemFormat.read(buildAttributeValue(allAssetFieldsPopulated)).value
    skipIngestPresentFalse.skipIngest should equal(false)

    val skipIngestPresentTrue =
      assetItemFormat.read(buildAttributeValue(allAssetFieldsPopulated + (skipIngest -> fromBool(true)))).value

    skipIngestPresentTrue.skipIngest should equal(true)

    val skipIngestMissing =
      assetItemFormat.read(buildAttributeValue(allAssetFieldsPopulated.filter { case (field, _) => field != skipIngest })).value
    skipIngestMissing.skipIngest should equal(false)
  }

  "assetItemFormat read" should "return a valid object when all asset fields are populated" in {
    val assetRow = assetItemFormat.read(buildAttributeValue(allAssetFieldsPopulated)).value

    assetRow.batchId should equal("testBatchId")
    assetRow.id should equal(UUID.fromString(allAssetFieldsPopulated(id).s()))
    assetRow.potentialParentPath.get should equal("testParentPath")
    assetRow.`type` should equal(Asset)
    assetRow.transferringBody.get should equal("testTransferringBody")
    assetRow.transferCompleteDatetime.get should equal(OffsetDateTime.parse("2023-06-01T00:00Z"))
    assetRow.upstreamSystem should equal("testUpstreamSystem")
    assetRow.digitalAssetSource should equal("testDigitalAssetSource")
    assetRow.potentialDigitalAssetSubtype.get should equal("testDigitalAssetSubtype")
    assetRow.originalMetadataFiles should equal(List(UUID.fromString("3f42e3f2-fffe-4fe9-87f7-262e95b86d75")))
    assetRow.potentialTitle.get should equal("testTitle")
    assetRow.potentialDescription.get should equal("testDescription")
    assetRow.correlationId.get should equal("correlationId")
    assetRow.identifiers.sortBy(_.identifierName) should equal(
      List(Identifier("Test2", "testIdentifier2"), Identifier("Test", "testIdentifier")).sortBy(_.identifierName)
    )
  }

  "fileItemFormat read" should "return true for ingested_PS if the value is true and false otherwise" in {
    val fileRowIngested = fileItemFormat.read(buildAttributeValue(allFileFieldsPopulated)).value
    fileRowIngested.ingestedPreservica should equal(true)

    val fileRowNotIngested =
      fileItemFormat.read(buildAttributeValue(allFileFieldsPopulated + (ingestedPreservica -> fromS("false")))).value
    fileRowNotIngested.ingestedPreservica should equal(false)

    val fileRowInvalidValue =
      fileItemFormat.read(buildAttributeValue(allFileFieldsPopulated + (ingestedPreservica -> fromS("1")))).value
    fileRowInvalidValue.ingestedPreservica should equal(false)
  }

  "fileItemFormat read" should "return a valid object when all file fields are populated" in {
    val fileRow = fileItemFormat.read(buildAttributeValue(allFileFieldsPopulated)).value

    fileRow.batchId should equal("testBatchId")
    fileRow.id should equal(UUID.fromString(allFileFieldsPopulated(id).s()))
    fileRow.potentialParentPath.get should equal("testParentPath")
    fileRow.name should equal("testName")
    fileRow.`type` should equal(File)
    fileRow.potentialTitle.get should equal("testTitle")
    fileRow.potentialDescription.get should equal("testDescription")
    fileRow.fileSize should equal(1)
    fileRow.sortOrder should equal(2)
    fileRow.checksums.head.fingerprint should equal("testChecksumAlgo1")
    fileRow.potentialFileExtension.get should equal("testFileExtension")
    fileRow.location.toString should equal("s3://bucket/key")
  }

  "fileItemFormat read" should "return a valid object when more than one checksum is provided in the file fields" in {

    val mapWithAdditionalChecksum = allFileFieldsPopulated + ("checksum_Algorithm2" -> fromS("testChecksumAlgo2"))
    val fileRow = fileItemFormat.read(buildAttributeValue(mapWithAdditionalChecksum)).value

    fileRow.batchId should equal("testBatchId")
    fileRow.id should equal(UUID.fromString(allFileFieldsPopulated(id).s()))
    fileRow.potentialParentPath.get should equal("testParentPath")
    fileRow.name should equal("testName")
    fileRow.`type` should equal(File)
    fileRow.potentialTitle.get should equal("testTitle")
    fileRow.potentialDescription.get should equal("testDescription")
    fileRow.fileSize should equal(1)
    fileRow.sortOrder should equal(2)
    fileRow.checksums.size should equal(2)
    fileRow.checksums.find(_.algorithm.equals("Algorithm1")).get.fingerprint should equal("testChecksumAlgo1")
    fileRow.checksums.find(_.algorithm.equals("Algorithm2")).get.fingerprint should equal("testChecksumAlgo2")
    fileRow.potentialFileExtension.get should equal("testFileExtension")
    fileRow.location.toString should equal("s3://bucket/key")
  }

  "fileItemFormat write" should "write all mandatory fields and ignore any optional ones" in {
    val uuid = UUID.randomUUID()
    val dynamoItem = generateFileDynamoItem(uuid)
    val res = fileItemFormat.write(dynamoItem)
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
    resultMap("checksum_Algorithm1").s() should equal("testChecksumAlgo1")
    resultMap(fileExtension).s() should equal("ext")
    resultMap("representationType").s() should equal("Preservation")
    resultMap(ingestedPreservica).s() should equal("true")
    resultMap(ingestedCustodialCopy).s() should equal("true")
    resultMap("representationSuffix").n() should equal("1")
    resultMap("id_FileIdentifier1").s() should equal("FileIdentifier1Value")
    resultMap(location).s() should equal("s3://bucket/key")
  }

  "archiveFolderItemFormat read" should "return a valid object when all folder fields are populated" in {
    val folderItem = archiveFolderItemFormat.read(buildAttributeValue(allFolderFieldsPopulated)).value

    folderItem.batchId should equal("testBatchId")
    folderItem.id should equal(UUID.fromString(allFolderFieldsPopulated(id).s()))
    folderItem.potentialParentPath.get should equal("testParentPath")
    folderItem.name should equal("testName")
    folderItem.`type` should equal(ArchiveFolder)
    folderItem.potentialTitle.get should equal("title")
    folderItem.potentialDescription.get should equal("description")
  }

  "archiveFolderItemFormat write" should "write all mandatory fields and ignore any optional ones" in {
    val uuid = UUID.randomUUID()
    val dynamoItem = generateFolderDynamoItem(uuid)
    val res = archiveFolderItemFormat.write(dynamoItem)
    val resultMap = res.toAttributeValue.m().asScala

    resultMap(batchId).s() should equal(batchId)
    resultMap(id).s() should equal(uuid.toString)
    resultMap(name).s() should equal(name)
    resultMap(parentPath).s() should equal("parentPath")
    resultMap(title).s() should equal("title")
    resultMap(description).s() should equal("description")
    resultMap(typeField).s() should equal("ArchiveFolder")
  }

  "assetItemFormat write" should "write all mandatory fields and ignore any optional ones" in {
    val uuid = UUID.randomUUID()
    val originalMetadataFilesUuid = UUID.randomUUID()
    val dynamoItem = AssetDynamoItem(
      batchId,
      uuid,
      None,
      Asset,
      None,
      None,
      Option(transferringBody),
      Option(OffsetDateTime.parse("2023-06-01T00:00Z")),
      upstreamSystem,
      digitalAssetSource,
      None,
      List(originalMetadataFilesUuid),
      true,
      true,
      Nil,
      1,
      false,
      None,
      ""
    )
    val res = assetItemFormat.write(dynamoItem)
    val resultMap = res.toAttributeValue.m().asScala
    resultMap(batchId).s() should equal(batchId)
    resultMap(id).s() should equal(uuid.toString)
    resultMap(typeField).s() should equal("Asset")
    resultMap(transferringBody).s() should equal(transferringBody)
    resultMap(transferCompleteDatetime).s() should equal("2023-06-01T00:00Z")
    resultMap(upstreamSystem).s() should equal(upstreamSystem)
    resultMap(digitalAssetSource).s() should equal(digitalAssetSource)
    resultMap(originalMetadataFiles).ss().asScala.toList should equal(List(originalMetadataFilesUuid.toString))
    resultMap(ingestedPreservica).s() should equal("true")
    resultMap(ingestedCustodialCopy).s() should equal("true")
    val optionalsInResult =
      List(parentPath, title, description, sortOrder, fileSize, "checksums", fileExtension, "identifiers", skipIngest, correlationId, digitalAssetSubtype).filter(
        resultMap.contains
      )
    assert(optionalsInResult.isEmpty, s"The following fields are not ignored: ${optionalsInResult.mkString(",")}")
  }

  "assetItemFormat write" should "write skipIngest if set to true and not write it otherwise" in {
    def skipIngestValue(skipIngestValue: Boolean): Option[Boolean] = {
      val res = assetItemFormat.write(generateAssetDynamoItem(skipIngest = skipIngestValue))
      val resultMap = res.toAttributeValue.m().asScala
      resultMap.get(skipIngest).map(_.bool().booleanValue())
    }
    skipIngestValue(false).isDefined should be(false)
    skipIngestValue(true).contains(true) should be(true)

  }

  "contentFolderItemFormat read" should "return a valid object when all folder fields are populated" in {
    val folderItem = contentFolderItemFormat.read(buildAttributeValue(allFolderFieldsPopulated)).value

    folderItem.batchId should equal("testBatchId")
    folderItem.id should equal(UUID.fromString(allFolderFieldsPopulated(id).s()))
    folderItem.potentialParentPath.get should equal("testParentPath")
    folderItem.name should equal("testName")
    folderItem.`type` should equal(ArchiveFolder)
    folderItem.potentialTitle.get should equal("title")
    folderItem.potentialDescription.get should equal("description")
    folderItem.identifiers should equal(List(Identifier("Test", "testIdentifier")))
  }

  "contentFolderItemFormat write" should "write all mandatory fields and ignore any optional ones" in {
    val uuid = UUID.randomUUID()
    val dynamoItem = generateContentFolderDynamoItem(uuid)
    val res = contentFolderItemFormat.write(dynamoItem)
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
    val input = fromM(Map("invalid" -> fromS(uuid.toString)).asJava) //
    val res = filesTablePkFormat.read(input)

    val dynamoReadError = res.left.value.asInstanceOf[InvalidPropertiesError].errors.head._2
    dynamoReadError should be(MissingProperty)
  }

  "filesTablePkFormat write" should "write the correct fields" in {
    val uuid = UUID.randomUUID()
    val attributeValueMap = filesTablePkFormat.write(FilesTablePrimaryKey(FilesTablePartitionKey(uuid), FilesTableSortKey(batchId))).toAttributeValue.m().asScala
    UUID.fromString(attributeValueMap(id).s()) should equal(uuid)
    attributeValueMap(batchId).s() should equal(batchId)
  }

  "postIngestStateTablePkFormat read" should "read the correct fields" in {
    val uuid = UUID.randomUUID()
    val batchId = "batchId"
    val input = fromM(Map(assetId -> fromS(uuid.toString), batchId -> fromS(batchId)).asJava)
    val res = postIngestStatePkFormat.read(input).value
    res.partitionKey.assetId should equal(uuid)
    res.sortKey.batchId should equal(batchId)
  }

  "postIngestStateTablePkFormat read" should "error if the field is missing" in {
    val uuid = UUID.randomUUID()
    val input = fromM(Map("invalid" -> fromS(uuid.toString)).asJava) //
    val res = postIngestStatePkFormat.read(input)

    val dynamoReadError = res.left.value.asInstanceOf[InvalidPropertiesError].errors.head._2
    dynamoReadError should be(MissingProperty)
  }

  "postIngestStateTablePkFormat write" should "write the correct fields" in {
    val uuid = UUID.randomUUID()
    val attributeValueMap =
      postIngestStatePkFormat.write(PostIngestStatePrimaryKey(PostIngestStatePartitionKey(uuid), PostIngestStateSortKey(batchId))).toAttributeValue.m().asScala
    UUID.fromString(attributeValueMap(assetId).s()) should equal(uuid)
    attributeValueMap(batchId).s() should equal(batchId)
  }

  "queueTablePkFormat write" should "write the correct fields" in {
    val uuid = UUID.randomUUID()
    val attributeValueMap = queueTablePkFormat
      .write(IngestQueuePrimaryKey(IngestQueuePartitionKey("TEST"), IngestQueueSortKey("2025-01-28T14:56:16.813553232Z_SOMESYS_2ec6248e_0")))
      .toAttributeValue
      .m()
      .asScala
    attributeValueMap(sourceSystem).s() should equal("TEST")
    attributeValueMap(queuedAt).s() should equal("2025-01-28T14:56:16.813553232Z_SOMESYS_2ec6248e_0")
  }

  "queueTablePkFormat read" should "read the ingest queue primary key " in {
    val queueTime = Instant.parse("2025-02-11T11:51:16.Z")
    val input = fromM(Map(sourceSystem -> fromS("SOME_SYSTEM"), queuedAt -> fromS(queueTime.toString + "_SOMESYS_2ec6248e_0")).asJava)
    val readResult = queueTablePkFormat.read(input).value

    readResult.partitionKey.sourceSystem should equal("SOME_SYSTEM")
    readResult.sortKey.queuedAt should equal("2025-02-11T11:51:16Z_SOMESYS_2ec6248e_0")
  }

  "queueTablePkFormat read" should "error when the property is missing" in {
    val input = fromM(Map("randomName" -> fromS("SOME_SYSTEM"), "notQueued" -> fromS("2025-02-11T11:51:16.Z")).asJava)
    val readResult = queueTablePkFormat.read(input)

    readResult.isLeft should be(true)
    val errors = readResult.left.value.asInstanceOf[InvalidPropertiesError].errors
    errors.size should be(2)
    errors.find(_._1 == sourceSystem).get._2 should be(MissingProperty)
    errors.find(_._1 == queuedAt).get._2 should be(MissingProperty)
  }

  "ingestLockTableItemFormat read" should "read the correct fields" in {
    val assetId = UUID.randomUUID()
    val groupId = "groupId"
    val message = "{}"
    val createdAt = "2025-07-03T19:29:00.000Z"

    val input =
      fromM(Map("assetId" -> fromS(assetId.toString), "groupId" -> fromS(groupId), "message" -> fromS(message), "createdAt" -> fromS(createdAt)).asJava)
    val res = ingestLockTableItemFormat.read(input).value
    res.assetId should equal(assetId)
    res.groupId should equal(groupId)
    res.message should equal(res.message)
    res.createdAt should equal(createdAt)
  }

  "ingestLockTableItemFormat read" should "error if the field is missing" in {
    val assetId = UUID.randomUUID()

    val input = fromM(Map("invalidField" -> fromS(assetId.toString)).asJava)
    val res = ingestLockTableItemFormat.read(input)

    val dynamoReadError = res.left.value.asInstanceOf[InvalidPropertiesError].errors.head._2
    dynamoReadError should be(MissingProperty)
  }

  "ingestLockTableItemFormat write" should "write the correct fields" in {
    val assetId = UUID.randomUUID()
    val groupId = "groupId"
    val message = "{}"
    val createdAt = "2025-07-03T19:29:00.000Z"

    val attributeValueMap =
      ingestLockTableItemFormat.write(IngestLockTableItem(assetId, groupId, message, createdAt)).toAttributeValue.m().asScala
    UUID.fromString(attributeValueMap("assetId").s()) should equal(assetId)
    attributeValueMap("groupId").s() should equal("groupId")
    attributeValueMap("message").s() should equal("{}")
    attributeValueMap("createdAt").s() should equal("2025-07-03T19:29:00.000Z")
  }

  "postIngestStatusTableItemFormat read" should "read the correct fields" in {
    val assetId = UUID.randomUUID()
    val batchId = "batchId"
    val inputAttr = "{}"
    val correlationId = "correlationId"
    val queue = "queue"
    val firstQueued = "2038-01-19T15:14:07.000Z"
    val lastQueued = "2038-01-19T15:14:07.000Z"
    val resultCC = "result_CC"

    val input =
      fromM(
        Map(
          "assetId" -> fromS(assetId.toString),
          "batchId" -> fromS(batchId),
          "input" -> fromS(inputAttr),
          "correlationId" -> fromS(correlationId),
          "queue" -> fromS(queue),
          "firstQueued" -> fromS(firstQueued),
          "lastQueued" -> fromS(lastQueued),
          "result_CC" -> fromS(resultCC)
        ).asJava
      )
    val res = postIngestStatusTableItemFormat.read(input).value

    res.assetId should equal(assetId)
    res.batchId should equal(batchId)
    res.input should equal(inputAttr)
    res.potentialCorrelationId should equal(Some(correlationId))
    res.potentialQueue should equal(Some(queue))
    res.potentialFirstQueued should equal(Some(firstQueued))
    res.potentialLastQueued should equal(Some(lastQueued))
    res.potentialResultCC should equal(Some(resultCC))
  }

  "postIngestStatusTableItemFormat read" should "return optional fields with the value of 'None' if they were not provided" in {
    val assetId = UUID.randomUUID()
    val batchId = "batchId"
    val inputAttr = "{}"

    val input =
      fromM(
        Map(
          "assetId" -> fromS(assetId.toString),
          "batchId" -> fromS(batchId),
          "input" -> fromS(inputAttr)
        ).asJava
      )
    val res = postIngestStatusTableItemFormat.read(input).value

    res.assetId should equal(assetId)
    res.batchId should equal(batchId)
    res.input should equal(inputAttr)

    res.potentialCorrelationId should equal(None)
    res.potentialQueue should equal(None)
    res.potentialFirstQueued should equal(None)
    res.potentialLastQueued should equal(None)
    res.potentialResultCC should equal(None)
  }

  "postIngestStatusTableItemFormat read" should "error if the field is missing" in {
    val assetId = UUID.randomUUID()

    val input = fromM(Map("invalidField" -> fromS(assetId.toString)).asJava)
    val res = postIngestStatusTableItemFormat.read(input)

    val dynamoReadError = res.left.value.asInstanceOf[InvalidPropertiesError].errors.head._2
    dynamoReadError should be(MissingProperty)
  }

  "postIngestStatusTableItemFormat write" should "write the correct fields" in {
    val assetId = UUID.randomUUID()
    val batchId = "batchId"
    val input = "{}"
    val potentialCorrelationId = Some("correlationId")
    val potentialQueue = Some("potentialQueue")
    val potentialFirstQueued = Some("2038-01-19T15:14:07.000Z")
    val potentialLastQueued = Some("2038-01-19T15:14:07.000Z")
    val potentialResultCC = Some("result_CC")

    val attributeValueMap =
      postIngestStatusTableItemFormat
        .write(
          PostIngestStateTableItem(
            assetId,
            batchId,
            input,
            potentialCorrelationId,
            potentialQueue,
            potentialFirstQueued,
            potentialLastQueued,
            potentialResultCC
          )
        )
        .toAttributeValue
        .m()
        .asScala

    UUID.fromString(attributeValueMap("assetId").s()) should equal(assetId)
    attributeValueMap("batchId").s() should equal(batchId)
    attributeValueMap("input").s() should equal(input)
    attributeValueMap("correlationId").s() should equal("correlationId")
    attributeValueMap("queue").s() should equal(potentialQueue.get)
    attributeValueMap("firstQueued").s() should equal(potentialFirstQueued.get)
    attributeValueMap("lastQueued").s() should equal(potentialLastQueued.get)
    attributeValueMap("result_CC").s() should equal(potentialResultCC.get)
  }

  "postIngestStatusTableItemFormat write" should "not write optional fields if their values are 'None'" in {
    val assetId = UUID.randomUUID()
    val batchId = "batchId"
    val input = "{}"
    val potentialCorrelationId = None
    val potentialQueue = None
    val potentialFirstQueued = None
    val potentialLastQueued = None
    val potentialResultCC = None

    val attributeValueMap =
      postIngestStatusTableItemFormat
        .write(
          PostIngestStateTableItem(
            assetId,
            batchId,
            input,
            potentialCorrelationId,
            potentialQueue,
            potentialFirstQueued,
            potentialLastQueued,
            potentialResultCC
          )
        )
        .toAttributeValue
        .m()
        .asScala

    UUID.fromString(attributeValueMap("assetId").s()) should equal(assetId)
    attributeValueMap("batchId").s() should equal(batchId)
    attributeValueMap("input").s() should equal(input)
    attributeValueMap.get("correlationId") should equal(None)
    attributeValueMap.get("queue") should equal(None)
    attributeValueMap.get("firstQueued") should equal(None)
    attributeValueMap.get("lastQueued") should equal(None)
    attributeValueMap.get("result_CC") should equal(None)
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

    val dynamoReadError = res.left.value.asInstanceOf[InvalidPropertiesError].errors.head._2
    dynamoReadError should be(MissingProperty)
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

  private def generateAssetDynamoItem(
      uuid: UUID = UUID.randomUUID(),
      originalMetadataFilesUuid: UUID = UUID.randomUUID(),
      ingestedPreservica: Boolean = true,
      skipIngest: Boolean = true
  ): AssetDynamoItem = {

    val identifiers = List(Identifier("Test1", "Value1"), Identifier("Test2", "Value2"))
    AssetDynamoItem(
      batchId,
      uuid,
      Option(parentPath),
      Asset,
      Option(title),
      Option(description),
      Option(transferringBody),
      Option(OffsetDateTime.parse("2023-06-01T00:00Z")),
      upstreamSystem,
      digitalAssetSource,
      Option(digitalAssetSubtype),
      List(originalMetadataFilesUuid),
      ingestedPreservica,
      true,
      identifiers,
      1,
      skipIngest,
      Option("correlationId"),
      ""
    )
  }

  private def generateFileDynamoItem(
      uuid: UUID = UUID.randomUUID(),
      ingestedPreservica: Boolean = true
  ): FileDynamoItem = {
    FileDynamoItem(
      batchId,
      uuid,
      Option(parentPath),
      name,
      File,
      Option(title),
      Option(description),
      1,
      2,
      List(Checksum("Algorithm1", "testChecksumAlgo1")),
      Option("ext"),
      PreservationRepresentationType,
      1,
      ingestedPreservica,
      true,
      List(Identifier("FileIdentifier1", "FileIdentifier1Value")),
      1,
      URI.create("s3://bucket/key")
    )
  }

  private def generateFolderDynamoItem(uuid: UUID = UUID.randomUUID()): ArchiveFolderDynamoItem = {
    ArchiveFolderDynamoItem(
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

  private def generateContentFolderDynamoItem(uuid: UUID = UUID.randomUUID()): ContentFolderDynamoItem =
    ContentFolderDynamoItem(
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
