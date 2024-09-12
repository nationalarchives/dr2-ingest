package uk.gov.nationalarchives.dynamoformatters

import org.scanamo.{DynamoObject, DynamoValue}
import org.scanamo.generic.semiauto.FieldName
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*

object DynamoWriteUtils {

  private def commonFieldsToMap(table: DynamoTable): Map[String, DynamoValue] = {
    val optionalFields: Map[FieldName, DynamoValue] = Map(
      "title" -> table.title.map(DynamoValue.fromString),
      "description" -> table.description.map(DynamoValue.fromString),
      "parentPath" -> table.parentPath.map(DynamoValue.fromString)
    ).flatMap {
      case (fieldName, Some(potentialValue)) => Map(fieldName -> potentialValue)
      case _                                 => Map.empty
    }
    Map(
      "batchId" -> DynamoValue.fromString(table.batchId),
      "id" -> DynamoValue.fromString(table.id.toString),
      "name" -> DynamoValue.fromString(table.name),
      "type" -> DynamoValue.fromString(table.`type`.toString)
    ) ++ table.identifiers.map(id => s"id_${id.identifierName}" -> DynamoValue.fromString(id.value)).toMap ++
      optionalFields
  }

  def writeArchiveFolderTable(archiveFolderDynamoTable: ArchiveFolderDynamoTable): DynamoValue =
    DynamoObject {
      commonFieldsToMap(archiveFolderDynamoTable)
    }.toDynamoValue

  def writeContentFolderTable(contentFolderDynamoTable: ContentFolderDynamoTable): DynamoValue =
    DynamoObject {
      commonFieldsToMap(contentFolderDynamoTable)
    }.toDynamoValue

  def writeAssetTable(assetDynamoTable: AssetDynamoTable): DynamoValue =
    DynamoObject {
      commonFieldsToMap(assetDynamoTable) ++
        Map(
          "transferringBody" -> DynamoValue.fromString(assetDynamoTable.transferringBody),
          "transferCompleteDatetime" -> DynamoValue.fromString(assetDynamoTable.transferCompleteDatetime.toString),
          "upstreamSystem" -> DynamoValue.fromString(assetDynamoTable.upstreamSystem),
          "digitalAssetSource" -> DynamoValue.fromString(assetDynamoTable.digitalAssetSource),
          "digitalAssetSubtype" -> DynamoValue.fromString(assetDynamoTable.digitalAssetSubtype),
          "originalFiles" -> DynamoValue.fromStrings(assetDynamoTable.originalFiles.map(_.toString)),
          "originalMetadataFiles" -> DynamoValue.fromStrings(assetDynamoTable.originalMetadataFiles.map(_.toString)),
          ingestedPreservica -> DynamoValue.fromString(assetDynamoTable.ingestedPreservica.toString),
          ingestedCustodialCopy -> DynamoValue.fromString(assetDynamoTable.ingestedCustodialCopy.toString)
        ) ++ (if (assetDynamoTable.skipIngest) Map("skipIngest" -> DynamoValue.fromBoolean(assetDynamoTable.skipIngest)) else Map())
        ++ assetDynamoTable.correlationId.map(id => Map(correlationId -> DynamoValue.fromString(id))).getOrElse(Map())
    }.toDynamoValue

  def writeFileTable(fileDynamoTable: FileDynamoTable): DynamoValue =
    DynamoObject {
      commonFieldsToMap(fileDynamoTable) ++
        fileDynamoTable.fileExtension.map(extension => Map(fileExtension -> DynamoValue.fromString(extension))).getOrElse(Map()) ++
        Map(
          sortOrder -> DynamoValue.fromNumber[Int](fileDynamoTable.sortOrder),
          fileSize -> DynamoValue.fromNumber[Long](fileDynamoTable.fileSize),
          representationType -> DynamoValue.fromString(fileDynamoTable.representationType.toString),
          representationSuffix -> DynamoValue.fromNumber(fileDynamoTable.representationSuffix),
          ingestedPreservica -> DynamoValue.fromString(fileDynamoTable.ingestedPreservica.toString),
          location -> DynamoValue.fromString(fileDynamoTable.location.toString),
          ingestedCustodialCopy -> DynamoValue.fromString(fileDynamoTable.ingestedCustodialCopy.toString)
        ) ++ fileDynamoTable.checksums.map(eachChecksum => s"${checksumPrefix}${eachChecksum.algorithm}" -> DynamoValue.fromString(eachChecksum.fingerprint)).toMap
    }.toDynamoValue

  def writeLockTable(lockTable: IngestLockTable): DynamoValue =
    DynamoObject {
      Map(
        assetId -> DynamoValue.fromString(lockTable.assetId.toString),
        groupId -> DynamoValue.fromString(lockTable.groupId),
        message -> DynamoValue.fromString(lockTable.message)
      )
    }.toDynamoValue
}
