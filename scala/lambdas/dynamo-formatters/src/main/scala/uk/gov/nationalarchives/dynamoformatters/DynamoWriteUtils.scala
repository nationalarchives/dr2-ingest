package uk.gov.nationalarchives.dynamoformatters

import org.scanamo.{DynamoObject, DynamoValue}
import org.scanamo.generic.semiauto.FieldName
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{archiveFolderItemFormat, *}

object DynamoWriteUtils {

  private def commonFieldsToMap(item: DynamoItem): Map[String, DynamoValue] = {
    val optionalFields: Map[FieldName, DynamoValue] = Map(
      "title" -> item.potentialTitle.map(DynamoValue.fromString),
      "description" -> item.potentialDescription.map(DynamoValue.fromString),
      "parentPath" -> item.potentialParentPath.map(DynamoValue.fromString)
    ).flatMap {
      case (fieldName, Some(potentialValue)) => Map(fieldName -> potentialValue)
      case _                                 => Map.empty
    }
    Map(
      "batchId" -> DynamoValue.fromString(item.batchId),
      "id" -> DynamoValue.fromString(item.id.toString),
      "type" -> DynamoValue.fromString(item.`type`.toString)
    ) ++ item.identifiers.map(id => s"id_${id.identifierName}" -> DynamoValue.fromString(id.value)).toMap ++
      optionalFields
  }

  def writeArchiveFolderItem(archiveFolderDynamoItem: ArchiveFolderDynamoItem): DynamoValue =
    DynamoObject {
      commonFieldsToMap(archiveFolderDynamoItem) ++
        Map(name -> DynamoValue.fromString(archiveFolderDynamoItem.name))
    }.toDynamoValue

  def writeContentFolderItem(contentFolderDynamoItem: ContentFolderDynamoItem): DynamoValue =
    DynamoObject {
      commonFieldsToMap(contentFolderDynamoItem) ++
        Map(name -> DynamoValue.fromString(contentFolderDynamoItem.name))
    }.toDynamoValue

  def writeAssetItem(assetDynamoItem: AssetDynamoItem): DynamoValue =
    DynamoObject {
      commonFieldsToMap(assetDynamoItem) ++
        Map(
          "transferringBody" -> DynamoValue.fromString(assetDynamoItem.transferringBody),
          "transferCompleteDatetime" -> DynamoValue.fromString(assetDynamoItem.transferCompleteDatetime.toString),
          "upstreamSystem" -> DynamoValue.fromString(assetDynamoItem.upstreamSystem),
          "digitalAssetSource" -> DynamoValue.fromString(assetDynamoItem.digitalAssetSource),
          "originalFiles" -> DynamoValue.fromStrings(assetDynamoItem.originalFiles.map(_.toString)),
          "originalMetadataFiles" -> DynamoValue.fromStrings(assetDynamoItem.originalMetadataFiles.map(_.toString)),
          ingestedPreservica -> DynamoValue.fromString(assetDynamoItem.ingestedPreservica.toString),
          ingestedCustodialCopy -> DynamoValue.fromString(assetDynamoItem.ingestedCustodialCopy.toString)
        ) ++ (if (assetDynamoItem.skipIngest) Map("skipIngest" -> DynamoValue.fromBoolean(assetDynamoItem.skipIngest)) else Map())
        ++ assetDynamoItem.correlationId.map(id => Map(correlationId -> DynamoValue.fromString(id))).getOrElse(Map())
        ++ assetDynamoItem.potentialDigitalAssetSubtype.map(subType => Map(digitalAssetSubtype -> DynamoValue.fromString(subType))).getOrElse(Map())
    }.toDynamoValue

  def writeFileItem(fileDynamoItem: FileDynamoItem): DynamoValue =
    DynamoObject {
      commonFieldsToMap(fileDynamoItem) ++
        fileDynamoItem.potentialFileExtension.map(extension => Map(fileExtension -> DynamoValue.fromString(extension))).getOrElse(Map()) ++
        Map(
          name -> DynamoValue.fromString(fileDynamoItem.name),
          sortOrder -> DynamoValue.fromNumber[Int](fileDynamoItem.sortOrder),
          fileSize -> DynamoValue.fromNumber[Long](fileDynamoItem.fileSize),
          representationType -> DynamoValue.fromString(fileDynamoItem.representationType.toString),
          representationSuffix -> DynamoValue.fromNumber(fileDynamoItem.representationSuffix),
          ingestedPreservica -> DynamoValue.fromString(fileDynamoItem.ingestedPreservica.toString),
          location -> DynamoValue.fromString(fileDynamoItem.location.toString),
          ingestedCustodialCopy -> DynamoValue.fromString(fileDynamoItem.ingestedCustodialCopy.toString)
        ) ++ fileDynamoItem.checksums.map(eachChecksum => s"${checksumPrefix}${eachChecksum.algorithm}" -> DynamoValue.fromString(eachChecksum.fingerprint)).toMap
    }.toDynamoValue

  def writeLockTableItem(lockTableItem: IngestLockTableItem): DynamoValue =
    DynamoObject {
      Map(
        assetId -> DynamoValue.fromString(lockTableItem.assetId.toString),
        groupId -> DynamoValue.fromString(lockTableItem.groupId),
        message -> DynamoValue.fromString(lockTableItem.message)
      )
    }.toDynamoValue
}
