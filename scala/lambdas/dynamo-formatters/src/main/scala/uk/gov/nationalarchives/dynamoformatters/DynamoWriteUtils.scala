package uk.gov.nationalarchives.dynamoformatters

import org.scanamo.generic.semiauto.FieldName
import org.scanamo.{DynamoObject, DynamoValue}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*

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
          "upstreamSystem" -> DynamoValue.fromString(assetDynamoItem.upstreamSystem),
          "digitalAssetSource" -> DynamoValue.fromString(assetDynamoItem.digitalAssetSource),
          "originalFiles" -> DynamoValue.fromStrings(assetDynamoItem.originalFiles.map(_.toString)),
          "originalMetadataFiles" -> DynamoValue.fromStrings(assetDynamoItem.originalMetadataFiles.map(_.toString)),
          ingestedPreservica -> DynamoValue.fromString(assetDynamoItem.ingestedPreservica.toString),
          ingestedCustodialCopy -> DynamoValue.fromString(assetDynamoItem.ingestedCustodialCopy.toString)
        ) ++ (if (assetDynamoItem.skipIngest) Map("skipIngest" -> DynamoValue.fromBoolean(assetDynamoItem.skipIngest)) else Map())
        ++ assetDynamoItem.transferCompleteDatetime.map(tcd => Map(transferCompleteDatetime -> DynamoValue.fromString(tcd.toString))).getOrElse(Map())
        ++ assetDynamoItem.correlationId.map(id => Map(correlationId -> DynamoValue.fromString(id))).getOrElse(Map())
        ++ assetDynamoItem.potentialDigitalAssetSubtype.map(subType => Map(digitalAssetSubtype -> DynamoValue.fromString(subType))).getOrElse(Map())
        ++ assetDynamoItem.transferringBody.map(tb => Map(transferringBody -> DynamoValue.fromString(tb))).getOrElse(Map())
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
        ) ++ fileDynamoItem.checksums.map(eachChecksum => s"$checksumPrefix${eachChecksum.algorithm}" -> DynamoValue.fromString(eachChecksum.fingerprint)).toMap
    }.toDynamoValue

  def writeLockTableItem(lockTableItem: IngestLockTableItem): DynamoValue =
    DynamoObject {
      Map(
        assetId -> DynamoValue.fromString(lockTableItem.assetId.toString),
        groupId -> DynamoValue.fromString(lockTableItem.groupId),
        message -> DynamoValue.fromString(lockTableItem.message),
        createdAt -> DynamoValue.fromString(lockTableItem.createdAt)
      )
    }.toDynamoValue

  def writeIngestQueueTableItem(ingestQueueTableItem: IngestQueueTableItem): DynamoValue =
    DynamoObject {
      Map(
        sourceSystem -> DynamoValue.fromString(ingestQueueTableItem.sourceSystem),
        queuedAt -> DynamoValue.fromString(ingestQueueTableItem.queuedTimeAndExecutionName),
        taskToken -> DynamoValue.fromString(ingestQueueTableItem.taskToken),
        executionName -> DynamoValue.fromString(ingestQueueTableItem.executionName)
      )
    }.toDynamoValue

  def writeStatusTableItem(stateTableItem: PostIngestStateTableItem): DynamoValue =
    DynamoObject {
      Map(
        assetId -> DynamoValue.fromString(stateTableItem.assetId.toString),
        batchId -> DynamoValue.fromString(stateTableItem.batchId),
        input -> DynamoValue.fromString(stateTableItem.input)
      ) ++
        stateTableItem.potentialCorrelationId.map(correlationIdAttrVal => Map(correlationId -> DynamoValue.fromString(correlationIdAttrVal))).getOrElse(Map()) ++
        stateTableItem.potentialQueue.map(queueAttrVal => Map(queue -> DynamoValue.fromString(queueAttrVal))).getOrElse(Map()) ++
        stateTableItem.potentialFirstQueued.map(firstQueuedAttrVal => Map(firstQueued -> DynamoValue.fromString(firstQueuedAttrVal))).getOrElse(Map()) ++
        stateTableItem.potentialLastQueued.map(lastQueuedAttrVal => Map(lastQueued -> DynamoValue.fromString(lastQueuedAttrVal))).getOrElse(Map()) ++
        stateTableItem.potentialResultCC.map(resultCCAttrVal => Map(resultCC -> DynamoValue.fromString(resultCCAttrVal))).getOrElse(Map())
    }.toDynamoValue
}
