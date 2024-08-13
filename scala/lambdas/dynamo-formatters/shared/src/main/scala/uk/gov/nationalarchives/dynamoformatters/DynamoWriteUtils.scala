package uk.gov.nationalarchives.dynamoformatters

import dynosaur.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import cats.syntax.all.*
import dynosaur.Schema.fields

import scala.jdk.CollectionConverters.*
import java.time.OffsetDateTime
import java.util.UUID

object DynamoWriteUtils {

  private def commonFieldsToMap(table: DynamoTable): Map[String, DynamoValue] = {
    val optionalFields: Map[String, DynamoValue] = Map(
      "title" -> table.title.map(DynamoValue.s),
      "description" -> table.description.map(DynamoValue.s),
      "parentPath" -> table.parentPath.map(DynamoValue.s)
    ).flatMap {
      case (string, Some(potentialValue)) => Map(string -> potentialValue)
      case _                                 => Map.empty
    }
    Map(
      "batchId" -> DynamoValue.s(table.batchId),
      "id" -> DynamoValue.s(table.id.toString),
      "name" -> DynamoValue.s(table.name),
      "type" -> DynamoValue.s(table.`type`.toString)
    ) ++ table.identifiers.map(id => s"id_${id.identifierName}" -> DynamoValue.s(id.value)).toMap ++
      optionalFields
  }

  def writeArchiveFolderTable(archiveFolderDynamoTable: ArchiveFolderDynamoTable): DynamoValue =
    DynamoValue.attributeMap(commonFieldsToMap(archiveFolderDynamoTable).view.mapValues(_.value).toMap.asJava)

  def writeContentFolderTable(contentFolderDynamoTable: ContentFolderDynamoTable): DynamoValue =
    DynamoValue.attributeMap(commonFieldsToMap(contentFolderDynamoTable).view.mapValues(_.value).toMap.asJava)



//  val schema = Schema.record[AssetDynamoTable] { field =>
//    (
//      field(batchId, _.batchId),
//      field(id, _.id),
//      field.opt(parentPath, _.parentPath),
//      field(name, _.name),
//      field(typeField, _.`type`),
//      field.opt(title, _.title),
//      field.opt(description, _.description),
//      field(transferringBody, _.transferringBody),
//      field(transferCompleteDatetime, _.transferCompleteDatetime),
//      field(upstreamSystem, _.upstreamSystem),
//      field(digitalAssetSource, _.digitalAssetSource),
//      field(digitalAssetSubtype, _.digitalAssetSubtype),
//      field(originalFiles, _.originalFiles),
//      field(originalMetadataFiles, _.originalMetadataFiles),
//      field(ingestedPreservica, _.ingestedPreservica),
//      field(ingestedCustodialCopy, _.ingestedCustodialCopy),
//      field(childCount, _.childCount),
//      field(skipIngest, _.skipIngest),
//      field.opt(correlationId, _.correlationId)
//    ).mapN { (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s) =>
//
//    }
//  }

  def writeAssetTable(assetDynamoTable: AssetDynamoTable): DynamoValue =
    DynamoValue.attributeMap(tableToMap(assetDynamoTable).view.mapValues(_.value).toMap.asJava)
    

  private def tableToMap(assetDynamoTable: AssetDynamoTable) = {
    commonFieldsToMap(assetDynamoTable) ++
      Map(
        "transferringBody" -> DynamoValue.s(assetDynamoTable.transferringBody),
        "transferCompleteDatetime" -> DynamoValue.s(assetDynamoTable.transferCompleteDatetime.toString),
        "upstreamSystem" -> DynamoValue.s(assetDynamoTable.upstreamSystem),
        "digitalAssetSource" -> DynamoValue.s(assetDynamoTable.digitalAssetSource),
        "digitalAssetSubtype" -> DynamoValue.s(assetDynamoTable.digitalAssetSubtype),
        "originalFiles" -> DynamoValue.l(assetDynamoTable.originalFiles.map(_.toString).map(DynamoValue.s)),
        "originalMetadataFiles" -> DynamoValue.l(assetDynamoTable.originalMetadataFiles.map(_.toString).map(DynamoValue.s)),
        ingestedPreservica -> DynamoValue.s(assetDynamoTable.ingestedPreservica.toString),
        ingestedCustodialCopy -> DynamoValue.s(assetDynamoTable.ingestedCustodialCopy.toString)
      ) ++ (if (assetDynamoTable.skipIngest) Map("skipIngest" -> DynamoValue.bool(assetDynamoTable.skipIngest)) else Map())
      ++ assetDynamoTable.correlationId.map(id => Map(correlationId -> DynamoValue.s(id))).getOrElse(Map())
  }

  def writeFileTable(fileDynamoTable: FileDynamoTable): DynamoValue =
    DynamoValue.attributeMap {
      (commonFieldsToMap(fileDynamoTable) ++
        Map(
          sortOrder -> DynamoValue.n[Int](fileDynamoTable.sortOrder),
          fileSize -> DynamoValue.n[Long](fileDynamoTable.fileSize),
          checksumSha256 -> DynamoValue.s(fileDynamoTable.checksumSha256),
          fileExtension -> DynamoValue.s(fileDynamoTable.fileExtension),
          representationType -> DynamoValue.s(fileDynamoTable.representationType.toString),
          representationSuffix -> DynamoValue.n(fileDynamoTable.representationSuffix),
          ingestedPreservica -> DynamoValue.s(fileDynamoTable.ingestedPreservica.toString),
          location -> DynamoValue.s(fileDynamoTable.location.toString),
          ingestedCustodialCopy -> DynamoValue.s(fileDynamoTable.ingestedCustodialCopy.toString)
        )).view.mapValues(_.value).toMap.asJava
    }

  def writeLockTable(lockTable: IngestLockTable): DynamoValue =
    DynamoValue.attributeMap(
      Map(
        assetId -> DynamoValue.s(lockTable.assetId.toString).value,
        groupId -> DynamoValue.s(lockTable.groupId).value,
        message -> DynamoValue.s(lockTable.message).value
      ).asJava
    )
}
