package uk.gov.nationalarchives.ingestmapper.testUtils

import uk.gov.nationalarchives.ingestmapper.MetadataService.Type

import java.util.UUID
object TestUtils {

  case class DynamoFilesTableItem(
      batchId: String,
      id: UUID,
      parentPath: String,
      name: String,
      `type`: Type,
      title: String,
      description: String,
      id_Code: Option[String],
      childCount: Int,
      ttl: Long,
      fileSize: Option[Long] = None,
      checksumSha256: Option[String] = None,
      fileExtension: Option[String] = None,
      customMetadataAttribute1: Option[String] = None,
      originalMetadataFiles: List[String] = Nil
  )
}
