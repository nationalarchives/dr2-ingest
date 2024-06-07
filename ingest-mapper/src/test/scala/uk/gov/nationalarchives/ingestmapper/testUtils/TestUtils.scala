package uk.gov.nationalarchives.ingestmapper.testUtils

import ujson.Obj
import uk.gov.nationalarchives.ingestmapper.MetadataService.Type
import upickle.default._

import java.util.UUID
object TestUtils {
  given Reader[DynamoSRequestField] = macroR[DynamoSRequestField]
  given Reader[DynamoNRequestField] = macroR[DynamoNRequestField]
  given Reader[DynamoItem] = reader[Obj].map[DynamoItem] { json =>
    val items: Map[String, DynamoField] = json.value.toMap.view.mapValues { value =>
      if (value.obj.contains("S")) {
        DynamoSRequestField(value.obj("S").str)
      } else if (value.obj.contains("L")) {
        DynamoLRequestField(value.obj("L").arr.map(v => v("S").str).toList)
      } else {
        DynamoNRequestField(value.obj("N").str.toLong)
      }
    }.toMap
    DynamoItem(items)
  }
  given Reader[DynamoTableItem] = macroR[DynamoTableItem]
  given Reader[DynamoPutRequest] = macroR[DynamoPutRequest]
  given Reader[DynamoRequestItem] = macroR[DynamoRequestItem]
  given Reader[DynamoRequestBody] = macroR[DynamoRequestBody]

  trait DynamoField

  case class DynamoLRequestField(L: List[String]) extends DynamoField

  case class DynamoSRequestField(S: String) extends DynamoField

  case class DynamoNRequestField(N: Long) extends DynamoField

  case class DynamoTable(
      batchId: String,
      id: UUID,
      parentPath: String,
      name: String,
      `type`: Type,
      title: String,
      description: String,
      id_Code: Option[String],
      fileSize: Option[Long] = None,
      checksumSha256: Option[String] = None,
      fileExtension: Option[String] = None,
      customMetadataAttribute1: Option[String] = None,
      customMetadataAttribute2: Option[String] = None,
      attributeUniqueToBagInfo: Option[String] = None,
      originalFiles: List[String] = Nil,
      originalMetadataFiles: List[String] = Nil
  )

  case class DynamoItem(
      items: Map[String, DynamoField]
  )

  case class DynamoTableItem(PutRequest: DynamoPutRequest)

  case class DynamoPutRequest(Item: DynamoItem)

  case class DynamoRequestItem(test: List[DynamoTableItem])

  case class DynamoRequestBody(RequestItems: DynamoRequestItem)

}
