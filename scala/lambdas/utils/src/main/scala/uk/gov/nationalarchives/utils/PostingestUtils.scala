package uk.gov.nationalarchives.utils

import io.circe.{Decoder, Encoder, HCursor}
import io.circe.generic.semiauto.deriveEncoder
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.PostIngestStateTableItem

import java.util.UUID

object PostingestUtils {
  given Encoder[OutputQueueMessage] = deriveEncoder[OutputQueueMessage]

  case class OutputQueueMessage(assetId: UUID, batchId: String, resultAttrName: String, payload: String)

  sealed trait Queue extends Product {
    def queueAlias: String
    def queueOrder: Int
    def queueUrl: String
    def resultAttrName: String = s"result_$queueAlias"
    def getResult: PostIngestStateTableItem => Option[String]
  }

  case class CCQueue(queueAlias: String, queueOrder: Int, queueUrl: String) extends Queue {
    val getResult: PostIngestStateTableItem => Option[String] = tableItem => tableItem.potentialResultCC
  }

  given Decoder[Queue] = (c: HCursor) =>
    for {
      queueAlias <- c.downField("queueAlias").as[String]
      queueOrder <- c.downField("queueOrder").as[Int]
      queueUrl <- c.downField("queueUrl").as[String]
    } yield queueAlias match
      case "CC" => CCQueue(queueAlias, queueOrder, queueUrl)

}
