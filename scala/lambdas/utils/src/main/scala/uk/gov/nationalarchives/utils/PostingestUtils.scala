package uk.gov.nationalarchives.utils

import io.circe.{Decoder, Encoder, HCursor, Json}
import io.circe.generic.semiauto.deriveEncoder
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.PostIngestStateTableItem
import uk.gov.nationalarchives.utils.ExternalUtils.MessageStatus
import uk.gov.nationalarchives.utils.ExternalUtils.MessageStatus.{IngestedCCDisk, IngestedPreservation}

import java.util.UUID

object PostingestUtils {
  given Encoder[OutputQueueMessage] = deriveEncoder[OutputQueueMessage]

  case class OutputQueueMessage(assetId: UUID, batchId: String, resultAttrName: String, payload: Json)

  sealed trait Queue extends Product {
    def queueAlias: String
    def queueOrder: Int
    def queueUrl: String
    def messageStatus: MessageStatus
    def resultAttrName: String = s"result_$queueAlias"
    def getResult(item: PostIngestStateTableItem): Option[String]
    def isResultChangeOnTheSameQueue(oldItem: PostIngestStateTableItem, newItem: PostIngestStateTableItem): Boolean =
      oldItem.potentialQueue.contains(queueAlias) && newItem.potentialQueue.contains(queueAlias) && getResult(oldItem) != getResult(newItem)
  }

  case class CCQueue(queueAlias: String, queueOrder: Int, queueUrl: String, messageStatus: MessageStatus) extends Queue {
    def getResult(item: PostIngestStateTableItem): Option[String] = item.potentialResultCC
  }

  case class TCQueue(queueAlias: String, queueOrder: Int, queueUrl: String, messageStatus: MessageStatus) extends Queue {
    def getResult(item: PostIngestStateTableItem): Option[String] = item.potentialResultTC
  }

  given Decoder[Queue] = (c: HCursor) =>
    for {
      queueAlias <- c.downField("queueAlias").as[String]
      queueOrder <- c.downField("queueOrder").as[Int]
      queueUrl <- c.downField("queueUrl").as[String]
    } yield queueAlias match
      case "CC" => CCQueue(queueAlias, queueOrder, queueUrl, IngestedPreservation)
      case "TC" => TCQueue(queueAlias, queueOrder, queueUrl, IngestedCCDisk)
      case _    => throw new IllegalArgumentException(s"Unsupported queue, '$queueAlias' found in the configuration.")
}
