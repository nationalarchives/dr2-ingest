package uk.gov.nationalarchives.utils

import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.amazonaws.services.lambda.runtime.events.{SQSBatchResponse, SQSEvent, ScheduledEvent}
import io.circe.generic.semiauto.deriveEncoder
import io.circe.{Decoder, Encoder, HCursor, Json, JsonObject}
import org.joda.time.DateTime
import uk.gov.nationalarchives.utils.ExternalUtils.*

import scala.jdk.CollectionConverters.*

object EventCodecs {
  given Encoder[MessageStatus] = (messageStatus: MessageStatus) => Json.fromString(messageStatus.value)

  given Encoder[MessageType] = (messageType: MessageType) => Json.fromString(messageType.toString)

  given Encoder[OutputProperties] = deriveEncoder[OutputProperties]

  given Encoder[OutputParameters] = deriveEncoder[OutputParameters]

  given Encoder[OutputMessage] = deriveEncoder[OutputMessage]

  given Decoder[ScheduledEvent] = (c: HCursor) =>
    for {
      time <- c.downField("time").as[String]
    } yield {
      val scheduledEvent = new ScheduledEvent()
      scheduledEvent.setTime(DateTime.parse(time))
      scheduledEvent
    }

  given Decoder[SQSMessage] = (c: HCursor) =>
    for {
      body <- c.downField("body").as[String]
    } yield {
      val message = new SQSMessage()
      message.setBody(body)
      message
    }

  given Decoder[SQSEvent] = (c: HCursor) =>
    for {
      records <- c.downField("Records").as[List[SQSMessage]]
    } yield {
      val event = new SQSEvent()
      event.setRecords(records.asJava)
      event

    }

  given Encoder[SQSBatchResponse] = new Encoder[SQSBatchResponse]:
    override def apply(batchResponse: SQSBatchResponse): Json = {
      val batchItemFailures = batchResponse.getBatchItemFailures.asScala.map { failure =>
        JsonObject("itemIdentifier" -> Json.fromString(failure.getItemIdentifier)).toJson
      }.toList
      JsonObject("batchItemFailures" -> Json.fromValues(batchItemFailures)).toJson
    }
}
