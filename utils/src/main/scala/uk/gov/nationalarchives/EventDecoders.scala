package uk.gov.nationalarchives

import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.amazonaws.services.lambda.runtime.events.{SQSEvent, ScheduledEvent}
import io.circe.{Decoder, HCursor}
import org.joda.time.DateTime

import scala.jdk.CollectionConverters._

object EventDecoders {

  implicit val scheduledEventDecoder: Decoder[ScheduledEvent] = (c: HCursor) => for {
    time <- c.downField("time").as[String]
  } yield {
    val scheduledEvent = new ScheduledEvent()
    scheduledEvent.setTime(DateTime.parse(time))
    scheduledEvent
  }

  implicit val sqsMessageDecoder: Decoder[SQSMessage] = (c: HCursor) => for {
    body <- c.downField("body").as[String]
  } yield {
    val message = new SQSMessage()
    message.setBody(body)
    message
  }

  implicit val sqsEventDecoder: Decoder[SQSEvent] = (c: HCursor) => for {
    records <- c.downField("records").as[List[SQSMessage]]
  } yield {
    val event = new SQSEvent()
    event.setRecords(records.asJava)
    event
  }
}
