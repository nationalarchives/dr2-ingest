package uk.gov.nationalarchives

import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.amazonaws.services.lambda.runtime.events.{SQSEvent, ScheduledEvent}
import io.circe.{Decoder, HCursor}
import org.joda.time.DateTime

import scala.jdk.CollectionConverters._

object EventDecoders {

  given Decoder[ScheduledEvent] = (c: HCursor) => for {
      time <- c.downField("time").as[String]
    } yield {
      val scheduledEvent = new ScheduledEvent()
      scheduledEvent.setTime(DateTime.parse(time))
      scheduledEvent
    }

  given Decoder[SQSMessage] = (c: HCursor) => for {
    body <- c.downField("body").as[String]
  } yield {
    val message = new SQSMessage()
    message.setBody(body)
    message
  }

  given Decoder[SQSEvent] = (c: HCursor) => for {
    records <- c.downField("Records").as[List[SQSMessage]]
  } yield {
    val event = new SQSEvent()
    event.setRecords(records.asJava)
    event

  }
}
