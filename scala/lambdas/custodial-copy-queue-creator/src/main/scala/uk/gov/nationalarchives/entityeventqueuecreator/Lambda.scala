package uk.gov.nationalarchives.entityeventqueuecreator

import cats.effect.*
import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.circe.{Decoder, Encoder, HCursor, Json, JsonObject}
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.DASQSClient
import uk.gov.nationalarchives.entityeventqueuecreator.Lambda.*
import uk.gov.nationalarchives.entityeventqueuecreator.Lambda.MessageBody.*
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.utils.EventDecoders.given
import io.circe.parser.decode
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType

import scala.jdk.CollectionConverters.*
import java.util.UUID

class Lambda extends LambdaRunner[SQSEvent, Unit, Config, Dependencies]:

  override def handler: (SQSEvent, Config, Dependencies) => IO[Unit] = { (sqsEvent, config, dependencies) =>
    sqsEvent.getRecords.asScala.toList.parTraverse { record =>
      for {
        messageBody <- IO.fromEither(decode[MessageBody](record.getBody))
        messageGroupId <- messageBody match
          case IoMessageBody(id, _) => IO.pure(id.some)
          case CoMessageBody(id, deleted) =>
            if deleted then
              IO.pure(id.some)
            else
              dependencies.entityClient.getEntity(id, EntityType.ContentObject).map(_.parent match
                case Some(parent) => parent.some
                case None => id.some)
          case SoMessageBody(id, _) => IO.none
        _ <- IO.whenA(messageGroupId.nonEmpty)(dependencies.sqsClient.sendMessage(config.outputQueue)(messageBody, messageGroupId.map(_.toString)).void)
        _ <- dependencies.sqsClient.deleteMessage(config.inputQueue, record.getReceiptHandle)
      } yield ()
    }.void
  }

  override def dependencies(config: Config): IO[Dependencies] =
    Fs2Client.entityClient(config.apiUrl, config.secretName).map(entityClient => Dependencies(entityClient, DASQSClient[IO]()))

object Lambda:

  case class Config(apiUrl: String, secretName: String, inputQueue: String, outputQueue: String) derives ConfigReader
  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], sqsClient: DASQSClient[IO])

  private def toJson(id: UUID, deleted: Boolean, messageType: String): Json =
    Json.fromJsonObject(JsonObject(("id", Json.fromString(s"$messageType:$id")), ("deleted", Json.fromBoolean(deleted))))

  given Encoder[MessageBody] =
    case IoMessageBody(id, deleted) => toJson(id, deleted, "io")
    case CoMessageBody(id, deleted) => toJson(id, deleted, "co")
    case SoMessageBody(id, deleted) => toJson(id, deleted, "so")

  given Decoder[MessageBody] = (c: HCursor) =>
    for {
      id <- c.downField("id").as[String]
      deleted <- c.downField("deleted").as[Boolean]
    } yield {
      val typeAndRef = id.split(":")
      val ref = UUID.fromString(typeAndRef.last)
      val entityType = typeAndRef.head
      entityType match {
        case "io" => IoMessageBody(ref, deleted)
        case "co" => CoMessageBody(ref, deleted)
        case "so" => SoMessageBody(ref, deleted)
      }
    }

  enum MessageBody(val id: UUID, val deleted: Boolean):
    case IoMessageBody(override val id: UUID, override val deleted: Boolean) extends MessageBody(id, deleted)
    case CoMessageBody(override val id: UUID, override val deleted: Boolean) extends MessageBody(id, deleted)
    case SoMessageBody(override val id: UUID, override val deleted: Boolean) extends MessageBody(id, deleted)

