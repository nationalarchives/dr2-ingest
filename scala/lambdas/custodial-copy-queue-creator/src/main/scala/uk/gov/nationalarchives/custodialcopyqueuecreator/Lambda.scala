package uk.gov.nationalarchives.custodialcopyqueuecreator

import cats.effect.*
import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.circe.{Decoder, Encoder, HCursor, Json, JsonObject}
import pureconfig.ConfigReader
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.DASQSClient
import uk.gov.nationalarchives.custodialcopyqueuecreator.Lambda.*
import uk.gov.nationalarchives.custodialcopyqueuecreator.Lambda.MessageBody.*
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.utils.EventCodecs.given
import io.circe.parser.decode
import uk.gov.nationalarchives.DASQSClient.FifoQueueConfiguration
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType

import scala.jdk.CollectionConverters.*
import java.util.UUID

class Lambda extends LambdaRunner[SQSEvent, Unit, Config, Dependencies]:

  override def handler: (SQSEvent, Config, Dependencies) => IO[Unit] = { (sqsEvent, config, dependencies) =>
    sqsEvent.getRecords.asScala.toList.parTraverse { record =>
      for {
        messageBody <- IO.fromEither(decode[MessageBody](record.getBody))
        potentialMessageGroupId <- messageBody match
          case IoMessageBody(id) => IO.pure(id.some)
          case CoMessageBody(id) =>
            dependencies.entityClient
              .getEntity(id, EntityType.ContentObject)
              .map(_.parent)
          case DeletedMessageBody(id) => IO.pure(id.some)
          case SoMessageBody(id)      => IO.none
        _ <- IO.whenA(potentialMessageGroupId.nonEmpty) {
          val fifoConfiguration = potentialMessageGroupId.map(messageGroupId => FifoQueueConfiguration(messageGroupId.toString, dependencies.uuidGenerator().toString))
          dependencies.sqsClient.sendMessage(config.outputQueue)(messageBody, fifoConfiguration).void
        }
      } yield ()
    }.void
  }

  override def dependencies(config: Config): IO[Dependencies] =
    Fs2Client.entityClient(config.secretName).map(entityClient => Dependencies(entityClient, DASQSClient[IO](), () => UUID.randomUUID))

object Lambda:

  case class Config(secretName: String, outputQueue: String) derives ConfigReader
  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], sqsClient: DASQSClient[IO], uuidGenerator: () => UUID)

  private def toJson(id: UUID, deleted: Boolean, potentialMessageType: Option[String]): Json = {
    val idValue = potentialMessageType.map(messageType => s"$messageType:$id").getOrElse(id.toString)
    Json.fromJsonObject(JsonObject(("id", Json.fromString(idValue)), ("deleted", Json.fromBoolean(deleted))))
  }

  given Encoder[MessageBody] =
    case IoMessageBody(id)      => toJson(id, false, "io".some)
    case CoMessageBody(id)      => toJson(id, false, "co".some)
    case SoMessageBody(id)      => toJson(id, false, "so".some)
    case DeletedMessageBody(id) => toJson(id, true, None)

  given Decoder[MessageBody] = (c: HCursor) =>
    for {
      id <- c.downField("id").as[String]
      deleted <- c.downField("deleted").as[Boolean]
    } yield {
      val typeAndRef = id.split(":")
      typeAndRef match
        case Array(entityType, refString) =>
          val ref = UUID.fromString(refString)
          entityType match {
            case "io" => IoMessageBody(ref)
            case "co" => CoMessageBody(ref)
            case "so" => SoMessageBody(ref)
          }
        case Array(ref) => DeletedMessageBody(UUID.fromString(ref))
    }

  enum MessageBody(val id: UUID, val deleted: Boolean):
    case IoMessageBody(override val id: UUID) extends MessageBody(id, false)
    case CoMessageBody(override val id: UUID) extends MessageBody(id, false)
    case SoMessageBody(override val id: UUID) extends MessageBody(id, false)
    case DeletedMessageBody(override val id: UUID) extends MessageBody(id, true)
