package uk.gov.nationalarchives.ingestfailurenotifications

import cats.effect.IO
import io.circe.DecodingFailure.Reason.CustomReason
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, DecodingFailure, HCursor}
import io.circe.parser.decode
import pureconfig.ConfigReader
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.ingestfailurenotifications.Lambda.{*, given}
import uk.gov.nationalarchives.DADynamoDBClient.given
import org.scanamo.syntax.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{IngestLockTableItem, groupId}
import uk.gov.nationalarchives.utils.ExternalUtils.MessageStatus.IngestError
import uk.gov.nationalarchives.utils.EventCodecs.given
import uk.gov.nationalarchives.utils.ExternalUtils.MessageType.IngestUpdate
import uk.gov.nationalarchives.utils.ExternalUtils.{OutputMessage, OutputParameters, OutputProperties}

import java.time.Instant
import java.util.UUID

class Lambda extends LambdaRunner[SfnEvent, Unit, Config, Dependencies]:
  override def handler: (SfnEvent, Config, Dependencies) => IO[Unit] = (event, config, dependencies) =>
    for {
      items <- dependencies.dynamoClient
        .queryItems[IngestLockTableItem](config.lockTableName, groupId === event.detail.input.groupId, Option(config.lockTableGsiName))
      messages = items.map { item =>
        OutputMessage(
          OutputProperties(event.detail.input.batchId, dependencies.uuidGenerator(), None, Instant.now, IngestUpdate),
          OutputParameters(item.assetId, IngestError)
        )
      }
      _ <- dependencies.snsClient.publish(config.topicArn)(messages)
    } yield ()

  override def dependencies(config: Config): IO[Dependencies] =
    IO.pure(Dependencies(DASNSClient[IO](), DADynamoDBClient[IO](), () => UUID.randomUUID))
end Lambda

object Lambda:
  given Decoder[SfnInput] = deriveDecoder[SfnInput]

  given Decoder[SfnDetail] = (c: HCursor) =>
    for {
      inputString <- c.downField("input").as[String]
      input <- decode[SfnInput](inputString).left.map(err => DecodingFailure(CustomReason(err.getMessage), c))
    } yield SfnDetail(input)

  given Decoder[SfnEvent] = (c: HCursor) =>
    for {
      detail <- c.downField("detail").as[SfnDetail]
    } yield SfnEvent(detail)

  case class SfnInput(groupId: String, batchId: String)

  case class SfnDetail(input: SfnInput)

  case class SfnEvent(detail: SfnDetail)

  case class Config(topicArn: String, lockTableName: String, lockTableGsiName: String) derives ConfigReader

  case class Dependencies(snsClient: DASNSClient[IO], dynamoClient: DADynamoDBClient[IO], uuidGenerator: () => UUID)
