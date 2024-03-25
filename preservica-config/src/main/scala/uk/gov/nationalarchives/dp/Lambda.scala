package uk.gov.nationalarchives.dp

import cats.effect.IO
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import org.typelevel.log4cats.SelfAwareStructuredLogger
import pureconfig.generic.auto._
import uk.gov.nationalarchives.EventDecoders._
import uk.gov.nationalarchives.dp.FileProcessors._
import uk.gov.nationalarchives.dp.Lambda.{Config, Dependencies}
import uk.gov.nationalarchives.dp.client.AdminClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.{DAS3Client, LambdaRunner}

class Lambda extends LambdaRunner[SQSEvent, Unit, Config, Dependencies] {

  implicit val configLogger: SelfAwareStructuredLogger[IO] = logger
  override def handler: (
      SQSEvent,
      Config,
      Dependencies
  ) => IO[Unit] = (input, _, dependencies) =>
    for {
      s3Objects <- s3ObjectsFromEvent(input)
      _ <- logger.info(s"Fetched ${s3Objects.length} objects from ${s3Objects.head.bucket}")
      s3Client = DAS3Client[IO]()
      _ <- processFiles(dependencies.adminClient, s3Client, s3Objects)
    } yield ()

  override def dependencies(config: Config): IO[Dependencies] = Fs2Client.adminClient(config.preservicaUrl, config.secretName).map(Dependencies.apply)
}
object Lambda {
  case class Config(preservicaUrl: String, secretName: String)

  case class Dependencies(adminClient: AdminClient[IO])
}
