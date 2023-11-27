package uk.gov.nationalarchives.dp

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.dp.FileProcessors._
import uk.gov.nationalarchives.dp.Lambda.Config
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client._

class Lambda extends RequestHandler[SQSEvent, Unit] {

  override def handleRequest(input: SQSEvent, context: Context): Unit = {
    val result = for {
      config <- ConfigSource.default.loadF[IO, Config]()
      s3Objects <- s3ObjectsFromEvent(input)
      preservicaClient <- adminClient(config.preservicaUrl, config.secretName)
      s3Client = DAS3Client[IO]()
      _ <- processFiles(preservicaClient, s3Client, s3Objects)
    } yield ()
    result.unsafeRunSync()
  }
}
object Lambda {
  case class Config(preservicaUrl: String, secretName: String)
}
