package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import io.circe.Encoder
import org.scanamo.generic.auto.genericDerivedFormat
import org.typelevel.log4cats.slf4j.Slf4jFactory
import org.typelevel.log4cats.{LoggerName, SelfAwareStructuredLogger}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.Lambda._
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import upickle.default._

import java.util.UUID

class Lambda extends RequestHandler[ScheduledEvent, Unit] {

  implicit val inputReader: Reader[Input] = macroR[Input]
  val dADynamoDBClient: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  val dASnsDBClient: DASNSClient[IO] = DASNSClient[IO]()

  implicit val enc: Encoder[LatestPreservicaVersionMessage] =
    Encoder.forProduct2("message", "version")(message => (message.message, message.version))

  implicit val loggerName: LoggerName = LoggerName("Get Latest Preservica Version")
  private val logger: SelfAwareStructuredLogger[IO] = Slf4jFactory.create[IO].getLogger

  lazy val entitiesClientIO: IO[EntityClient[IO, Fs2Streams[IO]]] = configIo.flatMap { config =>
    Fs2Client.entityClient(config.apiUrl, config.secretName)
  }
  private val lowImpactEndpoint = "by-identifier?type=tnaTest&value=getLatestPreservicaVersion"
  private val configIo: IO[Config] = ConfigSource.default.loadF[IO, Config]()
  override def handleRequest(event: ScheduledEvent, context: Context): Unit = {
    for {
      config <- configIo
      logCtx: Map[String, String] = Map("endpointToCheck" -> lowImpactEndpoint)
      log = logger.info(logCtx)(_)

      versionWeAreUsingResponses <- dADynamoDBClient.getItems[GetDr2PreservicaVersionResponse, PartitionKey](
        List(PartitionKey("DR2PreservicaVersion")),
        config.currentPreservicaVersionTableName
      )
      _ <- log("Retrieved the version of Preservica that we are using")

      entitiesClient <- entitiesClientIO
      latestPreservicaVersion <- entitiesClient.getPreservicaNamespaceVersion(lowImpactEndpoint)
      _ <- log("Retrieved version of Preservica that Preservica are using")

      versionWeAreUsing = versionWeAreUsingResponses.head.version
      _ <-
        if (latestPreservicaVersion != versionWeAreUsing) {
          val newVersion = List(
            LatestPreservicaVersionMessage(
              s"Preservica has upgraded to version $latestPreservicaVersion; we are using $versionWeAreUsing",
              latestPreservicaVersion
            )
          )
          dASnsDBClient.publish[LatestPreservicaVersionMessage](config.snsArn)(newVersion)
        }.map(_ => log("Latest version of Preservica published to SNS"))
        else IO.unit
    } yield ()
  }.onError(logLambdaError).unsafeRunSync()

  private def logLambdaError(error: Throwable): IO[Unit] =
    logger.error(error)("Error running Get Latest Preservica Version")
}

object Lambda {
  case class Input(executionId: String, batchId: String, assetId: UUID)

  private case class Config(
      apiUrl: String,
      secretName: String,
      snsArn: String,
      currentPreservicaVersionTableName: String
  )

  case class PartitionKey(id: String)

  sealed trait PreservicaVersion {
    val version: Float
  }
  case class GetDr2PreservicaVersionResponse(version: Float) extends PreservicaVersion
  case class LatestPreservicaVersionMessage(message: String, version: Float) extends PreservicaVersion
}
