package uk.gov.nationalarchives.getlatestpreservicaversion

import cats.effect.IO
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import io.circe.generic.auto.*
import org.scanamo.generic.auto.*
import pureconfig.ConfigReader
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.utils.EventCodecs.given
import uk.gov.nationalarchives.utils.ExternalUtils.DetailType.DR2DevMessage
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.getlatestpreservicaversion.Lambda.*
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.{DADynamoDBClient, DAEventBridgeClient}

class Lambda extends LambdaRunner[ScheduledEvent, Unit, Config, Dependencies] {

  private val lowImpactEndpoint = "entities/by-identifier?type=tnaTest&value=getLatestPreservicaVersion"

  override def handler: (ScheduledEvent, Config, Dependencies) => IO[Unit] = { (_, config, dependencies) =>
    for {
      log <- IO(log(Map("endpointToCheck" -> lowImpactEndpoint)))

      versionWeAreUsingResponses <- dependencies.dynamoDBClient.getItems[GetDr2PreservicaVersionResponse, PartitionKey](
        List(PartitionKey("DR2PreservicaVersion")),
        config.currentPreservicaVersionTableName
      )
      versionWeAreUsing <- IO.fromOption(versionWeAreUsingResponses.headOption.map(_.version))(
        new RuntimeException("The version of Preservica we are using was not found")
      )
      _ <- log(s"Retrieved the version of Preservica that we are using: v$versionWeAreUsing")

      latestPreservicaVersion <- dependencies.entityClient.getPreservicaNamespaceVersion(lowImpactEndpoint)
      _ <- log(s"Retrieved the latest version of Preservica: v$latestPreservicaVersion")
      _ <- IO.whenA(latestPreservicaVersion != versionWeAreUsing) {
        val msg = s"Preservica has upgraded to version $latestPreservicaVersion; we are using $versionWeAreUsing"
        dependencies.eventBridgeClient.publishEventToEventBridge(getClass.getName, DR2DevMessage, Detail(msg)).void
      }
    } yield ()
  }

  override def dependencies(config: Config): IO[Dependencies] = for {
    entitiesClient <- Fs2Client.entityClient(config.demoApiUrl, config.secretName)
  } yield Dependencies(entitiesClient, DAEventBridgeClient[IO](), DADynamoDBClient[IO]())
}

object Lambda {
  case class Config(
      demoApiUrl: String,
      secretName: String,
      snsArn: String,
      currentPreservicaVersionTableName: String
  ) derives ConfigReader

  case class Detail(slackMessage: String)

  case class PartitionKey(id: String)

  case class GetDr2PreservicaVersionResponse(version: Float)

  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], eventBridgeClient: DAEventBridgeClient[IO], dynamoDBClient: DADynamoDBClient[IO])
}
