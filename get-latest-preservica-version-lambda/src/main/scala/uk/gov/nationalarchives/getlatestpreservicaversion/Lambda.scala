package uk.gov.nationalarchives.getlatestpreservicaversion

import cats.effect.IO
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import io.circe.generic.auto.*
import org.scanamo.generic.auto.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.utils.EventDecoders.given
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.getlatestpreservicaversion.Lambda.*
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}

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
      _ <-
        if (latestPreservicaVersion != versionWeAreUsing) {
          val newVersion = List(
            LatestPreservicaVersionMessage(
              s"Preservica has upgraded to version $latestPreservicaVersion; we are using $versionWeAreUsing",
              latestPreservicaVersion
            )
          )
          dependencies.snsClient.publish[LatestPreservicaVersionMessage](config.snsArn)(newVersion)
        }.map(_ => log("Latest version of Preservica published to SNS"))
        else IO.unit
    } yield ()
  }

  override def dependencies(config: Config): IO[Dependencies] = for {
    entitiesClient <- Fs2Client.entityClient(config.demoApiUrl, config.secretName)
  } yield Dependencies(entitiesClient, DASNSClient[IO](), DADynamoDBClient[IO]())
}

object Lambda {
  case class Config(
      demoApiUrl: String,
      secretName: String,
      snsArn: String,
      currentPreservicaVersionTableName: String
  ) derives ConfigReader

  case class PartitionKey(id: String)

  case class GetDr2PreservicaVersionResponse(version: Float)
  case class LatestPreservicaVersionMessage(message: String, version: Float)

  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], snsClient: DASNSClient[IO], dynamoDBClient: DADynamoDBClient[IO])
}
