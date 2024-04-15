package uk.gov.nationalarchives

import cats.effect._
import cats.implicits._
import io.circe.generic.auto._
import pureconfig.generic.derivation.default.*
import pureconfig.ConfigReader
import uk.gov.nationalarchives.Lambda._
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient.MonitorsStatus._
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient.MonitorCategory._
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient.MessageStatus._
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient._
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client

import java.util.UUID

class Lambda extends LambdaRunner[Input, StateOutput, Config, Dependencies] {

  private val opexSuccessMessage = "monitor.info.successful.ingest.with.opex"
  private val inProgressStatuses = List(Pending.toString(), Running.toString())
  private val mappedStatuses = Map(
    "Running" -> "Running",
    "Pending" -> "Running",
    "Suspended" -> "Succeeded",
    "Recoverable" -> "Succeeded",
    "Succeeded" -> "Succeeded",
    "Failed" -> "Failed"
  )
  private val monitorCategoryType = Ingest

  override def handler: (
      Input,
      Config,
      Dependencies
  ) => IO[StateOutput] = (input, _, dependencies) =>
    for {
      logWithExecutionId <- IO(log(Map("executionId" -> input.executionId))(_))
      _ <- logWithExecutionId(s"Getting $monitorCategoryType Monitor for executionId ${input.executionId}")
      monitors <- dependencies.processMonitorClient.getMonitors(
        GetMonitorsRequest(name = Some(s"opex/${input.executionId}"), category = List(monitorCategoryType))
      )
      monitor <- IO.fromOption(monitors.headOption)(new Exception(s"'$monitorCategoryType' Monitor was not found!"))
      _ <- logWithExecutionId(s"Retrieved $monitorCategoryType monitor for ${input.executionId}")
      monitorStatus <- IO.fromOption(mappedStatuses.get(monitor.status))(
        new Exception(s"'${monitor.status}' is an unexpected status!")
      )
      assetsIds <-
        if (inProgressStatuses.contains(monitorStatus)) IO.pure(AssetIds(Nil, Nil, Nil))
        else
          for {
            monitorMessages <- dependencies.processMonitorClient.getMessages(
              GetMessagesRequest(List(monitor.mappedId), List(Info, Warning, Error))
            )

            _ <- logWithExecutionId(s"Retrieved messages for ${monitorCategoryType} monitor ${monitor.mappedId}")

            pathsOfAssetsIngestedWithOpex = monitorMessages.collect {
              case monitorMessage if monitorMessage.message == opexSuccessMessage => monitorMessage.path
            }

            _ <- pathsOfAssetsIngestedWithOpex
              .map(path => IO.raiseWhen(!path.endsWith(".pax"))(new Exception("There is no pax file at the end of this path!")))
              .sequence

            idsOfIngestedAssets = pathsOfAssetsIngestedWithOpex.map { path =>
              val splitPath = path.split("/")
              val paxFileNameForAsset = splitPath.last
              UUID.fromString(paxFileNameForAsset.stripSuffix(".pax"))
            }

            allAssetIds = input.contentAssets.map(UUID.fromString).toList

            (succeededAssetIds, failedAssetIds) = allAssetIds.partition(idsOfIngestedAssets.contains)
            duplicatedAssetIds = Nil // once we know how to find/extract duplicate assets, we'll add the implementation
          } yield AssetIds(succeededAssetIds, failedAssetIds, duplicatedAssetIds)
    } yield StateOutput(
      monitorStatus,
      monitor.mappedId,
      assetsIds.succeededAssets,
      assetsIds.failedAssets,
      assetsIds.duplicatedAssets
    )
  override def dependencies(config: Config): IO[Dependencies] =
    Fs2Client.processMonitorClient(config.apiUrl, config.secretName).map(Dependencies.apply)
}

object Lambda {
  case class Input(executionId: String, contentAssets: Seq[String])
  case class AssetIds(succeededAssets: List[UUID], failedAssets: List[UUID], duplicatedAssets: List[UUID])

  case class StateOutput(
      status: String,
      mappedId: String,
      succeededAssets: List[UUID],
      failedAssets: List[UUID],
      duplicatedAssets: List[UUID]
  )

  case class Config(apiUrl: String, secretName: String) derives ConfigReader

  case class Dependencies(processMonitorClient: ProcessMonitorClient[IO])

}
