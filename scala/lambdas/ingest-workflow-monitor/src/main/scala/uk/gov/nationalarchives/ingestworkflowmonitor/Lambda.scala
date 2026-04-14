package uk.gov.nationalarchives.ingestworkflowmonitor

import cats.effect.*
import pureconfig.ConfigReader
import io.circe.generic.auto.*
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient.*
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient.MonitorCategory.*
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.ingestworkflowmonitor.Lambda.*
import uk.gov.nationalarchives.utils.LambdaRunner

class Lambda extends LambdaRunner[Input, StateOutput, Config, Dependencies]:

  private val mappedStatuses = Map(
    "Running" -> "Running",
    "Pending" -> "Running",
    "Suspended" -> "Succeeded",
    "Recoverable" -> "Succeeded",
    "Succeeded" -> "Succeeded",
    "Failed" -> "Failed"
  )

  override def handler: (
      Input,
      Config,
      Dependencies
  ) => IO[StateOutput] = (input, _, dependencies) =>
    for {
      logWithBatchId <- IO(log(Map("batchId" -> input.batchId))(_))
      _ <- logWithBatchId(s"Getting Ingest Monitor for batchId ${input.batchId}")
      monitors <- dependencies.processMonitorClient.getMonitors(
        GetMonitorsRequest(name = Some(s"opex/${input.batchId}"), category = List(Ingest))
      )
      monitor <- IO.fromOption(monitors.headOption)(new Exception(s"'Ingest' Monitor was not found!"))
      _ <- logWithBatchId(s"Retrieved Ingest monitor for ${input.batchId}")
      monitorStatus <- IO.fromOption(mappedStatuses.get(monitor.status))(
        new Exception(s"'${monitor.status}' is an unexpected status!")
      )
    } yield StateOutput(monitorStatus, monitor.mappedId)

  override def dependencies(config: Config): IO[Dependencies] =
    Fs2Client.processMonitorClient(config.secretName).map(Dependencies.apply)

object Lambda:
  case class Input(batchId: String)

  case class StateOutput(status: String, mappedId: String)

  case class Config(secretName: String) derives ConfigReader

  case class Dependencies(processMonitorClient: ProcessMonitorClient[IO])
