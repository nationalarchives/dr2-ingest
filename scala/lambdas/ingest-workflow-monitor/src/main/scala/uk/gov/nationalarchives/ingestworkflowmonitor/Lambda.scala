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
import scala.annotation.static

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
      logWithExecutionId <- IO(log(Map("executionId" -> input.executionId))(_))
      _ <- logWithExecutionId(s"Getting Ingest Monitor for executionId ${input.executionId}")
      monitors <- dependencies.processMonitorClient.getMonitors(
        GetMonitorsRequest(name = Some(s"opex/${input.executionId}"), category = List(Ingest))
      )
      monitor <- IO.fromOption(monitors.headOption)(new Exception(s"'Ingest' Monitor was not found!"))
      _ <- logWithExecutionId(s"Retrieved Ingest monitor for ${input.executionId}")
      monitorStatus <- IO.fromOption(mappedStatuses.get(monitor.status))(
        new Exception(s"'${monitor.status}' is an unexpected status!")
      )
    } yield StateOutput(monitorStatus, monitor.mappedId)

  override def dependencies(config: Config): IO[Dependencies] =
    Fs2Client.processMonitorClient(config.apiUrl, config.secretName).map(Dependencies.apply)

object Lambda:
  @static def main(args: Array[String]): Unit = new Lambda().run()

  case class Input(executionId: String)

  case class StateOutput(status: String, mappedId: String)

  case class Config(apiUrl: String, secretName: String) derives ConfigReader

  case class Dependencies(processMonitorClient: ProcessMonitorClient[IO])
