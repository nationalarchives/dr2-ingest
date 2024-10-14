package uk.gov.nationalarchives.ingestworkflowmonitor.testUtils

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient.*
import uk.gov.nationalarchives.ingestworkflowmonitor.Lambda.{Dependencies, Input}

class ExternalServicesTestUtils extends AnyFlatSpec with TableDrivenPropertyChecks:
  val executionId = "5619e6b0-e959-4e61-9f6e-17170f7c06e2-3a3443ae-92c4-4fc8-9cbd-10c2a58b6045"
  val monitorName = s"opex/$executionId"
  val defaultMonitor: Monitors = Monitors(
    "a69099e5236501684d415d70b9e8ec7d",
    monitorName,
    "Succeeded",
    Option("2023-11-07T12:02:04.000Z"),
    Option("2023-11-07T12:02:48.000Z"),
    "Ingest",
    "OPEX",
    None,
    Option("100"),
    3,
    6083,
    1,
    0,
    0,
    true
  )

  val runningStatuses: TableFor2[String, String] = Table(
    ("API status", "Normalised status"),
    ("Running", "Running"),
    ("Pending", "Running")
  )
  val failedStatuses: TableFor2[String, String] = Table(
    ("API status", "Normalised status"),
    ("Failed", "Failed")
  )
  val succeededStatuses: TableFor2[String, String] = Table(
    ("API status", "Normalised status"),
    ("Suspended", "Succeeded"),
    ("Recoverable", "Succeeded"),
    ("Succeeded", "Succeeded")
  )

  val input: Input = Input(executionId)

  def createDependencies(monitors: IO[Seq[Monitors]]): Dependencies = {
    val processMonitorClient: ProcessMonitorClient[IO] = new ProcessMonitorClient[IO]:
      override def getMonitors(getMonitorsRequest: GetMonitorsRequest): IO[Seq[Monitors]] = monitors.map(_.filter { monitor =>
        getMonitorsRequest.name.contains(monitor.name) && getMonitorsRequest.category.map(_.toString).contains(monitor.category)
      })

      override def getMessages(getMessagesRequest: GetMessagesRequest, start: Int, max: Int): IO[Seq[Message]] = IO.pure(Nil)
    Dependencies(processMonitorClient)
  }
