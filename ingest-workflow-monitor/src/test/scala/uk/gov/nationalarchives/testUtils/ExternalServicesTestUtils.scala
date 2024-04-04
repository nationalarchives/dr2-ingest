package uk.gov.nationalarchives.testUtils

import cats.effect.IO
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import uk.gov.nationalarchives.Lambda.{Dependencies, Input}
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient._

class ExternalServicesTestUtils extends AnyFlatSpec with TableDrivenPropertyChecks {
  private val path =
    "ba662d66-6997-489b-9f65-f6050e1f298d/608f5a47-5283-42da-b7aa-4711ba18b30c/56f99f38-d400-4f77-b047-1efa8577bc84"

  val executionId = "5619e6b0-e959-4e61-9f6e-17170f7c06e2-3a3443ae-92c4-4fc8-9cbd-10c2a58b6045"
  val monitorName = s"opex/$executionId"
  val defaultMonitor: Monitors = Monitors(
    "a69099e5236501684d415d70b9e8ec7d",
    "monitorName",
    "Succeeded",
    "2023-11-07T12:02:04.000Z",
    "2023-11-07T12:02:48.000Z",
    "Ingest",
    "OPEX",
    "",
    "100",
    3,
    6083,
    1,
    0,
    0,
    true
  )
  val defaultMessage: Message = Message(
    1,
    monitorName,
    "ba662d66-6997-489b-9f65-f6050e1f298d/608f5a47-5283-42da-b7aa-4711ba18b30c/56f99f38-d400-4f77-b047-1efa8577bc84/5594c3d7-4b39-408f-a0b0-f79279966205.pax",
    "2023-01-09T11:46:03.000Z",
    "Info",
    "Ingested into asset 4f7754d4-04fc-4f77-ac4b-1279a5de1245 with opex",
    "test workflowName",
    "a69099e5236501684d415d70b9e8ec7d",
    "monitor.info.successful.ingest.with.opex",
    "6edf984e57457dee47d2cbb555a72c9a",
    "open",
    "file.docx",
    "4f7754d4-04fc-4f77-ac4b-1279a5de1245",
    ""
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
  val pathsWithNoPaxFileAtTheEnd: TableFor2[String, String] = Table(
    (s"$path/nonValidName.pax", "Invalid UUID string: nonValidName"),
    (s"$path/5594c3d7-4b39-408f-a0b0-f79279966205.paxo", "There is no pax file at the end of this path!"),
    (s"$path/5594c3d7-4b39-408f-a0b0-f79279966205pax", "There is no pax file at the end of this path!"),
    (s"$path/751b75fc76d5a3090ec9a3f69c51fbe7.pax", "Invalid UUID string: 751b75fc76d5a3090ec9a3f69c51fbe7"),
    (s"$path/", "There is no pax file at the end of this path!"),
    ("", "There is no pax file at the end of this path!")
  )

  val input: Input = Input(executionId, List("5594c3d7-4b39-408f-a0b0-f79279966205", "ae0dac57-d80a-43d1-a436-e912a91eca60", "765ba5b9-6d39-462f-a62a-cec5d4a87043"))

  case class ArgumentVerifier(monitors: IO[Seq[Monitors]], messages: IO[Seq[Message]]) {
    private val mockProcessMonitorClient: ProcessMonitorClient[IO] = mock[ProcessMonitorClient[IO]]
    when(mockProcessMonitorClient.getMonitors(any[GetMonitorsRequest])).thenReturn(monitors)
    when(mockProcessMonitorClient.getMessages(any[GetMessagesRequest], any[Int], any[Int])).thenReturn(messages)

    val dependencies: Dependencies = Dependencies(mockProcessMonitorClient)

    def verifyInvocationsAndArgumentsPassed(
        expectedMonitorStatus: List[MonitorsStatus],
        expectedName: Option[String],
        expectedCategories: List[MonitorCategory],
        expectedMonitor: List[String],
        expectedMessageStatus: List[MessageStatus]
    ): Boolean = {
      val timesMonitorRequestWasSent =
        if (expectedMonitorStatus.nonEmpty || expectedName.nonEmpty || expectedCategories.nonEmpty) 1 else 0
      val timesMsgRequestWasSent = if (expectedMonitor.nonEmpty || expectedMessageStatus.nonEmpty) 1 else 0
      val msgRequestWasSent = timesMsgRequestWasSent > 0
      val getMonitorRequestCaptor: ArgumentCaptor[GetMonitorsRequest] =
        ArgumentCaptor.forClass(classOf[GetMonitorsRequest])
      val getMessagesRequestCaptor: ArgumentCaptor[GetMessagesRequest] =
        ArgumentCaptor.forClass(classOf[GetMessagesRequest])
      verify(mockProcessMonitorClient, times(timesMonitorRequestWasSent)).getMonitors(getMonitorRequestCaptor.capture())
      verify(mockProcessMonitorClient, times(timesMsgRequestWasSent)).getMessages(
        getMessagesRequestCaptor.capture(),
        any[Int],
        any[Int]
      )

      getMonitorRequestCaptor.getValue.status should be(expectedMonitorStatus)
      getMonitorRequestCaptor.getValue.name should be(expectedName)
      getMonitorRequestCaptor.getValue.category should be(expectedCategories)

      if (msgRequestWasSent) {
        getMessagesRequestCaptor.getValue.monitor should be(expectedMonitor)
        getMessagesRequestCaptor.getValue.status should be(expectedMessageStatus)
        msgRequestWasSent
      } else false
    }
  }
}
