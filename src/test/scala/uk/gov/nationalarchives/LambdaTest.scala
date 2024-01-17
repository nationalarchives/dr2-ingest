package uk.gov.nationalarchives

import cats.effect.IO
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import uk.gov.nationalarchives.Lambda.StateOutput
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient.{
  GetMonitorsRequest,
  Ingest,
  MonitorCategory,
  Monitors,
  MonitorsStatus
}
import upickle.default._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class LambdaTest extends AnyFlatSpec with MockitoSugar {
  implicit val stateDataReader: Reader[StateOutput] = macroR[StateOutput]
  private val executionId = "5619e6b0-e959-4e61-9f6e-17170f7c06e2-3a3443ae-92c4-4fc8-9cbd-10c2a58b6045"
  private val monitorName = s"opex/$executionId"

  private def defaultInputStream: ByteArrayInputStream = {
    val inJson = s"""{"executionId": "$executionId"}""".stripMargin
    new ByteArrayInputStream(inJson.getBytes())
  }

  case class ProcessMonitorTest(monitors: IO[Seq[Monitors]]) extends Lambda {
    override lazy val processMonitorClientIO: IO[ProcessMonitorClient[IO]] = {
      when(mockProcessMonitorClient.getMonitors(any[GetMonitorsRequest])).thenReturn(monitors)
      IO(mockProcessMonitorClient)
    }
    private val mockProcessMonitorClient: ProcessMonitorClient[IO] = mock[ProcessMonitorClient[IO]]

    def verifyInvocationsAndArgumentsPassed(
        expectedStatus: List[MonitorsStatus],
        expectedName: Option[String],
        expectedCategories: List[MonitorCategory]
    ): Assertion = {
      val processMonitorRequestCaptor: ArgumentCaptor[GetMonitorsRequest] =
        ArgumentCaptor.forClass(classOf[GetMonitorsRequest])
      verify(mockProcessMonitorClient, times(1)).getMonitors(processMonitorRequestCaptor.capture())

      println(processMonitorRequestCaptor.getValue)

      processMonitorRequestCaptor.getValue.status should be(expectedStatus)
      processMonitorRequestCaptor.getValue.name should be(expectedName)
      processMonitorRequestCaptor.getValue.category should be(expectedCategories)
    }
  }

  "handleRequest" should "pass the 'state' and 'mappedId', returned from the API, to the OutputStream" in {
    val os = new ByteArrayOutputStream()
    val mockProcessMonitorLambda = ProcessMonitorTest(
      IO(
        Seq(
          Monitors(
            "a69099e5236501684d415d70b9e8ec7d",
            monitorName,
            "Succeeded",
            "2023-11-07T12:02:04.000Z",
            "2023-11-07T12:02:48.000Z",
            "Ingest",
            "OPEX",
            3,
            6083,
            1,
            0,
            0,
            true
          )
        )
      )
    )
    mockProcessMonitorLambda.handleRequest(defaultInputStream, os, null)
    val stateData = read[StateOutput](os.toByteArray.map(_.toChar).mkString)

    mockProcessMonitorLambda.verifyInvocationsAndArgumentsPassed(
      Nil,
      Some(monitorName),
      List(Ingest)
    )
    stateData.status should be("Succeeded")
    stateData.mappedId should be("a69099e5236501684d415d70b9e8ec7d")
  }

  "handleRequest" should "return an exception if the API returns one" in {
    val os = new ByteArrayOutputStream()
    val exception = IO.raiseError(new Exception("API has encountered an issue when calling 'getMonitors'"))
    val mockProcessMonitorLambda = ProcessMonitorTest(exception)

    val ex = intercept[Exception] {
      mockProcessMonitorLambda.handleRequest(defaultInputStream, os, null)
    }

    mockProcessMonitorLambda.verifyInvocationsAndArgumentsPassed(
      Nil,
      Some(monitorName),
      List(Ingest)
    )
    ex.getMessage should equal("API has encountered an issue when calling 'getMonitors'")
  }
}
