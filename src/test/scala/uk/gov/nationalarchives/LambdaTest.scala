package uk.gov.nationalarchives

import cats.effect.IO
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.TableDrivenPropertyChecks
import uk.gov.nationalarchives.Lambda.StateOutput
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient._
import uk.gov.nationalarchives.testUtils.ExternalServicesTestUtils
import upickle.default._

import java.io.ByteArrayOutputStream
import java.util.UUID

class LambdaTest extends ExternalServicesTestUtils with MockitoSugar with TableDrivenPropertyChecks {
  implicit val stateDataReader: Reader[StateOutput] = macroR[StateOutput]

  forAll(runningStatuses) { (apiStatus, normalisedStatus) =>
    "handleRequest" should s"pass a '$normalisedStatus' 'state', 'mappedId', 0 succeededAssetId, 0 failedAssetIds, 0 duplicatedAssetIds" +
      s"to the OutputStream if the status returned from the API is $apiStatus" in {
        val os = new ByteArrayOutputStream()
        val mockProcessMonitorLambda = ProcessMonitorTest(
          IO(Seq(defaultMonitor.copy(status = apiStatus))),
          IO.pure(Nil)
        )
        mockProcessMonitorLambda.handleRequest(defaultInputStream, os, null)
        val stateData = read[StateOutput](os.toByteArray.map(_.toChar).mkString)

        mockProcessMonitorLambda.verifyInvocationsAndArgumentsPassed(
          Nil,
          Some(monitorName),
          List(Ingest),
          Nil,
          Nil
        )
        stateData.status should be(normalisedStatus)
        stateData.mappedId should be("a69099e5236501684d415d70b9e8ec7d")
        stateData.succeededAssets should be(Nil)
        stateData.duplicatedAssets should be(Nil)
        stateData.failedAssets should be(Nil)
      }
  }

  forAll(succeededStatuses) { (apiStatus, normalisedStatus) =>
    "handleRequest" should s"pass a '$normalisedStatus' 'state', 'mappedId', 1 succeededAssetId, 2 failedAssetIds, 0 duplicatedAssetIds" +
      s"to the OutputStream if the status returned from the API is $apiStatus" in {
        val os = new ByteArrayOutputStream()
        val mockProcessMonitorLambda = ProcessMonitorTest(
          IO(Seq(defaultMonitor.copy(status = apiStatus))),
          IO(Seq(defaultMessage))
        )
        mockProcessMonitorLambda.handleRequest(defaultInputStream, os, null)
        val stateData = read[StateOutput](os.toByteArray.map(_.toChar).mkString)

        mockProcessMonitorLambda.verifyInvocationsAndArgumentsPassed(
          Nil,
          Some(monitorName),
          List(Ingest),
          List("a69099e5236501684d415d70b9e8ec7d"),
          List(Info, Warning, Error)
        )
        stateData.status should be(normalisedStatus)
        stateData.mappedId should be("a69099e5236501684d415d70b9e8ec7d")
        stateData.succeededAssets should be(List(UUID.fromString("5594c3d7-4b39-408f-a0b0-f79279966205")))
        stateData.duplicatedAssets should be(Nil)
        stateData.failedAssets should be(
          List(
            UUID.fromString("ae0dac57-d80a-43d1-a436-e912a91eca60"),
            UUID.fromString("765ba5b9-6d39-462f-a62a-cec5d4a87043")
          )
        )
      }
  }

  forAll(failedStatuses) { (apiStatus, normalisedStatus) =>
    "handleRequest" should s"pass a '$normalisedStatus' 'state', 'mappedId', 0 succeededAssetId, 3 failedAssetIds, 0 duplicatedAssetIds" +
      s"to the OutputStream if the status returned from the API is $apiStatus" in {
        val os = new ByteArrayOutputStream()
        val mockProcessMonitorLambda = ProcessMonitorTest(
          IO(Seq(defaultMonitor.copy(status = apiStatus))),
          IO(Seq(defaultMessage.copy(message = "monitor.error.folder.not.ingested")))
        )
        mockProcessMonitorLambda.handleRequest(defaultInputStream, os, null)
        val stateData = read[StateOutput](os.toByteArray.map(_.toChar).mkString)

        mockProcessMonitorLambda.verifyInvocationsAndArgumentsPassed(
          Nil,
          Some(monitorName),
          List(Ingest),
          List("a69099e5236501684d415d70b9e8ec7d"),
          List(Info, Warning, Error)
        )
        stateData.status should be(normalisedStatus)
        stateData.mappedId should be("a69099e5236501684d415d70b9e8ec7d")
        stateData.succeededAssets should be(Nil)
        stateData.duplicatedAssets should be(Nil)
        stateData.failedAssets should be(
          List(
            UUID.fromString("5594c3d7-4b39-408f-a0b0-f79279966205"),
            UUID.fromString("ae0dac57-d80a-43d1-a436-e912a91eca60"),
            UUID.fromString("765ba5b9-6d39-462f-a62a-cec5d4a87043")
          )
        )
      }
  }

  forAll(pathsWithNoPaxFileAtTheEnd) { (pathWithNoPaxFileAtTheEnd, exceptionMessage) =>
    "handleRequest" should s"return an exception if an UUID could not be parsed from the end of path '$pathWithNoPaxFileAtTheEnd'" in {
      val os = new ByteArrayOutputStream()
      val mockProcessMonitorLambda = ProcessMonitorTest(
        IO(Seq(defaultMonitor)),
        IO(Seq(defaultMessage.copy(path = pathWithNoPaxFileAtTheEnd)))
      )

      val ex = intercept[Exception] {
        mockProcessMonitorLambda.handleRequest(defaultInputStream, os, null)
      }

      mockProcessMonitorLambda.verifyInvocationsAndArgumentsPassed(
        Nil,
        Some(monitorName),
        List(Ingest),
        List("a69099e5236501684d415d70b9e8ec7d"),
        List(Info, Warning, Error)
      )
      ex.getMessage should equal(exceptionMessage)
    }
  }

  "handleRequest" should "return an exception if the API returns one" in {
    val os = new ByteArrayOutputStream()
    val exception = IO.raiseError(new Exception("API has encountered an issue when calling 'getMonitors'"))
    val mockProcessMonitorLambda = ProcessMonitorTest(exception, IO.pure(Nil))

    val ex = intercept[Exception] {
      mockProcessMonitorLambda.handleRequest(defaultInputStream, os, null)
    }

    mockProcessMonitorLambda.verifyInvocationsAndArgumentsPassed(
      Nil,
      Some(monitorName),
      List(Ingest),
      Nil,
      Nil
    )
    ex.getMessage should equal("API has encountered an issue when calling 'getMonitors'")
  }

  "handleRequest" should s"return an exception if the API returns a status that's unexpected" in {
    val os = new ByteArrayOutputStream()
    val mockProcessMonitorLambda = ProcessMonitorTest(
      IO(Seq(defaultMonitor.copy(status = "InvalidStatus"))),
      IO.pure(Nil)
    )

    val ex = intercept[Exception] {
      mockProcessMonitorLambda.handleRequest(defaultInputStream, os, null)
    }

    mockProcessMonitorLambda.verifyInvocationsAndArgumentsPassed(
      Nil,
      Some(monitorName),
      List(Ingest),
      Nil,
      Nil
    )

    ex.getMessage should equal("'InvalidStatus' is an unexpected status!")
  }
}
