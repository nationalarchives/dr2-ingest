package uk.gov.nationalarchives

import cats.effect.IO
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import uk.gov.nationalarchives.Lambda.StateOutput
import uk.gov.nationalarchives.dp.client.WorkflowClient
import uk.gov.nationalarchives.dp.client.WorkflowClient.{Parameter, StartWorkflowRequest}
import upickle.default
import upickle.default._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class LambdaTest extends AnyFlatSpec with MockitoSugar {
  implicit val stateDataReader: default.Reader[StateOutput] = macroR[StateOutput]

  private def defaultInputStream: ByteArrayInputStream = {
    val inJson =
      s"""{
         |  "workflowContextName": "testContextName",
         |  "executionId": "TST-1234-345"
         |}""".stripMargin
    new ByteArrayInputStream(inJson.getBytes())
  }

  case class StartWorkflowTest(startWorkflowReturnId: IO[Int] = IO(123)) extends Lambda {
    override lazy val workflowClientIO: IO[WorkflowClient[IO]] = {
      when(
        mockWorkflowClient.startWorkflow(any[StartWorkflowRequest])
      ).thenReturn(startWorkflowReturnId)

      IO(mockWorkflowClient)
    }
    private val mockWorkflowClient: WorkflowClient[IO] = mock[WorkflowClient[IO]]

    def verifyInvocationsAndArgumentsPassed(
        expectedWorkflowContextName: Option[String],
        expectedParameters: List[Parameter]
    ): Assertion = {
      val startWorkflowRequestCaptor: ArgumentCaptor[StartWorkflowRequest] =
        ArgumentCaptor.forClass(classOf[StartWorkflowRequest])
      verify(mockWorkflowClient, times(1)).startWorkflow(startWorkflowRequestCaptor.capture())
      startWorkflowRequestCaptor.getValue.workflowContextName should be(expectedWorkflowContextName)
      startWorkflowRequestCaptor.getValue.parameters should be(expectedParameters)
    }
  }

  "handleRequest" should "pass the 'Id', returned from the API, to the OutputStream" in {
    val os = new ByteArrayOutputStream()
    val mockStartWorkflowLambda = StartWorkflowTest()
    mockStartWorkflowLambda.handleRequest(defaultInputStream, os, null)
    val stateData = read[StateOutput](os.toByteArray.map(_.toChar).mkString)

    mockStartWorkflowLambda.verifyInvocationsAndArgumentsPassed(
      Some("testContextName"),
      List(Parameter("OpexContainerDirectory", s"opex/TST-1234-345"))
    )
    stateData.id should be(123)
  }

  "handleRequest" should "return an exception if the API returns one" in {
    val os = new ByteArrayOutputStream()
    val exception = IO.raiseError(new Exception("API has encountered an issue when calling startWorkflow"))
    val mockStartWorkflowLambda = StartWorkflowTest(exception)

    val ex = intercept[Exception] {
      mockStartWorkflowLambda.handleRequest(defaultInputStream, os, null)
    }

    mockStartWorkflowLambda.verifyInvocationsAndArgumentsPassed(
      Some("testContextName"),
      List(Parameter("OpexContainerDirectory", s"opex/TST-1234-345"))
    )
    ex.getMessage should equal("API has encountered an issue when calling startWorkflow")
  }
}
