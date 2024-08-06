package uk.gov.nationalarchives.ingeststartworkflow

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatestplus.mockito.MockitoSugar
import uk.gov.nationalarchives.ingeststartworkflow.Lambda.{Config, Dependencies, Input}
import uk.gov.nationalarchives.dp.client.WorkflowClient
import uk.gov.nationalarchives.dp.client.WorkflowClient.{Parameter, StartWorkflowRequest}

class LambdaTest extends AnyFlatSpec with MockitoSugar {
  val config: Config = Config("", "")
  private def input: Input = Input("testContextName", "TST-1234-345")

  case class ArgumentVerifier(startWorkflowReturnId: IO[Int] = IO.pure(123)) {
    private val mockWorkflowClient: WorkflowClient[IO] = mock[WorkflowClient[IO]]
    when(
      mockWorkflowClient.startWorkflow(any[StartWorkflowRequest])
    ).thenReturn(startWorkflowReturnId)

    val dependencies: Dependencies = Dependencies(mockWorkflowClient)

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

  "handler" should "pass the 'Id', returned from the API, to the OutputStream" in {
    val argumentVerifier = ArgumentVerifier()
    val stateData = new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()

    argumentVerifier.verifyInvocationsAndArgumentsPassed(
      Some("testContextName"),
      List(Parameter("OpexContainerDirectory", s"opex/TST-1234-345"))
    )
    stateData.id should be(123)
  }

  "handler" should "return an exception if the API returns one" in {
    val exception = IO.raiseError(new Exception("API has encountered an issue when calling startWorkflow"))
    val argumentVerifier = ArgumentVerifier(exception)

    val ex = intercept[Exception] {
      new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
    }

    argumentVerifier.verifyInvocationsAndArgumentsPassed(
      Some("testContextName"),
      List(Parameter("OpexContainerDirectory", s"opex/TST-1234-345"))
    )
    ex.getMessage should equal("API has encountered an issue when calling startWorkflow")
  }
}
