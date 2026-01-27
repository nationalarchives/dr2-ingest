package uk.gov.nationalarchives.ingeststartworkflow

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dp.client.WorkflowClient
import uk.gov.nationalarchives.dp.client.WorkflowClient.StartWorkflowRequest
import uk.gov.nationalarchives.ingeststartworkflow.Lambda.{Config, Dependencies, Input}

import scala.util.hashing.MurmurHash3

class LambdaTest extends AnyFlatSpec {
  private val config: Config = Config("")
  private val input: Input = Input("testContextName", "TST-1234-345")

  private val workflowClient: WorkflowClient[IO] = (startWorkflowRequest: StartWorkflowRequest) => IO.pure(MurmurHash3.caseClassHash(startWorkflowRequest))

  private val errorWorkflowClient: WorkflowClient[IO] = (startWorkflowRequest: StartWorkflowRequest) =>
    IO.raiseError(new Exception("API has encountered an issue when calling startWorkflow"))

  "handler" should "pass the 'Id', returned from the API, to the OutputStream" in {
    val stateData = new Lambda().handler(input, config, Dependencies(workflowClient)).unsafeRunSync()
    stateData.id should be(-1365245746)
  }

  "handler" should "return an exception if the API returns one" in {
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, Dependencies(errorWorkflowClient)).unsafeRunSync()
    }
    ex.getMessage should equal("API has encountered an issue when calling startWorkflow")
  }
}
