package uk.gov.nationalarchives.ingeststartworkflow

import cats.effect.*
import io.circe.generic.auto.*
import pureconfig.ConfigReader
import uk.gov.nationalarchives.ingeststartworkflow.Lambda.*
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.dp.client.WorkflowClient
import uk.gov.nationalarchives.dp.client.WorkflowClient.{Parameter, StartWorkflowRequest}
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client

class Lambda extends LambdaRunner[Input, StateOutput, Config, Dependencies] {

  override def handler: (
      Input,
      Config,
      Dependencies
  ) => IO[StateOutput] = (input, _, dependencies) => {
    val batchRef = input.executionId.split('-').take(3).mkString("-")
    val logWithBatch = log(Map("batchRef" -> batchRef))(_)
    for {
      _ <- logWithBatch(s"Starting ingest workflow ${input.workflowContextName} for $batchRef")
      id <- dependencies.workflowClient.startWorkflow(
        StartWorkflowRequest(
          Some(input.workflowContextName),
          parameters = List(Parameter("OpexContainerDirectory", s"opex/${input.executionId}"))
        )
      )
      _ <- logWithBatch(s"Workflow ${input.workflowContextName} for $batchRef started")
    } yield StateOutput(id)
  }

  override def dependencies(config: Config): IO[Dependencies] = Fs2Client.workflowClient(config.secretName).map(Dependencies.apply)
}

object Lambda {
  case class Input(workflowContextName: String, executionId: String)
  case class Config(secretName: String) derives ConfigReader
  case class StateOutput(id: Int)

  case class Dependencies(workflowClient: WorkflowClient[IO])
}
