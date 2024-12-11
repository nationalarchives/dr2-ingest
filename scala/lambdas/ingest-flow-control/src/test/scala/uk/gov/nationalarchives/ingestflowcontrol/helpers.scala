package uk.gov.nationalarchives.ingestflowcontrol

object helpers {
  case class StepFunctionExecution(name: String, taskToken: String, taskTokenSuccess: Boolean = false)

}
