package uk.gov.nationalarchives.postprocesscleanup

import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage

object Helper {
  def runLambda(
                 sqsMessage: SQSMessage,
                 initialItemsInTable: List[IngestFilesTableItem],
               ): LambdaRunResults = {
    for {
      dynamoRef <- Ref.of[IO, List[IngestQueueTableItem]](initialItemsInTable)
      ssmRef <- Ref.of[IO, FlowControlConfig](flowControlConfig)
      sfnRef <- Ref.of[IO, List[StepFunctionExecution]](initialExecutions)
      dependencies = Dependencies(dynamoClient(dynamoRef, error), sfnClient(sfnRef, error), ssmClient(ssmRef, error), randomSelection)
      result <- new Lambda().handler(input, config, dependencies).attempt
      dynamoResult <- dynamoRef.get
      ssmResult <- ssmRef.get
      sfnResult <- sfnRef.get
    } yield LambdaRunResults(result, dynamoResult, ssmResult, sfnResult)
  }.unsafeRunSync()

  case class IngestFilesTableItem(id: String, potentialParentPath: Option[String], location: String, ttl: String)
  case class LambdaRunResults(finalItemsInTable: List[IngestFilesTableItem])

}
