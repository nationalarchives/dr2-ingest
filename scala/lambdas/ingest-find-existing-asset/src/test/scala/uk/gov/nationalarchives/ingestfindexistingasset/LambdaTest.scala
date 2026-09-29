package uk.gov.nationalarchives.ingestfindexistingasset

import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.ArchiveFolder
import uk.gov.nationalarchives.ingestfindexistingasset.testUtils.ExternalServicesTestUtils

class LambdaTest extends ExternalServicesTestUtils {

  "handler" should "return an error if the asset is not found in dynamo" in {
    val lambdaTestOutput = runLambda(Nil, Nil)
    lambdaTestOutput.sfnSendFailureOutput.head.error should equal(s"No asset found for $assetId from $batchId")
  }

  "handler" should "return an error if the dynamo entry does not have a type of 'asset'" in {
    val lambdaTestOutput = runLambda(List(generateAsset.copy(`type` = ArchiveFolder)), Nil)
    lambdaTestOutput.sfnSendFailureOutput.head.error should equal(s"Object $assetId is of type ArchiveFolder and not 'Asset'")
  }

  "handler" should "return an error if the entity client returns an error" in {
    val lambdaTestOutput = runLambda(List(generateAsset), Nil, apiError = true)
    lambdaTestOutput.sfnSendFailureOutput.head.error should equal("API has encountered an error")
  }

  "handler" should "return an error if the update call to dynamo db returns an error" in {
    val lambdaTestOutput = runLambda(List(generateAsset), Nil, dynamoError = true)
    lambdaTestOutput.sfnSendFailureOutput.head.error should equal(s"${config.dynamoTableName} not found")
  }

  "handler" should "return an error if sendTaskSuccess returns an error" in {
    val lambdaTestOutput = runLambda(List(generateAsset), Nil, sfnSuccessError = true)
    lambdaTestOutput.sfnSendFailureOutput.head.error should equal("Failure sending task success for task token taskToken")
  }

  "handler" should "raise an error if sendTaskFailure returns an error" in {
    val error = intercept[Exception] {
      runLambda(Nil, Nil, sfnFailureError = true)
    }
    error.getMessage should equal("Failure sending task failure for task token taskToken")
  }

  List(Some(ContentObject), Some(StructuralObject), None).foreach { unexpectedEntityType =>
    "handler" should s"return 'assetExists' value of 'false' if the SourceID lookup returned a non-IO type like $unexpectedEntityType" in {
      val asset = generateAsset
      val entity = generateEntity(asset.id.toString, unexpectedEntityType)
      val lambdaTestOutput = runLambda(List(asset), List(entity))

      lambdaTestOutput.stateOutput.head.items.head.assetExists should equal(false)
      lambdaTestOutput.dynamoItems.head.skipIngest should equal(false)
    }
  }

  "handler" should "not update skipIngest and return an assetExists value of 'false' if the identifier is not found" in {
    val lambdaTestOutput = runLambda(List(generateAsset), Nil)
    lambdaTestOutput.stateOutput.head.items.head.assetExists should equal(false)
    lambdaTestOutput.dynamoItems.head.skipIngest should equal(false)
  }

  "handler" should "update skipIngest and return an assetExists value of 'true' if the identifier is not found" in {
    val asset = generateAsset
    val entity = generateEntity(asset.id.toString)
    val lambdaTestOutput = runLambda(List(asset), List(entity))
    lambdaTestOutput.stateOutput.head.items.head.assetExists should equal(true)
    lambdaTestOutput.dynamoItems.head.skipIngest should equal(true)
  }
}
