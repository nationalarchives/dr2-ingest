package uk.gov.nationalarchives.ingestfindexistingasset

import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.ArchiveFolder
import uk.gov.nationalarchives.ingestfindexistingasset.testUtils.ExternalServicesTestUtils

class LambdaTest extends ExternalServicesTestUtils {

  "handler" should "return an error if the asset is not found in dynamo" in {
    val (_, _, results) = runLambda(Nil, Nil)
    results.left.value.getMessage should equal(s"No asset found for $assetId from $batchId")
  }

  "handler" should "return an error if the dynamo entry does not have a type of 'asset'" in {
    val (_, _, results) = runLambda(List(generateAsset.copy(`type` = ArchiveFolder)), Nil)
    results.left.value.getMessage should equal(s"Object $assetId is of type ArchiveFolder and not 'Asset'")
  }

  "handler" should "return an error if the entity client returns an error" in {
    val (_, _, results) = runLambda(List(generateAsset), Nil, apiError = true)
    results.left.value.getMessage should equal("API has encountered an error")
  }

  "handler" should "return an error if the update call to dynamo db returns an error" in {
    val (_, _, results) = runLambda(List(generateAsset), Nil, dynamoError = true)
    results.left.value.getMessage should equal(s"${config.dynamoTableName} not found")
  }

  List(Some(ContentObject), Some(StructuralObject), None).foreach { unexpectedEntityType =>
    "handler" should s"return 'assetExists' value of 'false' if the SourceID lookup returned a non-IO type like $unexpectedEntityType" in {
      val asset = generateAsset
      val entity = generateEntity(asset.id.toString, unexpectedEntityType)
      val (_, items, res) = runLambda(List(asset), List(entity))
      res.value.items.head.assetExists should equal(false)
      items.head.skipIngest should equal(false)
    }
  }

  "handler" should "not update skipIngest and return an assetExists value of 'false' if the identifier is not found" in {
    val (_, items, res) = runLambda(List(generateAsset), Nil)
    res.value.items.head.assetExists should equal(false)
    items.head.skipIngest should equal(false)
  }

  "handler" should "update skipIngest and return an assetExists value of 'true' if the identifier is not found" in {
    val asset = generateAsset
    val entity = generateEntity(asset.id.toString)
    val (_, items, res) = runLambda(List(asset), List(entity))
    res.value.items.head.assetExists should equal(true)
    items.head.skipIngest should equal(true)
  }
}
