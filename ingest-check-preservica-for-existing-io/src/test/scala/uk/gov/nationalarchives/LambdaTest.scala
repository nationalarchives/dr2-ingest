package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.matchers.should.Matchers._
import uk.gov.nationalarchives.Lambda.Dependencies
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.testUtils.ExternalServicesTestUtils

class LambdaTest extends ExternalServicesTestUtils {

  "handler" should "return an error if the asset is not found in dynamo" in {
    val verifier = ArgumentVerifier()
    val dependencies: Dependencies = Dependencies(verifier.mockEntityClient, dynamoClient)
    stubBatchGetRequest(emptyDynamoGetResponse)
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"No asset found for $assetId from $batchId")
    verifier.verifyInvocationsAndArgumentsPassed()
  }

  "handler" should "return an error if the dynamo entry does not have a type of 'asset'" in {
    val verifier = ArgumentVerifier()
    val dependencies: Dependencies = Dependencies(verifier.mockEntityClient, dynamoClient)
    stubBatchGetRequest(dynamoGetResponse.replace(""""S": "Asset"""", """"S": "ArchiveFolder""""))
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"Object $assetId is of type ArchiveFolder and not 'Asset'")
    verifier.verifyInvocationsAndArgumentsPassed()
  }

  "handler" should "return an error if the entity client returns an error" in {
    stubBatchGetRequest(dynamoGetResponse)
    val verifier = ArgumentVerifier(IO.raiseError(new Exception("Preservica error")))
    val dependencies: Dependencies = Dependencies(verifier.mockEntityClient, dynamoClient)
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal("Preservica error")
    verifier.verifyInvocationsAndArgumentsPassed()
  }

  "handler" should "return an error if the update call to dynamo db returns an error" in {
    stubBatchGetRequest(dynamoGetResponse)
    stubBatchPostRequest()
    val verifier = ArgumentVerifier(defaultEntityWithSourceIdReturnValue)
    val dependencies: Dependencies = Dependencies(verifier.mockEntityClient, dynamoClient)

    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }

    ex.getMessage should equal("Service returned HTTP status code 500 (Service: DynamoDb, Status Code: 500, Request ID: null)")
    verifier.verifyInvocationsAndArgumentsPassed(1, 9)
  }

  List(Some(ContentObject), Some(StructuralObject), None).foreach { unexpectedEntityType =>
    "handler" should s"return 'assetExists' value of 'false' if the SourceID lookup returned a non-IO type like $unexpectedEntityType" in {
      stubBatchGetRequest(dynamoGetResponse)
      val verifier = ArgumentVerifier(
        defaultEntityWithSourceIdReturnValue.map(entityResponse => entityResponse.map(_.copy(entityType = unexpectedEntityType)))
      )
      val dependencies: Dependencies = Dependencies(verifier.mockEntityClient, dynamoClient)

      val response = new Lambda().handler(input, config, dependencies).unsafeRunSync()

      verifier.verifyInvocationsAndArgumentsPassed(1, 0, expectedAssetExistsResponse = false, Some(response))
    }
  }

  "handler" should "get asset from ddb, lookup SourceID in Preservica, not send a request to add a 'skipIngest' attribute if SourceID lookup " +
    "does not return an Entity and return an assetExists value of 'false'" in {
      stubBatchGetRequest(dynamoGetResponse)
      val verifier = ArgumentVerifier()
      val dependencies: Dependencies = Dependencies(verifier.mockEntityClient, dynamoClient)

      val response = new Lambda().handler(input, config, dependencies).unsafeRunSync()
      verifier.verifyInvocationsAndArgumentsPassed(1, 0, expectedAssetExistsResponse = false, Some(response))
    }

  "handler" should "get asset from ddb, lookup SourceID in Preservica, send a request to add a 'skipIngest' attribute if SourceID lookup " +
    "returns an Entity and return an assetExists value of 'true'" in {
      stubBatchGetRequest(dynamoGetResponse)
      stubBatchPostRequest(Some("1"))
      val verifier = ArgumentVerifier(defaultEntityWithSourceIdReturnValue)
      val dependencies: Dependencies = Dependencies(verifier.mockEntityClient, dynamoClient)

      val response = new Lambda().handler(input, config, dependencies).unsafeRunSync()
      verifier.verifyInvocationsAndArgumentsPassed(1, 1, expectedAssetExistsResponse = true, Some(response))
    }

  "handler" should "return an error if the Dynamo API is unavailable" in {
    val verifier = ArgumentVerifier()
    val dependencies: Dependencies = Dependencies(verifier.mockEntityClient, dynamoClient)
    dynamoServer.stop()
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }

    ex.getMessage should equal("Unable to execute HTTP request: Connection refused: localhost/127.0.0.1:9016")
    verifier.verifyInvocationsAndArgumentsPassed(0)
  }
}
