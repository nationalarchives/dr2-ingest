package uk.gov.nationalarchives.getlatestpreservicaversion

import cats.syntax.all.*
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.getlatestpreservicaversion.Lambda.GetDr2PreservicaVersionResponse
import uk.gov.nationalarchives.getlatestpreservicaversion.testUtils.ExternalServicesTestUtils.*

class LambdaTest extends AnyFlatSpec with EitherValues {

  "handler" should "send an SNS message if there is an update to the version" in {
    val dynamoResponses = List(GetDr2PreservicaVersionResponse(1.0))

    val (_, eventBridgeMessages) = runLambda(2.0)

    eventBridgeMessages.length should equal(1)
    eventBridgeMessages.head should equal("Preservica has upgraded to version 2.0; we are using 7.7 in the EntityClient")
  }

  "handler" should "not send an SNS message if there is not an update to the version" in {
    val (_, eventBridgeMessages) = runLambda(EntityClient.apiVersion)

    eventBridgeMessages.length should equal(0)
  }

  "handler" should "return an error if there is an error getting the Preservica version" in {
    val (result, eventBridgeMessages) = runLambda(2.0, Errors(preservicaError = true).some)

    result.left.value.getMessage should equal("Error getting Preservica version")
    eventBridgeMessages.length should equal(0)
  }

  "handler" should "return an error if there is an error publishing to EventBridge" in {
    val (result, eventBridgeMessages) = runLambda(2.0, Errors(eventBridgeError = true).some)

    result.left.value.getMessage should equal("Error sending message to EventBridge")
    eventBridgeMessages.length should equal(0)
  }
}
