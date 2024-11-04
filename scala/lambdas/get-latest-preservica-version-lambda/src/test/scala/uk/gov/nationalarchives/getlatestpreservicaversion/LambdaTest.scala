package uk.gov.nationalarchives.getlatestpreservicaversion

import cats.syntax.all.*
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.getlatestpreservicaversion.Lambda.GetDr2PreservicaVersionResponse
import uk.gov.nationalarchives.getlatestpreservicaversion.testUtils.ExternalServicesTestUtils.*

class LambdaTest extends AnyFlatSpec with EitherValues {

  "handler" should "send an SNS message if there is an update to the version" in {
    val dynamoResponses = List(GetDr2PreservicaVersionResponse(1.0))

    val (_, snsMessages) = runLambda(dynamoResponses, 2.0)

    snsMessages.length should equal(1)
    snsMessages.head.message should equal("Preservica has upgraded to version 2.0; we are using 1.0")
    snsMessages.head.version should equal(2.0)
  }

  "handler" should "return an error if there is an error from Dynamo" in {
    val (result, snsMessages) = runLambda(Nil, 2.0, Errors(dynamoError = true).some)

    result.left.value.getMessage should equal("Error getting version from Dynamo")
    snsMessages.size should equal(0)
  }

  "handler" should "only call 'getItems' but return a runtimeException if the list of items returned was empty" in {
    val (result, snsMessages) = runLambda(Nil, 2.0)

    result.left.value.getMessage should be("The version of Preservica we are using was not found")
  }

  "handler" should "return an error if there is an error getting the Preservica version" in {
    val dynamoResponses = List(GetDr2PreservicaVersionResponse(1.0))
    val (result, snsMessages) = runLambda(dynamoResponses, 2.0, Errors(preservicaError = true).some)

    result.left.value.getMessage should equal("Error getting Preservica version")
    snsMessages.length should equal(0)
  }

  "handler" should "return an error if there is an error publishing to SNS" in {
    val dynamoResponses = List(GetDr2PreservicaVersionResponse(1.0))
    val (result, snsMessages) = runLambda(dynamoResponses, 2.0, Errors(snsError = true).some)

    result.left.value.getMessage should equal("Error sending to SNS")
    snsMessages.length should equal(0)
  }
}
