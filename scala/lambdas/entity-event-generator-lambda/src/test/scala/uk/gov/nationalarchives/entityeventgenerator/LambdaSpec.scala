package uk.gov.nationalarchives.entityeventgenerator

import cats.syntax.all.*
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dp.client.DataProcessor.EventAction
import uk.gov.nationalarchives.entityeventgenerator.testUtils.ExternalServicesTestUtils.*

import java.time.ZonedDateTime
import java.util.UUID

class LambdaSpec extends AnyFlatSpec with EitherValues {

  "handler" should "update dynamo and send the sns messages" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val entities = List(generateEntity)
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entities, List(generateEventAction(eventActionTime)), dynamoResponse)

    dynamoResult.head should equal("2023-06-05T00:00+01:00", 0)

    snsResult.head.deleted should equal(false)
    snsResult.head.id should equal(s"io:${entities.head.ref}")

    lambdaResult.value should equal(1)
  }

  "handler" should "increment the start number in dynamo if the event action time is the same as the update since time" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val dynamoResponse = List(eventActionTime)
    val entities = List(generateEntity)
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entities, List(generateEventAction(eventActionTime)), dynamoResponse)

    dynamoResult.head should equal("2023-06-05T00:00+01:00", 1000)
  }

  "handler" should "write a start number of 0 if the original start number was 1 and the event action time is after the update since time" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val entities = List(generateEntity)
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entities, List(generateEventAction(eventActionTime)), dynamoResponse, startCount = 1)

    dynamoResult.head should equal("2023-06-05T00:00+01:00", 0)
  }

  "handler" should "filter out ignored event types" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val entities = List(generateEntity)
    val eventActions = List(
      EventAction(UUID.randomUUID, "event", ZonedDateTime.parse(eventActionTime)),
      EventAction(UUID.randomUUID, "Download", ZonedDateTime.parse("2023-07-05T00:00:00.000000+01:00")),
      EventAction(UUID.randomUUID, "Characterise", ZonedDateTime.parse("2023-08-05T00:00:00.000000+01:00")),
      EventAction(UUID.randomUUID, "VirusCheck", ZonedDateTime.parse("2023-09-05T00:00:00.000000+01:00"))
    )

    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entities, eventActions, dynamoResponse)

    dynamoResult.head should equal("2023-06-05T00:00+01:00", 0)

    snsResult.head.deleted should equal(false)
    snsResult.head.id should equal(s"io:${entities.head.ref}")

    lambdaResult.value should equal(1)
  }

  "handler" should "not update the datetime or send a message if there was an error getting the datetime" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, Nil, Nil, Nil, Errors(getItemsError = true).some)

    dynamoResult.size should equal(0)
    snsResult.size should equal(0)
    lambdaResult.left.value.getMessage should equal("Error getting items from Dynamo")
  }

  "handler" should "not update the datetime or send a message if no entities were returned" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, Nil, Nil, dynamoResponse)

    dynamoResult.size should equal(1)
    dynamoResult.head should equal("2023-06-06T20:39:53.377170+01:00", 0)
    snsResult.size should equal(0)
    lambdaResult.value should equal(0)
  }

  "handler" should "not update the datetime or send a message if the date returned is not before the event triggered date" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-07T00:00:00.000000+01:00"
    val entities = List(generateEntity)
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entities, List(generateEventAction(eventActionTime)), dynamoResponse)

    dynamoResult.size should equal(1)
    dynamoResult.head should equal("2023-06-06T20:39:53.377170+01:00", 0)
    snsResult.size should equal(0)
    lambdaResult.value should equal(1)
  }

  "handler" should "not update the datetime or send a message if there is an error getting the event actions" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val entities = List(generateEntity)
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entities, Nil, dynamoResponse, errors = Errors(getEventActionsError = true).some)

    dynamoResult.size should equal(1)
    dynamoResult.head should equal("2023-06-06T20:39:53.377170+01:00", 0)
    snsResult.size should equal(0)
    lambdaResult.left.value.getMessage should equal("Error getting event actions")
  }

  "handler" should "not update the time in DynamoDB if an exception was thrown when publishing to SNS" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val entities = List(generateEntity)
    val (dynamoResult, snsResult, lambdaResult) =
      runLambda(inputEvent, entities, List(generateEventAction(eventActionTime)), dynamoResponse, errors = Errors(publishError = true).some)

    dynamoResult.size should equal(1)
    dynamoResult.head should equal("2023-06-06T20:39:53.377170+01:00", 0)
    snsResult.size should equal(0)
    lambdaResult.left.value.getMessage should equal("Error publishing to SNS")
  }

  "handler" should "return an error if there is an error trying to update the time in Dynamo DB" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val entities = List(generateEntity)
    val (dynamoResult, snsResult, lambdaResult) =
      runLambda(inputEvent, entities, List(generateEventAction(eventActionTime)), dynamoResponse, errors = Errors(updateAttributeValuesError = true).some)

    dynamoResult.size should equal(1)
    dynamoResult.head should equal("2023-06-06T20:39:53.377170+01:00", 0)
    snsResult.size should equal(1)
    snsResult.head.id should equal(s"io:${entities.head.ref}")
    lambdaResult.left.value.getMessage should equal("Error updating Dynamo attribute values")
  }
}
