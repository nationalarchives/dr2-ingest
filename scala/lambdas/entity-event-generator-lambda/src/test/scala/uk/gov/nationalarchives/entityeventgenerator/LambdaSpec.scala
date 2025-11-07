package uk.gov.nationalarchives.entityeventgenerator

import cats.syntax.all.*
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dp.client.DataProcessor.EventAction
import uk.gov.nationalarchives.dp.client.EntityClient.EntitiesUpdated
import uk.gov.nationalarchives.entityeventgenerator.testUtils.ExternalServicesTestUtils.*

import java.time.ZonedDateTime
import java.util.UUID

class LambdaSpec extends AnyFlatSpec with EitherValues {

  "handler" should "update dynamo and send the sns messages" in {
    val inputEvent = event("2023-06-08T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-07T00:00:00.000000+01:00"
    val entitiesUpdated = EntitiesUpdated(false, List(generateEntity))
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entitiesUpdated, List(generateEventAction(eventActionTime)), dynamoResponse)

    dynamoResult.head should equal("2023-06-07T00:00+01:00", 0)

    snsResult.head.deleted should equal(false)
    snsResult.head.id should equal(s"io:${entitiesUpdated.entities.head.ref}")

    lambdaResult.value should equal(1)
  }

  "handler" should "increment the startAt argument, not update the event datetime and send messages if all entities are deleted and there is no next page" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val entitiesUpdated = EntitiesUpdated(false, List(generateEntity, generateEntity).map(_.copy(deleted = true)))
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entitiesUpdated, Nil, dynamoResponse)

    dynamoResult.head should equal("2023-06-06T20:39:53.377170+01:00", 2)

    snsResult.length should equal(2)

    lambdaResult.value should equal(2)
  }

  "handler" should "increment the startAt argument, not update the event datetime and send messages if all entities are deleted and there is a next page" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val entitiesUpdated = EntitiesUpdated(true, List(generateEntity, generateEntity).map(_.copy(deleted = true)))
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entitiesUpdated, Nil, dynamoResponse)

    dynamoResult.head should equal("2023-06-06T20:39:53.377170+01:00", 2)

    snsResult.length should equal(2)

    lambdaResult.value should equal(2)
  }

  "handler" should "set the startAt argument to zero, update the event datetime and send all messages if some entities are deleted and there is no next page" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val entitiesUpdated = EntitiesUpdated(false, generateEntity :: List(generateEntity, generateEntity).map(_.copy(deleted = true)))
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entitiesUpdated, List(generateEventAction(eventActionTime)), dynamoResponse)

    dynamoResult.head should equal("2023-06-05T00:00+01:00", 0)

    snsResult.length should equal(3)

    lambdaResult.value should equal(3)
  }

  "handler" should "increment the startAt argument in dynamo if there is another page" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val dynamoResponse = List(eventActionTime)
    val entities = EntitiesUpdated(true, List(generateEntity))
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entities, List(generateEventAction(eventActionTime)), dynamoResponse)

    dynamoResult.head should equal("2023-06-05T00:00+01:00", 1)
    snsResult.length should equal(1)
    lambdaResult.value should equal(1)
  }

  "handler" should "write a startAt argument of 0 if the original startAt argument was 1 and the event action time is after the event datetime" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val entitiesUpdated = EntitiesUpdated(false, List(generateEntity))
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entitiesUpdated, List(generateEventAction(eventActionTime)), dynamoResponse, startCount = 1)

    dynamoResult.head should equal("2023-06-05T00:00+01:00", 0)
  }

  "handler" should "write a startAt argument of original startAt plus the length of updated entities if there is a next page" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val entitiesUpdated = EntitiesUpdated(true, (1 to 15).map(_ => generateEntity).toList)
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entitiesUpdated, List(generateEventAction(eventActionTime)), dynamoResponse, startCount = 56)

    dynamoResult.head should equal("2023-06-06T20:39:53.377170+01:00", 71)
  }

  "handler" should "write a startAt argument of zero if there is no next page and not all entities are deleted" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val entitiesUpdated = EntitiesUpdated(false, (1 to 15).map(_ => generateEntity).toList)
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entitiesUpdated, List(generateEventAction(eventActionTime)), dynamoResponse, startCount = 56)

    dynamoResult.head should equal("2023-06-05T00:00+01:00", 0)
  }

  "handler" should "filter out ignored event types" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val entitiesUpdated = EntitiesUpdated(false, List(generateEntity))
    val eventActions = List(
      EventAction(UUID.randomUUID, "event", ZonedDateTime.parse(eventActionTime)),
      EventAction(UUID.randomUUID, "Download", ZonedDateTime.parse("2023-07-05T00:00:00.000000+01:00")),
      EventAction(UUID.randomUUID, "Characterise", ZonedDateTime.parse("2023-08-05T00:00:00.000000+01:00")),
      EventAction(UUID.randomUUID, "VirusCheck", ZonedDateTime.parse("2023-09-05T00:00:00.000000+01:00"))
    )

    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entitiesUpdated, eventActions, dynamoResponse)

    dynamoResult.head should equal("2023-06-05T00:00+01:00", 0)

    snsResult.head.deleted should equal(false)
    snsResult.head.id should equal(s"io:${entitiesUpdated.entities.head.ref}")

    lambdaResult.value should equal(1)
  }

  "handler" should "do nothing if all of the event actions have an ignored type" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val entitiesUpdated = EntitiesUpdated(false, List(generateEntity))
    val eventActions = List(
      EventAction(UUID.randomUUID, "Download", ZonedDateTime.parse("2023-07-05T00:00:00.000000+01:00")),
      EventAction(UUID.randomUUID, "Characterise", ZonedDateTime.parse("2023-08-05T00:00:00.000000+01:00")),
      EventAction(UUID.randomUUID, "VirusCheck", ZonedDateTime.parse("2023-09-05T00:00:00.000000+01:00"))
    )

    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entitiesUpdated, eventActions, dynamoResponse)

    dynamoResult.head should equal("2023-06-06T20:39:53.377170+01:00", 0)

    snsResult.isEmpty should equal(true)

    lambdaResult.value should equal(0)
  }

  "handler" should "not update the datetime or send a message if there was an error getting the datetime" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, EntitiesUpdated(false, Nil), Nil, Nil, Errors(getItemsError = true).some)

    dynamoResult.size should equal(0)
    snsResult.size should equal(0)
    lambdaResult.left.value.getMessage should equal("Error getting items from Dynamo")
  }

  "handler" should "not update the datetime or send a message if no entities were returned" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, EntitiesUpdated(false, Nil), Nil, dynamoResponse)

    dynamoResult.size should equal(1)
    dynamoResult.head should equal("2023-06-06T20:39:53.377170+01:00", 0)
    snsResult.size should equal(0)
    lambdaResult.value should equal(0)
  }

  "handler" should "not update the datetime or send a message if the date returned is not before the event triggered date" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-07T00:00:00.000000+01:00"
    val entitiesUpdated = EntitiesUpdated(false, List(generateEntity))
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entitiesUpdated, List(generateEventAction(eventActionTime)), dynamoResponse)

    dynamoResult.size should equal(1)
    dynamoResult.head should equal("2023-06-06T20:39:53.377170+01:00", 0)
    snsResult.size should equal(0)
    lambdaResult.value should equal(0)
  }

  "handler" should "not update the datetime or send a message if there is an error getting the event actions" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val entitiesUpdated = EntitiesUpdated(false, List(generateEntity))
    val (dynamoResult, snsResult, lambdaResult) = runLambda(inputEvent, entitiesUpdated, Nil, dynamoResponse, errors = Errors(getEventActionsError = true).some)

    dynamoResult.size should equal(1)
    dynamoResult.head should equal("2023-06-06T20:39:53.377170+01:00", 0)
    snsResult.size should equal(0)
    lambdaResult.left.value.getMessage should equal("Error getting event actions")
  }

  "handler" should "not update the time in DynamoDB if an exception was thrown when publishing to SNS" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val entitiesUpdated = EntitiesUpdated(false, List(generateEntity))
    val (dynamoResult, snsResult, lambdaResult) =
      runLambda(inputEvent, entitiesUpdated, List(generateEventAction(eventActionTime)), dynamoResponse, errors = Errors(publishError = true).some)

    dynamoResult.size should equal(1)
    dynamoResult.head should equal("2023-06-06T20:39:53.377170+01:00", 0)
    snsResult.size should equal(0)
    lambdaResult.left.value.getMessage should equal("Error publishing to SNS")
  }

  "handler" should "return an error if there is an error trying to update the time in Dynamo DB" in {
    val inputEvent = event("2023-06-07T00:00:00.000000+01:00")
    val dynamoResponse = List("2023-06-06T20:39:53.377170+01:00")
    val eventActionTime = "2023-06-05T00:00:00.000000+01:00"
    val entitiesUpdated = EntitiesUpdated(false, List(generateEntity))
    val (dynamoResult, snsResult, lambdaResult) =
      runLambda(inputEvent, entitiesUpdated, List(generateEventAction(eventActionTime)), dynamoResponse, errors = Errors(updateAttributeValuesError = true).some)

    dynamoResult.size should equal(1)
    dynamoResult.head should equal("2023-06-06T20:39:53.377170+01:00", 0)
    snsResult.size should equal(1)
    snsResult.head.id should equal(s"io:${entitiesUpdated.entities.head.ref}")
    lambdaResult.left.value.getMessage should equal("Error updating Dynamo attribute values")
  }
}
