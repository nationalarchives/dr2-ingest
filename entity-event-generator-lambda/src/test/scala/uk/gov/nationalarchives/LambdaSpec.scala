package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import org.joda.time.DateTime
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers._
import software.amazon.awssdk.services.dynamodb.model._
import uk.gov.nationalarchives.Lambda.Config
import uk.gov.nationalarchives.dp.client.DataProcessor.EventAction
import uk.gov.nationalarchives.testUtils.ExternalServicesTestUtils

import java.time.ZonedDateTime
import java.util.UUID

class LambdaSpec extends ExternalServicesTestUtils with MockitoSugar {
  private val mockScheduleEvent = mock[ScheduledEvent]
  val config: Config = Config("", "", "arn:aws:sns:eu-west-2:123456789012:MockResourceId", "table-name")
  when(mockScheduleEvent.getTime).thenReturn(DateTime.parse("2023-06-07T00:00:00.000000+01:00"))

  "handler" should "make 3 calls to getAttributeValues, 3 calls to entitiesUpdatedSince and 2 to the others if everything goes correctly" in {
    val argumentVerifier = ArgumentVerifier()

    new Lambda().handler(mockScheduleEvent, config, argumentVerifier.dependencies).unsafeRunSync()
    argumentVerifier.verifyInvocationsAndArgumentsPassed(3, 3, 2, 2, 2)
  }

  "handler" should "call getAttributeValues method once but not make the call to get entities, actions, publish to SNS nor update the time" +
    "in DynamoDB if an exception was thrown when it tried to get the datetime" in {
      val argumentVerifier = ArgumentVerifier(
        getAttributeValuesReturnValue = IO.raiseError(
          ResourceNotFoundException
            .builder()
            .message("Table name not found")
            .build()
        )
      )

      val thrownException = intercept[ResourceNotFoundException] {
        new Lambda().handler(mockScheduleEvent, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be("Table name not found")

      argumentVerifier.verifyInvocationsAndArgumentsPassed(1, 0, 0, 0, 0)
    }

  "handler" should "call getAttributeValues and entitiesUpdatedSince methods once but not make the call to publish to SNS nor update the time in DynamoDB if no entities were returned" in {
    val argumentVerifier = ArgumentVerifier(Nil)
    new Lambda().handler(mockScheduleEvent, config, argumentVerifier.dependencies).unsafeRunSync()

    argumentVerifier.verifyInvocationsAndArgumentsPassed(1, 1, 0, 0, 0)
  }

  "handler" should "call getAttributeValues and entitiesUpdatedSince 3 times and entityEventActions twice, but not " +
    "make the call to publish to SNS nor update the time in DynamoDB if the date returned is not before the event triggered date" in {
      val argumentVerifier = ArgumentVerifier(entityEventActionsReturnValue =
        IO(
          Seq(
            EventAction(
              UUID.fromString("f24313ce-dd5d-4b28-9ebc-b47893f55a8e"),
              "Ingest",
              ZonedDateTime.parse("2023-06-07T20:39:53.377170+01:00")
            )
          )
        )
      )

      new Lambda().handler(mockScheduleEvent, config, argumentVerifier.dependencies).unsafeRunSync()

      argumentVerifier.verifyInvocationsAndArgumentsPassed(3, 3, 2, 0, 0)
    }

  "handler" should "call getAttributeValues, entitiesUpdatedSince and entityEventActions once but not " +
    "make the call to publish to SNS nor update the time in DynamoDB if something went wrong getting Event Actions" in {
      val argumentVerifier = ArgumentVerifier(
        entityEventActionsReturnValue = IO.raiseError(
          new Exception("Preservica error")
        )
      )

      val thrownException = intercept[Exception] {
        new Lambda().handler(mockScheduleEvent, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be("Preservica error")

      argumentVerifier.verifyInvocationsAndArgumentsPassed(1, 1, 1, 0, 0)
    }

  "handler" should "call all methods once except the call to update the time in DynamoDB if an exception was thrown when it tried to publish to SNS" in {
    val argumentVerifier = ArgumentVerifier(
      snsPublishReturnValue = IO.raiseError(
        ResourceNotFoundException
          .builder()
          .message("Something went wrong")
          .build()
      )
    )

    val thrownException = intercept[ResourceNotFoundException] {
      new Lambda().handler(mockScheduleEvent, config, argumentVerifier.dependencies).unsafeRunSync()
    }

    thrownException.getMessage should be("Something went wrong")

    argumentVerifier.verifyInvocationsAndArgumentsPassed(1, 1, 1, 1, 0)
  }

  "handler" should "should call all methods once and throw an exception if one was returned when trying to update the time in DynamoDB" in {
    val argumentVerifier = ArgumentVerifier(
      updateAttributeValuesReturnValue = IO.raiseError(
        ResourceNotFoundException
          .builder()
          .message("Something went wrong")
          .build()
      )
    )

    val thrownException = intercept[ResourceNotFoundException] {
      new Lambda().handler(mockScheduleEvent, config, argumentVerifier.dependencies).unsafeRunSync()
    }

    thrownException.getMessage should be("Something went wrong")

    argumentVerifier.verifyInvocationsAndArgumentsPassed(1, 1, 1, 1, 1)
  }
}
