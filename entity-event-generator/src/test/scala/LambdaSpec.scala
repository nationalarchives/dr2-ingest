package uk.gov.nationalarchives

import cats.effect.IO
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import org.joda.time.DateTime
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers._
import software.amazon.awssdk.services.dynamodb.model._
import testUtils.ExternalServicesTestUtils
import uk.gov.nationalarchives.dp.client.DataProcessor.EventAction

import java.time.ZonedDateTime
import java.util.UUID

class LambdaSpec extends ExternalServicesTestUtils with MockitoSugar {
  private val mockScheduleEvent = mock[ScheduledEvent]
  when(mockScheduleEvent.getTime).thenReturn(DateTime.parse("2023-06-07T00:00:00.000000+01:00"))

  private val mockContext = mock[Context]

  "handleRequest" should "make 3 calls to getAttributeValues, 3 calls to entitiesUpdatedSince and 2 to the others if everything goes correctly" in {
    val mockLambda = MockLambda()

    mockLambda.handleRequest(mockScheduleEvent, mockContext)
    mockLambda.verifyInvocationsAndArgumentsPassed(3, 3, 2, 2, 2)
  }

  "handleRequest" should "call getAttributeValues method once but not make the call to get entities, actions, publish to SNS nor update the time" +
    "in DynamoDB if an exception was thrown when it tried to get the datetime" in {
      val mockLambda = MockLambda(
        getAttributeValuesReturnValue = IO.raiseError(
          ResourceNotFoundException
            .builder()
            .message("Table name not found")
            .build()
        )
      )

      val thrownException = intercept[ResourceNotFoundException] {
        mockLambda.handleRequest(mockScheduleEvent, mockContext)
      }

      thrownException.getMessage should be("Table name not found")

      mockLambda.verifyInvocationsAndArgumentsPassed(1, 0, 0, 0, 0)
    }

  "handleRequest" should "call getAttributeValues and entitiesUpdatedSince methods once but not make the call to publish to SNS nor update the time in DynamoDB if no entities were returned" in {
    val mockLambda = MockLambda(Nil)
    mockLambda.handleRequest(mockScheduleEvent, mockContext)

    mockLambda.verifyInvocationsAndArgumentsPassed(1, 1, 0, 0, 0)
  }

  "handleRequest" should "call getAttributeValues and entitiesUpdatedSince 3 times and entityEventActions twice, but not " +
    "make the call to publish to SNS nor update the time in DynamoDB if the date returned is not before the event triggered date" in {
      val mockLambda = MockLambda(entityEventActionsReturnValue =
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

      mockLambda.handleRequest(mockScheduleEvent, mockContext)

      mockLambda.verifyInvocationsAndArgumentsPassed(3, 3, 2, 0, 0)
    }

  "handleRequest" should "call getAttributeValues, entitiesUpdatedSince and entityEventActions once but not " +
    "make the call to publish to SNS nor update the time in DynamoDB if something went wrong getting Event Actions" in {
      val mockLambda = MockLambda(
        entityEventActionsReturnValue = IO.raiseError(
          new Exception("Preservica error")
        )
      )

      val thrownException = intercept[Exception] {
        mockLambda.handleRequest(mockScheduleEvent, mockContext)
      }

      thrownException.getMessage should be("Preservica error")

      mockLambda.verifyInvocationsAndArgumentsPassed(1, 1, 1, 0, 0)
    }

  "handleRequest" should "call all methods once except the call to update the time in DynamoDB if an exception was thrown when it tried to publish to SNS" in {
    val mockLambda = MockLambda(
      snsPublishReturnValue = IO.raiseError(
        ResourceNotFoundException
          .builder()
          .message("Something went wrong")
          .build()
      )
    )

    val thrownException = intercept[ResourceNotFoundException] {
      mockLambda.handleRequest(mockScheduleEvent, mockContext)
    }

    thrownException.getMessage should be("Something went wrong")

    mockLambda.verifyInvocationsAndArgumentsPassed(1, 1, 1, 1, 0)
  }

  "handleRequest" should "should call all methods once and throw an exception if one was returned when trying to update the time in DynamoDB" in {
    val mockLambda = MockLambda(
      updateAttributeValuesReturnValue = IO.raiseError(
        ResourceNotFoundException
          .builder()
          .message("Something went wrong")
          .build()
      )
    )

    val thrownException = intercept[ResourceNotFoundException] {
      mockLambda.handleRequest(mockScheduleEvent, mockContext)
    }

    thrownException.getMessage should be("Something went wrong")

    mockLambda.verifyInvocationsAndArgumentsPassed(1, 1, 1, 1, 1)
  }
}
