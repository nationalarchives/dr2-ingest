package uk.gov.nationalarchives

import cats.effect.IO
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers._
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.sns.model.SnsException
import testUtils.ExternalServicesTestUtils
import uk.gov.nationalarchives.dp.client.PreservicaClientException

class LambdaTest extends ExternalServicesTestUtils with MockitoSugar {
  private val mockScheduleEvent = mock[ScheduledEvent]
  private val mockContext = mock[Context]

  "handleRequest" should "make the correct amount of calls to the client, DDB and SNS if everything goes correctly" +
    "and there is a new version" in {
      val mockLambda = MockLambda()

      mockLambda.handleRequest(mockScheduleEvent, mockContext)
      mockLambda.verifyInvocationsAndArgumentsPassed()
    }

  "handleRequest" should "only call 'getItems' and return an DynamoDB exception if one was thrown when it tried to get the current version" in {
    val mockLambda = MockLambda(
      getCurrentPreservicaVersionReturnValue = IO.raiseError(
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

    mockLambda.verifyInvocationsAndArgumentsPassed(1, 0, 0)
  }

  "handleRequest" should "only call 'getItems' but return a runtimeException if the list of items returned was empty" in {
    val mockLambda = MockLambda(getCurrentPreservicaVersionReturnValue = IO.pure(Nil))

    val thrownException = intercept[RuntimeException] {
      mockLambda.handleRequest(mockScheduleEvent, mockContext)
    }

    thrownException.getMessage should be("The version of Preservica we are using was not found")

    mockLambda.verifyInvocationsAndArgumentsPassed(1, 0, 0)
  }

  "handleRequest" should "call the 'getItems' but after calling 'getPreservicaNamespaceVersion', return an exception " +
    "if one was thrown when it tried to get the latest version of Preservica" in {
      val mockLambda = MockLambda(
        getPreservicaNamespaceVersionReturnValue = IO.raiseError(
          PreservicaClientException("An error occurred")
        )
      )
      val thrownException = intercept[PreservicaClientException] {
        mockLambda.handleRequest(mockScheduleEvent, mockContext)
      }

      thrownException.getMessage should be("An error occurred")

      mockLambda.verifyInvocationsAndArgumentsPassed(1, 1, 0)
    }

  "handleRequest" should "call the 'getItems' and 'getPreservicaNamespaceVersion' but after calling 'publish', return an exception " +
    "if one was thrown when it tried to publish to an SNS topic" in {
      val mockLambda = MockLambda(
        snsPublishReturnValue = IO.raiseError(
          SnsException
            .builder()
            .message("An SNS error has occurred")
            .build()
        )
      )
      val thrownException = intercept[SnsException] {
        mockLambda.handleRequest(mockScheduleEvent, mockContext)
      }

      thrownException.getMessage should be("An SNS error has occurred")

      mockLambda.verifyInvocationsAndArgumentsPassed()
    }
}
