package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import org.scalatest.matchers.should.Matchers.*
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.sns.model.SnsException
import uk.gov.nationalarchives.Lambda.Config
import uk.gov.nationalarchives.dp.client.PreservicaClientException
import uk.gov.nationalarchives.testUtils.ExternalServicesTestUtils

class LambdaTest extends ExternalServicesTestUtils with MockitoSugar {
  private val mockScheduleEvent = mock[ScheduledEvent]
  val config: Config = Config("", "", "arn:aws:sns:eu-west-2:123456789012:MockResourceId", "table-name")

  "handler" should "make the correct amount of calls to the client, DDB and SNS if everything goes correctly" +
    "and there is a new version" in {
      val argumentVerifier = ArgumentVerifier()

      new Lambda().handler(mockScheduleEvent, config, argumentVerifier.dependencies).unsafeRunSync()
      argumentVerifier.verifyInvocationsAndArgumentsPassed()
    }

  "handler" should "only call 'getItems' and return an DynamoDB exception if one was thrown when it tried to get the current version" in {
    val argumentVerifier = ArgumentVerifier(
      getCurrentPreservicaVersionReturnValue = IO.raiseError(
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

    argumentVerifier.verifyInvocationsAndArgumentsPassed(1, 0, 0)
  }

  "handler" should "only call 'getItems' but return a runtimeException if the list of items returned was empty" in {
    val argumentVerifier = ArgumentVerifier(getCurrentPreservicaVersionReturnValue = IO.pure(Nil))

    val thrownException = intercept[RuntimeException] {
      new Lambda().handler(mockScheduleEvent, config, argumentVerifier.dependencies).unsafeRunSync()
    }

    thrownException.getMessage should be("The version of Preservica we are using was not found")

    argumentVerifier.verifyInvocationsAndArgumentsPassed(1, 0, 0)
  }

  "handler" should "call the 'getItems' but after calling 'getPreservicaNamespaceVersion', return an exception " +
    "if one was thrown when it tried to get the latest version of Preservica" in {
      val argumentVerifier = ArgumentVerifier(
        getPreservicaNamespaceVersionReturnValue = IO.raiseError(
          PreservicaClientException("An error occurred")
        )
      )
      val thrownException = intercept[PreservicaClientException] {
        new Lambda().handler(mockScheduleEvent, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be("An error occurred")

      argumentVerifier.verifyInvocationsAndArgumentsPassed(1, 1, 0)
    }

  "handler" should "call the 'getItems' and 'getPreservicaNamespaceVersion' but after calling 'publish', return an exception " +
    "if one was thrown when it tried to publish to an SNS topic" in {
      val argumentVerifier = ArgumentVerifier(
        snsPublishReturnValue = IO.raiseError(
          SnsException
            .builder()
            .message("An SNS error has occurred")
            .build()
        )
      )
      val thrownException = intercept[SnsException] {
        new Lambda().handler(mockScheduleEvent, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be("An SNS error has occurred")

      argumentVerifier.verifyInvocationsAndArgumentsPassed()
    }
}
