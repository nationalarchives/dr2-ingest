package uk.gov.nationalarchives.postingestresender

import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.PostIngestStateTableItem
import uk.gov.nationalarchives.postingestresender.Helpers.*
import uk.gov.nationalarchives.postingestresender.Lambda.Config

import java.time.Instant
import java.util.UUID

class LambdaTest extends AnyFlatSpec with EitherValues:

  "handler" should "not send message and not update dynamo when the message is within retention period" in {

    val initialDynamo = List(
      PostIngestStateTableItem(
        UUID.randomUUID(),
        "batchId",
        "input",
        Some("correlationId"),
        Some("CC"),
        Some(Instant.now().minus(java.time.Duration.ofDays(4)).toString),
        Some(Instant.now().minus(java.time.Duration.ofDays(4)).toString),
        Some("result_queue1")
      )
    )
    val inputEvent = new ScheduledEvent()
    val config = Config("testPostIngestTable", "dynamoGsi", s"""[{"queueAlias": "CC", "queueOrder": 1, "queueUrl": "${testQueueUrl}"}]""")

    // Call the handler method
    val lambdaRunResults = runLambda(initialDynamo, inputEvent, config)

    lambdaRunResults.finalItemsInTable.size should be(1)
  }
