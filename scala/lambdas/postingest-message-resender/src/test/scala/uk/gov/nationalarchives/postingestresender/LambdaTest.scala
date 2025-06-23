package uk.gov.nationalarchives.postingestresender

import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import io.circe.jawn.decode
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.PostIngestStateTableItem
import uk.gov.nationalarchives.postingestresender.Helpers.*
import uk.gov.nationalarchives.postingestresender.Lambda.{Config, QueueMessage}

import java.time.Instant
import java.util.UUID

class LambdaTest extends AnyFlatSpec with EitherValues:
  given Decoder[QueueMessage] = deriveDecoder[QueueMessage]

  "handler" should "not send message and not update dynamo table when the message is within retention period" in {
    val lastQueuedTime = Instant.now().minus(java.time.Duration.ofDays(2)).toString
    val initialDynamo = List(
      PostIngestStateTableItem(
        UUID.randomUUID(),
        "batchId",
        "input",
        Some("correlationId"),
        Some("CC"),
        Some(Instant.now().minus(java.time.Duration.ofDays(6)).toString),
        Some(lastQueuedTime),
        Some("result_queue1")
      )
    )
    val placeholderInputEvent = new ScheduledEvent()
    val config = Config("testPostIngestTable", "dynamoGsi", s"""[{"queueAlias": "CC", "queueOrder": 1, "queueUrl": "${testQueueUrl}"}]""")

    // Call the handler method
    val lambdaRunResults = runLambda(initialDynamo, placeholderInputEvent, config)

    lambdaRunResults.finalItemsInTable.size should be(1)
    lambdaRunResults.finalItemsInTable.head.potentialLastQueued.get should be(lastQueuedTime)
  }

  "handler" should "update the 'lastQueued' time for all items older than the message retention period" in {
    val assetId1 = UUID.randomUUID()
    val assetId2 = UUID.randomUUID()

    val initialDynamo = List(
      PostIngestStateTableItem(
        assetId1,
        "batchId",
        "input_message_payload1",
        Some("correlationId"),
        Some("CC"),
        Some(Instant.now().minus(java.time.Duration.ofDays(6)).toString),
        Some(Instant.now().minus(java.time.Duration.ofDays(6)).toString), // 6 days old, cutoff time is 4 days
        Some("result_queue1")
      ),
      PostIngestStateTableItem(
        assetId2,
        "batchId1",
        "input_message_payload2",
        Some("correlationId1"),
        Some("CC"),
        Some(Instant.now().minus(java.time.Duration.ofDays(6)).toString),
        Some(Instant.now().minus(java.time.Duration.ofDays(16)).toString), // 16 days old, cutoff time is 4 days
        Some("result_queue1")
      )
    )
    val placeholderInputEvent = new ScheduledEvent()
    val config = Config("testPostIngestTable", "dynamoGsi", s"""[{"queueAlias": "CC", "queueOrder": 1, "queueUrl": "${testQueueUrl}"}]""")

    // Call the handler method
    val lambdaRunResults = runLambda(initialDynamo, placeholderInputEvent, config, predictableStartOfTheDay)
    val expectedUpdatedTime = predictableStartOfTheDay().toString()

    lambdaRunResults.finalItemsInTable.size should be(2)
    lambdaRunResults.finalItemsInTable.map(_.potentialLastQueued.get) should contain only expectedUpdatedTime

    val messages = lambdaRunResults.sentSqsMessages.get(testQueueUrl).get.map { message =>
      decode[QueueMessage](message.getBody).getOrElse(throw new RuntimeException("could not decode messages"))
    }
    messages.size should be(2)
    messages.find(_.assetId == assetId1).get.payload should be("input_message_payload1")
    messages.find(_.assetId == assetId2).get.payload should be("input_message_payload2")
  }

  "handler" should "not update 'lastQueued' time for an item belonging to a different queue" in {
    val uuidForUpdate = UUID.randomUUID()
    val uuidForNoUpdate = UUID.randomUUID()

    val sixDaysOld = Instant.now().minus(java.time.Duration.ofDays(6)).toString
    val sixteenDaysOld = Instant.now().minus(java.time.Duration.ofDays(16)).toString

    val initialDynamo = List(
      PostIngestStateTableItem(
        uuidForUpdate,
        "batchId",
        "this_message_to_be_resent",
        Some("correlationId"),
        Some("CC"),
        Some(sixDaysOld),
        Some(sixDaysOld), // 6 days old, cutoff time is 4 days
        Some("result_queue1")
      ),
      PostIngestStateTableItem(
        uuidForNoUpdate,
        "batchId1",
        "input",
        Some("correlationId1"),
        Some("NO_CC"), // different queue
        Some(sixteenDaysOld),
        Some(sixteenDaysOld), // 16 days old, cutoff time is 4 days
        Some("result_queue1")
      )
    )
    val placeholderInputEvent = new ScheduledEvent()
    val config = Config("testPostIngestTable", "dynamoGsi", s"""[{"queueAlias": "CC", "queueOrder": 1, "queueUrl": "$testQueueUrl"}]""")

    // Call the handler method
    val lambdaRunResults = runLambda(initialDynamo, placeholderInputEvent, config, predictableStartOfTheDay)
    val expectedUpdatedTime = predictableStartOfTheDay().toString()

    lambdaRunResults.finalItemsInTable.size should be(2)
    lambdaRunResults.finalItemsInTable.find(_.assetId.equals(uuidForUpdate)).get.potentialLastQueued.get should be(expectedUpdatedTime)
    lambdaRunResults.finalItemsInTable.find(_.assetId.equals(uuidForNoUpdate)).get.potentialLastQueued.get should be(sixteenDaysOld)

    val messages = lambdaRunResults.sentSqsMessages.get(testQueueUrl).get.map { message =>
      decode[QueueMessage](message.getBody).getOrElse(throw new RuntimeException("could not decode messages"))
    }
    messages.size should be(1)
    messages.find(_.assetId == uuidForUpdate).get.payload should be("this_message_to_be_resent")
  }

  "handler" should "report error when it cannot fetch queue attributes" in {
    val lambdaRunResults = runLambdaWithStandardParameters(2, Option(Errors(getQueueAttributes = true)))

    lambdaRunResults.result.isLeft should be(true)
    lambdaRunResults.result.left.value.getMessage should equal("Unable to retrieve queue attributes")
  }

  "handler" should "report error when it cannot send a message to the queue" in {
    val lambdaRunResults = runLambdaWithStandardParameters(6, Option(Errors(sendMessage = true)))

    lambdaRunResults.result.isLeft should be(true)
    lambdaRunResults.result.left.value.getMessage should equal("Unable to send message to queue")
  }

  "handler" should "report error when it cannot query items from dynamo table" in {
    val lambdaRunResults = runLambdaWithStandardParameters(6, Option(Errors(queryItems = true)))
    lambdaRunResults.result.isLeft should be(true)
    lambdaRunResults.result.left.value.getMessage should equal("Unable to query items from the table")
  }

  "handler" should "report error when it cannot update attribute values in the table" in {
    val lambdaRunResults = runLambdaWithStandardParameters(6, Option(Errors(updateAttributeValues = true)))
    lambdaRunResults.result.isLeft should be(true)
    lambdaRunResults.result.left.value.getMessage should equal("Unable to update attribute values in the table")
  }

  private def runLambdaWithStandardParameters(lastQueuedTime: Int, errors: Option[Errors]): LambdaRunResults = {
    val lastQueued = Instant.now().minus(java.time.Duration.ofDays(lastQueuedTime)).toString
    val initialDynamo = List(
      PostIngestStateTableItem(
        UUID.randomUUID(),
        "batchId",
        "input",
        Some("correlationId"),
        Some("CC"),
        Some(Instant.now().minus(java.time.Duration.ofDays(6)).toString),
        Some(lastQueued),
        Some("result_queue1")
      )
    )
    val placeholderInputEvent = new ScheduledEvent()
    val config = Config("testPostIngestTable", "dynamoGsi", s"""[{"queueAlias": "CC", "queueOrder": 1, "queueUrl": "${testQueueUrl}"}]""")

    // Call the handler method
    runLambda(initialDynamo, placeholderInputEvent, config, () => Instant.now(), errors)
  }
