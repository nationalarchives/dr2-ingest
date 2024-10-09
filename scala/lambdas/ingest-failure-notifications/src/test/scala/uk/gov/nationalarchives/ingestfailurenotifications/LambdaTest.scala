package uk.gov.nationalarchives.ingestfailurenotifications

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.ingestfailurenotifications.Lambda.*
import uk.gov.nationalarchives.ingestfailurenotifications.LambdaTestUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.{MessageStatus, MessageType, OutputMessage}

class LambdaTest extends AnyFlatSpec:

  "handler" should "send one message for each item in the lock table" in {
    val items = generateItems()
    val snsMessages = runLambda(items, SfnInput("groupId", "groupId_0"))

    snsMessages.size should equal(items.size)

    snsMessages.zipWithIndex.foreach { (message, idx) =>
      val parameters = message.parameters
      val properties = message.properties

      parameters.assetId should equal(items(idx).assetId)
      parameters.status should equal(MessageStatus.IngestError)
      properties.messageType should equal(MessageType.IngestUpdate)
      properties.messageId should equal(uuids.head)
      properties.executionId should equal("groupId_0")
      properties.parentMessageId should equal(None)
    }
  }

  "handler" should "send no messages if there are items in the lock table with a different group id" in {
    val items = generateItems()
    val snsMessages = runLambda(items, SfnInput("differentGroupId", "groupId_0"))

    snsMessages.size should equal(0)
  }

  "handler" should "send no messages if there are no items in the lock table" in {
    val snsMessages = runLambda(Nil, SfnInput("differentGroupId", "groupId_0"))

    snsMessages.size should equal(0)
  }

  "handler" should "return an error if there is an error fetching from dynamo" in {
    val ex = intercept[Exception] {
      runLambda(Nil, SfnInput("groupId", "groupId_0"), dynamoError = true)
    }
    ex.getMessage should equal("Error getting dynamo items")
  }

  "handler" should "return an error if there is an error sending to sns" in {
    val ex = intercept[Exception] {
      runLambda(Nil, SfnInput("groupId", "groupId_0"), snsError = true)
    }
    ex.getMessage should equal("Error sending to SNS")
  }
