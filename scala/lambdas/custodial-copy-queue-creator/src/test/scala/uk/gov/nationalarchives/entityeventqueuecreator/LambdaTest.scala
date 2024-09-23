package uk.gov.nationalarchives.entityeventqueuecreator

import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import io.circe.parser.decode
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.{Entities, EntityClient}
import uk.gov.nationalarchives.entityeventqueuecreator.Lambda.*
import uk.gov.nationalarchives.entityeventqueuecreator.Lambda.MessageBody.*
import uk.gov.nationalarchives.entityeventqueuecreator.Utils.*

import java.util.UUID

class LambdaTest extends AnyFlatSpec with EitherValues:

  "lambda handler" should "send the correct message for an IO" in {
    val message = new SQSMessage()
    val id = UUID.randomUUID
    message.setBody(s"""{"id": "io:$id", "deleted": false}""")

    val sqsMessages = runLambda(List(message), Nil)

    sqsMessages(inputQueue).size should equal(0)
    sqsMessages(outputQueue).size should equal(1)

    val sqsMessage =sqsMessages(outputQueue).head

    sqsMessage.getMessageId should equal(id.toString)
    val messageBody = decode[MessageBody](sqsMessage.getBody).value
    messageBody.isInstanceOf[IoMessageBody] should equal(true)
    messageBody.id should equal(id)
    messageBody.deleted should equal(false)
  }

  "lambda handler" should "send a message with the CO id if the CO has no parent" in {
    val message = new SQSMessage()
    val id = UUID.randomUUID
    message.setBody(s"""{"id": "co:$id", "deleted": false}""")
    val sqsMessages = runLambda(List(message), List(createEntity(ContentObject, id)))
    sqsMessages(inputQueue).size should equal(0)
    sqsMessages(outputQueue).size should equal(1)

    val sqsMessage = sqsMessages(outputQueue).head
    val messageBody = decode[MessageBody](sqsMessage.getBody).value
    messageBody.isInstanceOf[CoMessageBody] should equal(true)
    messageBody.id should equal(id)
    messageBody.deleted should equal(false)
  }

  "lambda handler" should "send a message with the parent ID for a CO if there is a parent ID" in {
    val message = new SQSMessage()
    val coId = UUID.randomUUID
    val ioId = UUID.randomUUID
    message.setBody(s"""{"id": "co:$coId", "deleted": false}""")

    val sqsMessages = runLambda(List(message), List(createEntity(ContentObject, coId, ioId.some)))
    sqsMessages(inputQueue).size should equal(0)
    sqsMessages(outputQueue).size should equal(1)

    val sqsMessage = sqsMessages(outputQueue).head

    sqsMessage.getMessageId should equal(ioId.toString)
    val messageBody = decode[MessageBody](sqsMessage.getBody).value
    messageBody.isInstanceOf[CoMessageBody] should equal(true)
    messageBody.id should equal(coId)
    messageBody.deleted should equal(false)
  }

  "lambda handler" should "raise an error for a CO message if no entity is found" in {
    val message = new SQSMessage()
    val coId = UUID.randomUUID
    val ioId = UUID.randomUUID
    message.setBody(s"""{"id": "co:$coId", "deleted": false}""")
    val ex = intercept[Exception] {
      runLambda(List(message), Nil)
    }
    ex.getMessage should equal(s"Entity $coId not found")
  }

  "lambda handler" should "use the CO for the message group ID if the entity is deleted" in {
    val message = new SQSMessage()
    val coId = UUID.randomUUID
    message.setBody(s"""{"id": "co:$coId", "deleted": true}""")

    val sqsMessages = runLambda(List(message), Nil)
    sqsMessages(inputQueue).size should equal(0)
    sqsMessages(outputQueue).size should equal(1)

    val sqsMessage = sqsMessages(outputQueue).head

    sqsMessage.getMessageId should equal(coId.toString)
  }

  "lambda handler" should "not send a message if this is an SO message" in {
    val message = new SQSMessage()
    val id = UUID.randomUUID
    message.setBody(s"""{"id": "so:$id", "deleted": false}""")
    val sqsMessages = runLambda(List(message), Nil)
    sqsMessages(inputQueue).size should equal(0)
    sqsMessages(outputQueue).size should equal(0)
  }

  private def createEntity(entityType: EntityType, id: UUID, parent: Option[UUID] = None) =
    Entity(Option(entityType), id, None, None, false, None, None, parent)
