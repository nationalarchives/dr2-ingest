package uk.gov.nationalarchives.postprocesscleanup

import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec

class LambdaTest extends AnyFlatSpec with EitherValues:
  "lambda handler" should "return an error if there are no SQS records" in {
    val sqsEvent = new com.amazonaws.services.lambda.runtime.events.SQSEvent()
    val result = Lambda.handler(sqsEvent, Config("filesTable", None), Dependencies(null, null)).attempt.unsafeRunSync()
    result.isLeft should equal(true)
    result.left.value.getMessage should equal("No SQS records")
  }

  def runLambda(sqsMessage: SQSMessage, entities: List[Entity]): Map[String, List[SQSMessage]] = {
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(sqsMessages.asJava)
    for {
      sqsMessagesRef <- Ref.of[IO, Map[String, List[SQSMessage]]](Map(inputQueue -> sqsMessages, outputQueue -> Nil))
      entitiesRef <- Ref.of[IO, List[Entity]](entities)
      _ <- new Lambda().handler(sqsEvent, config, Dependencies(entityClient(entitiesRef), sqsClient(sqsMessagesRef), () => dedupeUuid))
      messages <- sqsMessagesRef.get
    } yield messages
  }.unsafeRunSync()

