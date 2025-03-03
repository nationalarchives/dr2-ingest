package uk.gov.nationalarchives.preingesttdraggregator

import cats.effect.IO
import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.{ClientContext, CognitoIdentity, Context, LambdaLogger}
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.amazonaws.services.lambda.runtime.events.{SQSBatchResponse, SQSEvent}
import io.circe.{Decoder, Encoder}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.preingesttdraggregator.Aggregator.Input
import uk.gov.nationalarchives.preingesttdraggregator.Duration.Seconds
import uk.gov.nationalarchives.preingesttdraggregator.Lambda.Config

import scala.jdk.CollectionConverters.*

class LambdaTest extends AnyFlatSpec {

  private val config = Config("", "", "", Seconds(1), 1)

  "lambda run" should "return the failed messages if there are failures" in {
    val aggregator = new Aggregator[IO] {
      override def aggregate(config: Lambda.Config, atomicCell: AtomicCell[IO, Map[String, Lambda.Group]], messages: List[SQSEvent.SQSMessage], remainingTimeInMillis: Int)(using
          Encoder[Aggregator.SFNArguments],
          Decoder[Input]
      ): IO[List[SQSBatchResponse.BatchItemFailure]] =
        IO.pure(List(BatchItemFailure.builder().withItemIdentifier("identifier").build()))
    }
    val response = new Lambda().run(aggregator, sqsEvent, context, config).unsafeRunSync()
    val itemFailures = response.getBatchItemFailures.asScala
    itemFailures.size should equal(1)
    itemFailures.head.getItemIdentifier should equal("identifier")
  }

  "lambda run" should "return an empty list if there are no failures" in {
    val aggregator = new Aggregator[IO] {
      override def aggregate(config: Lambda.Config, atomicCell: AtomicCell[IO, Map[String, Lambda.Group]], messages: List[SQSEvent.SQSMessage], remainingTimeInMillis: Int)(using
          Encoder[Aggregator.SFNArguments],
          Decoder[Input]
      ): IO[List[SQSBatchResponse.BatchItemFailure]] =
        IO.pure(Nil)
    }
    val response = new Lambda().run(aggregator, sqsEvent, context, config).unsafeRunSync()
    val itemFailures = response.getBatchItemFailures.asScala
    itemFailures.size should equal(0)
  }

  private def sqsEvent: SQSEvent = {
    val sqsMessage = new SQSMessage()
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(List(sqsMessage).asJava)
    sqsEvent
  }

  private def context: Context = new Context:
    override def getAwsRequestId: String = ""

    override def getLogGroupName: String = ""

    override def getLogStreamName: String = ""

    override def getFunctionName: String = ""

    override def getFunctionVersion: String = ""

    override def getInvokedFunctionArn: String = ""

    override def getIdentity: CognitoIdentity = null

    override def getClientContext: ClientContext = null

    override def getRemainingTimeInMillis: Int = 1

    override def getMemoryLimitInMB: Int = 1

    override def getLogger: LambdaLogger = null
}
