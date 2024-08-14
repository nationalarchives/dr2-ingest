package uk.gov.nationalarchives.eventaggregator

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Outcome, Ref}
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import io.circe.Encoder
import org.scalatest.flatspec.AnyFlatSpec
import uk.gov.nationalarchives.eventaggregator.Aggregator.{Input, SFNArguments, given}
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient}
import uk.gov.nationalarchives.eventaggregator.Lambda.{Batch, Config}
import io.circe.generic.auto.*
import org.scalatest.{Assertion, EitherValues}
import org.scalatest.matchers.should.Matchers.*
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbWriteItemRequest

import java.net.URI
import java.time.Instant
import java.util
import java.util.UUID
import scala.collection.immutable.Map

class AggregatorTest extends AnyFlatSpec with EitherValues:
  val groupUUID: UUID = UUID.randomUUID
  val newBatchId = s"TST_${groupUUID}_0"
  val config: Config = Config("test-table", "", "TST", "sfnArn", 1, 10)
  given DASFNClient[IO] = DASFNClient[IO]()
  given DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  val instant: Instant = Instant.ofEpochSecond(1723559947)

  case class StartExecutionArgs(stateMachineArn: String, sfnArguments: SFNArguments, name: Option[String])

  def dynamoClient(ref: Ref[IO, List[DADynamoDbWriteItemRequest]], failWrite: Boolean): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.pure(Nil)

    override def writeItem(dynamoDbWriteRequest: DADynamoDbWriteItemRequest): IO[Int] =
      if failWrite then IO.raiseError(new Exception("Write item failed")) else ref.update(args => dynamoDbWriteRequest :: args).map(_ => 2)

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.pure(Nil)

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      IO.pure(Nil)

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = IO.pure(Nil)

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = IO.pure(1)

  def checkLockTableCall(messageId: UUID, args: List[DADynamoDbWriteItemRequest], expectedBatchId: String): Assertion = {
    args.length should equal(1)
    val tableName = args.head.tableName
    val attributeNamesAndValues = args.head.attributeNamesAndValuesToWrite
    val conditionalExpression = args.head.conditionalExpression
    true should equal(true)
  }

  def checkBatch(batch: Batch, batchId: String, instant: Instant, items: Int): Assertion = {
    batch.batchId should equal(batchId)
    batch.expires should equal(instant)
    batch.items should equal(items)
  }

  def checkSfnArgs(startExecutionArgs: StartExecutionArgs, batchId: String): Assertion = {
    startExecutionArgs.stateMachineArn should equal("sfnArn")
    val groupId = batchId.dropRight(2)
    val arguments = startExecutionArgs.sfnArguments
    arguments.batchId should equal(batchId)
    arguments.groupId should equal(batchId.dropRight(2))
    arguments.waitFor should equal(2)
    startExecutionArgs.name.get should equal(batchId)
  }

  def checkWriteItemArgs(dynamoDbWriteItemRequest: DADynamoDbWriteItemRequest, messageId: UUID, batchId: String): Assertion = {
    dynamoDbWriteItemRequest.tableName should equal("test-table")
    dynamoDbWriteItemRequest.conditionalExpression should equal(None)
    val attributes = dynamoDbWriteItemRequest.attributeNamesAndValuesToWrite
    attributes("messageId").s() should equal(messageId.toString)
    attributes("batchId").s() should equal(batchId)
    attributes("message").s() should equal(s"""{"id":"$messageId","location":"s3://bucket/key"}""")
  }

  def generators(instant: Instant): Generators[IO] = new Generators[IO]:
    override def generateUuid: UUID = groupUUID

    override def generateInstant: Instant = instant

  def sfnClient(ref: Ref[IO, List[StartExecutionArgs]], sfnError: Boolean): DASFNClient[IO] = new DASFNClient[IO]:
    override def startExecution[T <: Product](stateMachineArn: String, input: T, name: Option[String])(using enc: Encoder[T]): IO[StartExecutionResponse] =
      if sfnError then IO.raiseError(new Exception("Error starting step function"))
      else ref.update(args => StartExecutionArgs(stateMachineArn, input.asInstanceOf[SFNArguments], name) :: args).map(_ => StartExecutionResponse.builder.build)

  private def getAggregatorOutput(
      messageId: UUID,
      batchMap: Map[String, Batch] = Map(),
      dynamoError: Boolean = false,
      sfnError: Boolean = false
  ): IO[(List[DADynamoDbWriteItemRequest], List[StartExecutionArgs], Map[String, Batch], List[Outcome[IO, Throwable, Unit]])] =
    for {
      writeItemArgsRef <- Ref.of[IO, List[DADynamoDbWriteItemRequest]](Nil)
      startSfnArgsRef <- Ref.of[IO, List[StartExecutionArgs]](Nil)
      batchRef <- Ref.of[IO, Map[String, Batch]](batchMap)
      output <- {
        given DASFNClient[IO] = sfnClient(startSfnArgsRef, sfnError)
        given DADynamoDBClient[IO] = dynamoClient(writeItemArgsRef, dynamoError)
        given Generators[IO] = generators(instant)

        val sqsMessage = new SQSMessage()
        sqsMessage.setEventSourceArn("eventSourceArn")
        sqsMessage.setBody(s"""{"id":"$messageId","location":"s3://bucket/key"}""")

        Aggregator[IO].aggregate(config, batchRef, List(sqsMessage), 1000)
      }
      writeItemArgs <- writeItemArgsRef.get
      startSfnArgs <- startSfnArgsRef.get
      batch <- batchRef.get
    } yield (writeItemArgs, startSfnArgs, batch, output)

  "aggregate" should "add a new batch when there is no current batch" in {
    val messageId = UUID.randomUUID
    val output = getAggregatorOutput(messageId)

    val (writeItemArgs, startSfnArgs, batch, results) = output.unsafeRunSync()

    results.forall(_.isSuccess) should equal(true)
    batch.size should equal(1)
    checkLockTableCall(messageId, writeItemArgs, newBatchId)
    checkBatch(batch.head._2, newBatchId, instant.plusMillis(2000), 1)
    checkSfnArgs(startSfnArgs.head, newBatchId)
    checkWriteItemArgs(writeItemArgs.head, messageId, newBatchId)
  }

  "aggregate" should "add a new batch if the existing batch if the expiry is before the lambda timeout" in {
    val messageId = UUID.randomUUID
    val existingBatchId = s"TST_${UUID.randomUUID}_0"
    val batchCache = Map("eventSourceArn" -> Batch(existingBatchId, instant, 1))
    val output = getAggregatorOutput(messageId, batchCache)

    val (writeItemArgs, startSfnArgs, batch, results) = output.unsafeRunSync()

    results.forall(_.isSuccess) should equal(true)
    batch.size should equal(1)
    checkLockTableCall(messageId, writeItemArgs, newBatchId)
    checkBatch(batch.head._2, newBatchId, instant.plusMillis(2000), 1)
    checkSfnArgs(startSfnArgs.head, newBatchId)
    checkWriteItemArgs(writeItemArgs.head, messageId, newBatchId)
  }

  "aggregate" should "add a new batch if the existing batch if the expiry is after the lambda timeout but items is more than max batch size" in {
    val messageId = UUID.randomUUID
    val existingBatchId = s"TST_${UUID.randomUUID}_0"
    val later = Instant.now.plusMillis(10000)
    val batchCache = Map("eventSourceArn" -> Batch(existingBatchId, later, 11))
    val output = getAggregatorOutput(messageId, batchCache)

    val (writeItemArgs, startSfnArgs, batch, results) = output.unsafeRunSync()

    results.forall(_.isSuccess) should equal(true)
    batch.size should equal(1)
    checkLockTableCall(messageId, writeItemArgs, newBatchId)
    checkBatch(batch.head._2, newBatchId, instant.plusMillis(2000), 1)
    checkSfnArgs(startSfnArgs.head, newBatchId)
    checkWriteItemArgs(writeItemArgs.head, messageId, newBatchId)
  }

  "aggregate" should "not add a new batch if the expiry is after the lambda timeout and the batch is smaller than the max" in {
    val messageId = UUID.randomUUID
    val existingBatchId = s"TST_${UUID.randomUUID}_0"
    val later = Instant.now.plusMillis(10000)
    val batchCache = Map("eventSourceArn" -> Batch(existingBatchId, later, 1))
    val output = getAggregatorOutput(messageId, batchCache)

    val (writeItemArgs, startSfnArgs, batch, results) = output.unsafeRunSync()

    results.forall(_.isSuccess) should equal(true)
    batch.size should equal(1)
    checkLockTableCall(messageId, writeItemArgs, newBatchId)
    checkBatch(batch.head._2, existingBatchId, later, 2)
    startSfnArgs.length should equal(0)
    checkWriteItemArgs(writeItemArgs.head, messageId, existingBatchId)
  }

  "aggregate" should "return a failed outcome if the write request returns an error" in {
    val messageId = UUID.randomUUID
    val (_, _, _, results) = getAggregatorOutput(messageId, dynamoError = true).unsafeRunSync()

    results.head.isError should equal(true)
    val errorMessage = results.head match
      case Outcome.Errored(e) => e.getMessage
      case _                  => ""
    errorMessage should equal("Write item failed")
  }

  "aggregate" should "return a failed outcome if the step function request returns an error" in {
    val messageId = UUID.randomUUID
    val (_, _, _, results) = getAggregatorOutput(messageId, sfnError = true).unsafeRunSync()

    results.head.isError should equal(true)
    val errorMessage = results.head match
      case Outcome.Errored(e) => e.getMessage
      case _                  => ""
    errorMessage should equal("Error starting step function")
  }
