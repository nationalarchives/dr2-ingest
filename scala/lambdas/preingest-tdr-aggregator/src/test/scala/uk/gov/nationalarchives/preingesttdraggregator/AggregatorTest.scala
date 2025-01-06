package uk.gov.nationalarchives.preingesttdraggregator

import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Outcome, Ref}
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import io.circe.Encoder
import org.scalatest.flatspec.AnyFlatSpec
import uk.gov.nationalarchives.preingesttdraggregator.Aggregator.{*, given}
import uk.gov.nationalarchives.preingesttdraggregator.Duration.*
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient}
import uk.gov.nationalarchives.preingesttdraggregator.Lambda.{Config, Group}
import io.circe.generic.auto.*
import org.scalatest.{Assertion, EitherValues}
import org.scalatest.matchers.should.Matchers.*
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbWriteItemRequest
import uk.gov.nationalarchives.preingesttdraggregator.Ids.GroupId
import uk.gov.nationalarchives.utils.Generators

import java.net.URI
import java.time.Instant
import java.util
import java.util.UUID
import scala.collection.immutable.Map

class AggregatorTest extends AnyFlatSpec with EitherValues:
  val groupUUID: UUID = UUID.randomUUID
  val groupId: GroupId = GroupId("TST", groupUUID)
  val newBatchId = s"${groupId}_0"
  val config: Config = Config("test-table", "TST", "sfnArn", 1.seconds, 10)
  given DASFNClient[IO] = DASFNClient[IO]()
  given DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  val instant: Instant = Instant.ofEpochSecond(1723559947)
  def notImplemented[A]: IO[A] = IO.raiseError(new Exception("Not implemented"))

  def notImplemented[T]: IO[Nothing] = IO.raiseError(new Exception("Not implemented"))

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

  def checkGroup(group: Group, groupId: GroupId, instant: Instant, items: Int): Assertion = {
    group.groupId should equal(groupId.groupValue)
    group.expires should equal(instant)
    group.itemCount should equal(items)
  }

  def checkSfnArgs(startExecutionArgs: StartExecutionArgs, batchId: String, groupId: GroupId): Assertion = {
    startExecutionArgs.stateMachineArn should equal("sfnArn")
    val arguments = startExecutionArgs.sfnArguments
    arguments.batchId should equal(batchId)
    arguments.groupId should equal(groupId)
    arguments.waitFor.length should equal(2)
    startExecutionArgs.name.get should equal(batchId)
  }

  def checkWriteItemArgs(dynamoDbWriteItemRequest: DADynamoDbWriteItemRequest, assetId: UUID, groupId: GroupId): Assertion = {
    dynamoDbWriteItemRequest.tableName should equal("test-table")
    dynamoDbWriteItemRequest.conditionalExpression should equal(Some("attribute_not_exists(assetId)"))
    val attributes = dynamoDbWriteItemRequest.attributeNamesAndValuesToWrite
    attributes("assetId").s() should equal(assetId.toString)
    attributes("groupId").s() should equal(groupId.groupValue)
    attributes("message").s() should equal(s"""{"id":"$assetId","location":"s3://bucket/key","messageId":"message-id"}""")
  }

  def generators(instant: Instant): Generators = new Generators:
    override def generateRandomUuid: UUID = groupUUID

    override def generateInstant: Instant = instant

    override def generateRandomInt(min: Int, max: Int): Int = throw new Exception("Not implemented")

  def sfnClient(ref: Ref[IO, List[StartExecutionArgs]], sfnError: Boolean): DASFNClient[IO] = new DASFNClient[IO]:
    override def listStepFunctions(stepFunctionArn: String, status: DASFNClient.Status): IO[List[String]] = notImplemented

    override def sendTaskSuccess(taskToken: String): IO[Unit] = notImplemented

    override def startExecution[T <: Product](stateMachineArn: String, input: T, name: Option[String])(using enc: Encoder[T]): IO[StartExecutionResponse] =
      if sfnError then IO.raiseError(new Exception("Error starting step function"))
      else ref.update(args => StartExecutionArgs(stateMachineArn, input.asInstanceOf[SFNArguments], name) :: args).map(_ => StartExecutionResponse.builder.build)

    override def listStepFunctions(stepFunctionArn: String, status: DASFNClient.Status): IO[List[String]] = notImplemented

    override def sendTaskSuccess(taskToken: String): IO[Unit] = notImplemented

  private def getAggregatorOutput(
      assetId: UUID,
      groupMap: Map[String, Group] = Map(),
      dynamoError: Boolean = false,
      sfnError: Boolean = false
  ): IO[(List[DADynamoDbWriteItemRequest], List[StartExecutionArgs], Map[String, Group], List[Outcome[IO, Throwable, Unit]])] =
    for {
      writeItemArgsRef <- Ref.of[IO, List[DADynamoDbWriteItemRequest]](Nil)
      startSfnArgsRef <- Ref.of[IO, List[StartExecutionArgs]](Nil)
      groupAtomicCell <- AtomicCell[IO].of[Map[String, Group]](groupMap)
      output <- {
        given DASFNClient[IO] = sfnClient(startSfnArgsRef, sfnError)
        given DADynamoDBClient[IO] = dynamoClient(writeItemArgsRef, dynamoError)
        given Generators = generators(instant)

        val sqsMessage = new SQSMessage()
        sqsMessage.setEventSourceArn("eventSourceArn")
        sqsMessage.setBody(s"""{"id":"$assetId","location":"s3://bucket/key","messageId":"message-id"}""")

        Aggregator[IO].aggregate(config, groupAtomicCell, List(sqsMessage), 1000)
      }
      writeItemArgs <- writeItemArgsRef.get
      startSfnArgs <- startSfnArgsRef.get
      group <- groupAtomicCell.get
    } yield (writeItemArgs, startSfnArgs, group, output)

  "aggregate" should "add a new group when there is no current group" in {
    val assetId = UUID.randomUUID
    val output = getAggregatorOutput(assetId)

    val (writeItemArgs, startSfnArgs, group, results) = output.unsafeRunSync()

    results.forall(_.isSuccess) should equal(true)
    group.size should equal(1)
    checkGroup(group.head._2, groupId, instant.plusMillis(2000), 1)
    checkSfnArgs(startSfnArgs.head, newBatchId, groupId)
    checkWriteItemArgs(writeItemArgs.head, assetId, groupId)
  }

  "aggregate" should "add a new group to the existing group if the expiry is before the lambda timeout" in {
    val assetId = UUID.randomUUID
    val existingGroupId = GroupId("TST")
    val groupCache = Map("eventSourceArn" -> Group(existingGroupId, instant, 1))
    val output = getAggregatorOutput(assetId, groupCache)

    val (writeItemArgs, startSfnArgs, group, results) = output.unsafeRunSync()

    results.forall(_.isSuccess) should equal(true)
    group.size should equal(1)
    checkGroup(group.head._2, groupId, instant.plusMillis(2000), 1)
    checkSfnArgs(startSfnArgs.head, newBatchId, groupId)
    checkWriteItemArgs(writeItemArgs.head, assetId, groupId)
  }

  "aggregate" should "add a new group to the existing group if the expiry is after the lambda timeout but items is more than max batch size" in {
    val assetId = UUID.randomUUID
    val existingGroupId = GroupId("TST")
    val later = Instant.now.plusMillis(10000)
    val groupCache = Map("eventSourceArn" -> Group(existingGroupId, later, 11))
    val output = getAggregatorOutput(assetId, groupCache)

    val (writeItemArgs, startSfnArgs, group, results) = output.unsafeRunSync()

    results.forall(_.isSuccess) should equal(true)
    group.size should equal(1)
    checkGroup(group.head._2, groupId, instant.plusMillis(2000), 1)
    checkSfnArgs(startSfnArgs.head, newBatchId, groupId)
    checkWriteItemArgs(writeItemArgs.head, assetId, groupId)
  }

  "aggregate" should "not add a new group if the expiry is after the lambda timeout and the group is smaller than the max" in {
    val assetId = UUID.randomUUID
    val existingGroupId = GroupId("TST")
    val later = Instant.now.plusMillis(10000)
    val groupCache = Map("eventSourceArn" -> Group(existingGroupId, later, 1))
    val output = getAggregatorOutput(assetId, groupCache)

    val (writeItemArgs, startSfnArgs, group, results) = output.unsafeRunSync()

    results.forall(_.isSuccess) should equal(true)
    group.size should equal(1)
    checkGroup(group.head._2, existingGroupId, later, 2)
    startSfnArgs.length should equal(0)
    checkWriteItemArgs(writeItemArgs.head, assetId, existingGroupId)
  }

  "aggregate" should "return a failed outcome if the write request returns an error" in {
    val assetId = UUID.randomUUID
    val (_, _, _, results) = getAggregatorOutput(assetId, dynamoError = true).unsafeRunSync()

    results.head.isError should equal(true)
    val errorMessage = results.head match
      case Outcome.Errored(e) => e.getMessage
      case _                  => ""
    errorMessage should equal("Write item failed")
  }

  "aggregate" should "return a failed outcome if the step function request returns an error" in {
    val assetId = UUID.randomUUID
    val (_, _, _, results) = getAggregatorOutput(assetId, sfnError = true).unsafeRunSync()

    results.head.isError should equal(true)
    val errorMessage = results.head match
      case Outcome.Errored(e) => e.getMessage
      case _                  => ""
    errorMessage should equal("Error starting step function")
  }
