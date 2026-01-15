package uk.gov.nationalarchives.preingesttdraggregator

import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import io.circe.Encoder
import org.scalatest.flatspec.AnyFlatSpec
import uk.gov.nationalarchives.preingesttdraggregator.Aggregator.{*, given}
import uk.gov.nationalarchives.preingesttdraggregator.Duration.*
import uk.gov.nationalarchives.utils.ExternalUtils.given
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient, utils}
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

import java.time.Instant
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

  case class StartExecutionArgs(stateMachineArn: String, sfnArguments: SFNArguments, name: Option[String])

  def dynamoClient(ref: Ref[IO, List[DADynamoDbWriteItemRequest]], dynamoErrors: Map[UUID, Boolean]): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def writeItem(dynamoDbWriteRequest: DADynamoDbWriteItemRequest): IO[Int] = {
      val assetId = UUID.fromString(dynamoDbWriteRequest.attributeNamesAndValuesToWrite("assetId").s())

      ref.update(args => dynamoDbWriteRequest :: args).flatMap { _ =>
        if dynamoErrors.getOrElse(assetId, false) then IO.raiseError(new Exception("Write item failed"))
        else IO.pure(2)
      }
    }

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      notImplemented

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = notImplemented

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

  private def validateWriteItemArgs(dynamoDbWriteItemRequest: DADynamoDbWriteItemRequest, assetId: UUID, groupId: GroupId) = {
    dynamoDbWriteItemRequest.tableName should equal("test-table")
    dynamoDbWriteItemRequest.conditionalExpression should equal(Some("attribute_not_exists(assetId)"))
    val attributes = dynamoDbWriteItemRequest.attributeNamesAndValuesToWrite
    attributes("groupId").s() should equal(groupId.groupValue)
    attributes("message").s() should equal(s"""{"id":"$assetId","location":"s3://bucket/key","messageId":"message-id","fileId":null}""")
    attributes("createdAt").s() should equal("2024-08-13T14:39:07Z")
  }

  def checkWriteItemArgs(dynamoDbWriteItemRequests: List[DADynamoDbWriteItemRequest], expectedAssetIds: List[UUID], groupId: GroupId): List[Assertion] =
    dynamoDbWriteItemRequests.map { dynamoDbWriteItemRequest =>
      val actualAssetId = UUID.fromString(dynamoDbWriteItemRequest.attributeNamesAndValuesToWrite("assetId").s())
      expectedAssetIds.contains(actualAssetId) should equal(true)
      validateWriteItemArgs(dynamoDbWriteItemRequest, actualAssetId, groupId)
    }

  def generators(instant: Instant): Generators = new Generators:
    override def generateRandomUuid: UUID = groupUUID

    override def generateInstant: Instant = instant

    override def generateRandomInt(min: Int, max: Int): Int = throw new Exception("Not implemented")

  def sfnClient(ref: Ref[IO, List[StartExecutionArgs]], sfnError: Boolean): DASFNClient[IO] = new DASFNClient[IO]:
    override def listStepFunctions(stepFunctionArn: String, status: DASFNClient.Status): IO[List[String]] = notImplemented

    override def sendTaskSuccess[T: Encoder](taskToken: String, potentialOutput: Option[T]): IO[Unit] = notImplemented

    override def startExecution[T <: Product](stateMachineArn: String, input: T, name: Option[String])(using enc: Encoder[T]): IO[StartExecutionResponse] =
      ref.update(args => StartExecutionArgs(stateMachineArn, input.asInstanceOf[SFNArguments], name) :: args).flatMap { _ =>
        if sfnError then IO.raiseError(new Exception("Error starting step function"))
        else IO.pure(StartExecutionResponse.builder.build)
      }

  private def getAggregatorOutput(
      assetIds: List[UUID],
      groupMap: Map[String, Group] = Map(),
      dynamoErrors: Map[UUID, Boolean] = Map.empty,
      sfnError: Boolean = false
  ): IO[(List[DADynamoDbWriteItemRequest], List[StartExecutionArgs], Map[String, Group], List[BatchItemFailure])] =
    for {
      writeItemArgsRef <- Ref.of[IO, List[DADynamoDbWriteItemRequest]](Nil)
      startSfnArgsRef <- Ref.of[IO, List[StartExecutionArgs]](Nil)
      groupAtomicCell <- AtomicCell[IO].of[Map[String, Group]](groupMap)
      output <- {
        val daSfnClient: DASFNClient[IO] = sfnClient(startSfnArgsRef, sfnError)
        val daDynamoClient: DADynamoDBClient[IO] = dynamoClient(writeItemArgsRef, dynamoErrors)

        given Generators = generators(instant)

        val sqsMessages = assetIds.map { assetId =>
          val sqsMessage = new SQSMessage()
          sqsMessage.setEventSourceArn("eventSourceArn")
          sqsMessage.setMessageId(assetId.toString)
          sqsMessage.setBody(s"""{"id":"$assetId","location":"s3://bucket/key","messageId":"message-id"}""")
          sqsMessage
        }

        Aggregator[IO](daSfnClient, daDynamoClient).aggregate(config, groupAtomicCell, sqsMessages, 1000)
      }
      writeItemArgs <- writeItemArgsRef.get
      startSfnArgs <- startSfnArgsRef.get
      group <- groupAtomicCell.get
    } yield (writeItemArgs, startSfnArgs, group, output)

  "aggregate" should "add a new group when there is no current group" in {
    val assetId = UUID.randomUUID
    val output = getAggregatorOutput(List(assetId))

    val (writeItemArgs, startSfnArgs, group, failures) = output.unsafeRunSync()

    failures.isEmpty should equal(true)
    group.size should equal(1)
    checkGroup(group.head._2, groupId, instant.plusMillis(2000), 1)
    checkSfnArgs(startSfnArgs.head, newBatchId, groupId)
    checkWriteItemArgs(writeItemArgs, List(assetId), groupId)
  }

  "aggregate" should "add a new group to the existing group if the expiry is before the lambda timeout" in {
    val assetId = UUID.randomUUID
    val existingGroupId = GroupId("TST")
    val groupCache = Map("eventSourceArn" -> Group(existingGroupId, instant, 1))
    val output = getAggregatorOutput(List(assetId), groupCache)

    val (writeItemArgs, startSfnArgs, group, failures) = output.unsafeRunSync()

    failures.isEmpty should equal(true)
    group.size should equal(1)
    checkGroup(group.head._2, groupId, instant.plusMillis(2000), 1)
    checkSfnArgs(startSfnArgs.head, newBatchId, groupId)
    checkWriteItemArgs(writeItemArgs, List(assetId), groupId)
  }

  "aggregate" should "add a new group to the existing group if the expiry is after the lambda timeout but items is more than max batch size" in {
    val assetId = UUID.randomUUID
    val existingGroupId = GroupId("TST")
    val later = Instant.now.plusMillis(10000)
    val groupCache = Map("eventSourceArn" -> Group(existingGroupId, later, 11))
    val output = getAggregatorOutput(List(assetId), groupCache)

    val (writeItemArgs, startSfnArgs, group, failures) = output.unsafeRunSync()

    failures.isEmpty should equal(true)
    group.size should equal(1)
    checkGroup(group.head._2, groupId, instant.plusMillis(2000), 1)
    checkSfnArgs(startSfnArgs.head, newBatchId, groupId)
    checkWriteItemArgs(writeItemArgs, List(assetId), groupId)
  }

  "aggregate" should "update the existing group's 'itemCount' if the expiry is after the lambda timeout and the group is smaller than the max" in {
    val assetId = UUID.randomUUID
    val existingGroupId = GroupId("TST")
    val later = Instant.now.plusMillis(10000)
    val groupCache = Map("eventSourceArn" -> Group(existingGroupId, later, 1))
    val output = getAggregatorOutput(List(assetId), groupCache)

    val (writeItemArgs, startSfnArgs, group, failures) = output.unsafeRunSync()

    failures.isEmpty should equal(true)
    group.size should equal(1)
    checkGroup(group.head._2, existingGroupId, later, 2)
    startSfnArgs.length should equal(0)
    checkWriteItemArgs(writeItemArgs, List(assetId), existingGroupId)
  }

  "aggregate" should "return a failed batch item if the write request returns an error" in {
    val assetId = UUID.randomUUID
    val existingGroupId = GroupId("TST")
    val later = Instant.now.plusMillis(10000)
    val groupCache = Map("eventSourceArn" -> Group(existingGroupId, later, 1))
    val (writeItemArgs, _, group, results) = getAggregatorOutput(List(assetId), groupCache, dynamoErrors = Map(assetId -> true)).unsafeRunSync()

    checkWriteItemArgs(writeItemArgs, List(assetId), existingGroupId)
    group.size should equal(1)
    checkGroup(group.head._2, existingGroupId, later, 1)
    results.size should equal(1)
    results.head.getItemIdentifier should equal(assetId.toString)
  }

  "aggregate" should "return one failed batch item if one of two write requests returns an error" in {
    val assetIdOne = UUID.randomUUID
    val assetIdTwo = UUID.randomUUID

    val existingGroupId = GroupId("TST")
    val later = Instant.now.plusMillis(10000)
    val groupCache = Map("eventSourceArn" -> Group(existingGroupId, later, 1))

    val (writeItemArgs, _, group, resultsOne) = getAggregatorOutput(List(assetIdOne, assetIdTwo), groupCache, dynamoErrors = Map(assetIdOne -> true)).unsafeRunSync()
    writeItemArgs.size should equal(2)
    checkWriteItemArgs(writeItemArgs, List(assetIdOne, assetIdTwo), existingGroupId)
    resultsOne.size should equal(1)
    resultsOne.head.getItemIdentifier should equal(assetIdOne.toString)

    val (writeItemArgs2, _, group2, resultsTwo) = getAggregatorOutput(List(assetIdOne, assetIdTwo), groupCache, dynamoErrors = Map(assetIdTwo -> true)).unsafeRunSync()
    checkWriteItemArgs(writeItemArgs, List(assetIdOne, assetIdTwo), existingGroupId)
    group2.size should equal(1)
    checkGroup(group2.head._2, existingGroupId, later, 2)
    resultsTwo.size should equal(1)
    resultsTwo.head.getItemIdentifier should equal(assetIdTwo.toString)
  }

  "aggregate" should "return a failed batch item if the step function request returns an error" in {
    val assetId = UUID.randomUUID
    val existingGroupId = GroupId("TST")
    val later = Instant.now
    val groupCache = Map("eventSourceArn" -> Group(existingGroupId, later, 11))

    val (writeItemArgs, startSfnArgs, group, results) = getAggregatorOutput(List(assetId), groupCache, sfnError = true).unsafeRunSync()
    checkWriteItemArgs(writeItemArgs, List(assetId), groupId)
    checkSfnArgs(startSfnArgs.head, newBatchId, groupId)
    group.size should equal(1)
    checkGroup(group.head._2, existingGroupId, later, 11)
    results.size should equal(1)
    results.head.getItemIdentifier should equal(assetId.toString)
  }
