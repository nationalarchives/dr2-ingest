package uk.gov.nationalarchives.e2etests

import cats.syntax.all.*
import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import fs2.interop.reactivestreams.*
import io.circe.{Decoder, Encoder}
import org.reactivestreams.Publisher
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.s3.model.{DeleteObjectsResponse, HeadObjectResponse, PutObjectResponse}
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse
import software.amazon.awssdk.services.sqs.model.{DeleteMessageResponse, SendMessageResponse}
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CompletedUpload}
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbWriteItemRequest
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.e2etests.StepDefs.*
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client, DASFNClient, DASQSClient}

import java.nio.ByteBuffer
import java.util.UUID
import scala.concurrent.duration.*

object E2ESpecUtils:
  val pollingDuration: FiniteDuration = 1.second

  def notImplemented[A]: IO[A] = IO.raiseError(new Exception("Not implemented"))

  val unusedStepDefs: StepDefs[IO] = new StepDefs[IO]:
    override def createFiles(number: Int, emptyChecksum: Boolean, invalidMetadata: Boolean, invalidChecksum: Boolean)(using ref: Ref[IO, List[UUID]]): IO[Unit] = notImplemented

    override def sendMessages(ref: Ref[IO, List[UUID]]): IO[Unit] = notImplemented

    override def createBatch(ref: Ref[IO, List[UUID]]): IO[Unit] = notImplemented

    override def waitForIngestCompleteMessages(ref: Ref[IO, List[UUID]]): IO[Unit] = notImplemented

    override def waitForIngestErrorMessages(ref: Ref[IO, List[UUID]]): IO[Unit] = notImplemented

    override def pollForTDRValidationMessages(ref: Ref[IO, List[UUID]]): IO[Unit] = notImplemented

  def s3Client(s3Ref: Ref[IO, List[String]]): DAS3Client[IO] = new DAS3Client[IO]:
    override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] = notImplemented

    override def download(bucket: String, key: String): IO[Publisher[ByteBuffer]] = notImplemented

    override def upload(bucket: String, key: String, publisher: Publisher[ByteBuffer]): IO[CompletedUpload] =
      for {
        uploadString <- publisher
          .toStreamBuffered[IO](1024)
          .map(_.array().map(_.toChar).mkString)
          .compile
          .string
        _ <- s3Ref.update(l => {
          uploadString :: l
        })
      } yield CompletedUpload.builder.response(PutObjectResponse.builder.build).build

    override def headObject(bucket: String, key: String): IO[HeadObjectResponse] = notImplemented

    override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = notImplemented

    override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[SdkPublisher[String]] = notImplemented

  def sqsClient(sqsRef: Ref[IO, List[MessageResponse[SqsMessage]]]): DASQSClient[IO] = new DASQSClient[IO]:
    override def sendMessage[T <: Product](
        queueUrl: String
    )(message: T, potentialFifoConfiguration: Option[DASQSClient.FifoQueueConfiguration], delaySeconds: Int)(using enc: Encoder[T]): IO[SendMessageResponse] = sqsRef
      .update { existingMessages =>
        MessageResponse(UUID.randomUUID.toString, None, message.asInstanceOf[SqsMessage]) :: existingMessages
      }
      .map(_ => SendMessageResponse.builder.build)

    override def receiveMessages[T](queueUrl: String, maxNumberOfMessages: Int)(using dec: Decoder[T]): IO[List[MessageResponse[T]]] = sqsRef.get.map { messages =>
      messages.map(msg => {
        val newMessage = msg.message match
          case sqsInput: SqsInputMessage                              => sqsInput.asInstanceOf[T]
          case notificationsOutputMessage: NotificationsOutputMessage => notificationsOutputMessage.asInstanceOf[T]
        msg.copy(message = newMessage)
      })

    }

    override def deleteMessage(queueUrl: String, receiptHandle: String): IO[DeleteMessageResponse] = sqsRef
      .update { messages =>
        messages.filter(_.receiptHandle == receiptHandle)
      }
      .map(_ => DeleteMessageResponse.builder.build)

  def dynamoClient(dynamoRef: Ref[IO, List[DADynamoDbWriteItemRequest]]): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] =
      dynamoRef.update(items => dynamoDbWriteRequest :: items).map(_ => 1)

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      notImplemented

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = notImplemented

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = notImplemented

  def sfnClient(sfnRef: Ref[IO, List[SFNArguments]]): DASFNClient[IO] = new DASFNClient[IO]:
    override def startExecution[T <: Product](stateMachineArn: String, input: T, name: Option[String])(using enc: Encoder[T]): IO[StartExecutionResponse] =
      sfnRef
        .update { existingArgs =>
          input.asInstanceOf[SFNArguments] :: existingArgs
        }
        .map(_ => StartExecutionResponse.builder.build)

    override def sendTaskSuccess(taskToken: String): IO[Unit] = notImplemented

    override def listStepFunctions(stepFunctionArn: String, status: DASFNClient.Status): IO[List[String]] = notImplemented

  def idRef(ids: List[UUID] = Nil): Ref[IO, List[UUID]] = Ref.unsafe[IO, List[UUID]](ids)

  def runGiven(testString: String): List[String] = {
    for {
      atomicCell <- AtomicCell[IO].empty[List[SqsMessage]]
      s3Ref <- Ref.of[IO, List[String]](Nil)
      stepDefs = StepDefs[IO](atomicCell, s3Client(s3Ref), DASQSClient[IO](), DADynamoDBClient[IO](), DASFNClient[IO](), pollingDuration)
      _ <- new IngestTestsRunner().testGiven(testString, stepDefs)(using idRef())
      s3Results <- s3Ref.get
    } yield s3Results
  }.unsafeRunSync()

  def runWhen(testString: String, ids: List[UUID]): (List[String], List[SqsMessage], List[DADynamoDbWriteItemRequest], List[SFNArguments]) = {
    for {
      atomicCell <- AtomicCell[IO].empty[List[SqsMessage]]
      s3Ref <- Ref.of[IO, List[String]](Nil)
      sqsRef <- Ref.of[IO, List[MessageResponse[SqsMessage]]](Nil)
      dynamoRef <- Ref.of[IO, List[DADynamoDbWriteItemRequest]](Nil)
      sfnRef <- Ref.of[IO, List[SFNArguments]](Nil)
      stepDefs = StepDefs[IO](atomicCell, s3Client(s3Ref), sqsClient(sqsRef), dynamoClient(dynamoRef), sfnClient(sfnRef), pollingDuration)
      _ <- new IngestTestsRunner().testWhen(testString, stepDefs)(using idRef(ids))
      s3Results <- s3Ref.get
      sqsResults <- sqsRef.get
      dynamoResults <- dynamoRef.get
      sfnResults <- sfnRef.get
    } yield (s3Results, sqsResults.map(_.message), dynamoResults, sfnResults)
  }.unsafeRunSync()

  def runThen(testString: String, ids: List[UUID] = Nil, sqsMessages: List[SqsMessage] = Nil, pollingTime: FiniteDuration = pollingDuration): List[SqsMessage] = {
    val messageResponses = sqsMessages.map(message => MessageResponse(UUID.randomUUID.toString, None, message))
    for {
      atomicCell <- AtomicCell[IO].empty[List[SqsMessage]]
      sqsRef <- Ref.of[IO, List[MessageResponse[SqsMessage]]](messageResponses)
      stepDefs = StepDefs[IO](atomicCell, DAS3Client[IO](), sqsClient(sqsRef), DADynamoDBClient[IO](), DASFNClient[IO](), pollingTime)
      _ <- new IngestTestsRunner().testThen(testString, stepDefs)(using idRef(ids))
      sqsResults <- sqsRef.get
    } yield sqsResults.map(_.message)
  }.unsafeRunSync()

  def runProcessEachStep(): (Int, Int, Int, Int, Int, Int) = {
    def stepDefs(
        createFilesCount: Ref[IO, Int],
        sendMessagesCount: Ref[IO, Int],
        createBatchCount: Ref[IO, Int],
        waitForIngestCompleteCount: Ref[IO, Int],
        waitForIngestErrorCount: Ref[IO, Int],
        pollForValidationCount: Ref[IO, Int]
    ): IO[StepDefs[IO]] = IO {
      new StepDefs[IO]:
        override def createFiles(number: Int, emptyChecksum: Boolean, invalidMetadata: Boolean, invalidChecksum: Boolean)(using ref: Ref[IO, List[UUID]]): IO[Unit] =
          createFilesCount.update(_ + 1)

        override def sendMessages(ref: Ref[IO, List[UUID]]): IO[Unit] = sendMessagesCount.update(_ + 1)

        override def createBatch(ref: Ref[IO, List[UUID]]): IO[Unit] = createBatchCount.update(_ + 1)

        override def waitForIngestCompleteMessages(ref: Ref[IO, List[UUID]]): IO[Unit] = waitForIngestCompleteCount.update(_ + 1)

        override def waitForIngestErrorMessages(ref: Ref[IO, List[UUID]]): IO[Unit] = waitForIngestErrorCount.update(_ + 1)

        override def pollForTDRValidationMessages(ref: Ref[IO, List[UUID]]): IO[Unit] = pollForValidationCount.update(_ + 1)
    }

    for {
      uuidRef <- Ref.of[IO, List[UUID]](Nil)
      createFilesCountRef <- Ref.of[IO, Int](0)
      sendMessagesCountRef <- Ref.of[IO, Int](0)
      createBatchCountRef <- Ref.of[IO, Int](0)
      waitForIngestCompleteCountRef <- Ref.of[IO, Int](0)
      waitForIngestErrorCountRef <- Ref.of[IO, Int](0)
      pollForValidationCountRef <- Ref.of[IO, Int](0)
      stepDefs <- stepDefs(createFilesCountRef, sendMessagesCountRef, createBatchCountRef, waitForIngestCompleteCountRef, waitForIngestErrorCountRef, pollForValidationCountRef)
      runner = IngestTestsRunner()
      _ <- IngestTestsRunner().loadAllScenarios().traverse { feature =>
        val eachTestLine = feature.split("\n").toList
        runner.processEachStep(eachTestLine.tail, stepDefs)(using uuidRef)
      }
      createFilesCount <- createFilesCountRef.get
      sendMessagesCount <- sendMessagesCountRef.get
      createBatchCount <- createBatchCountRef.get
      waitForIngestCompleteCount <- waitForIngestCompleteCountRef.get
      waitForIngestErrorCount <- waitForIngestErrorCountRef.get
      pollForValidationCount <- pollForValidationCountRef.get
    } yield (createFilesCount, sendMessagesCount, createBatchCount, waitForIngestCompleteCount, waitForIngestErrorCount, pollForValidationCount)
  }.unsafeRunSync()
