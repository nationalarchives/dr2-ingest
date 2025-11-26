package uk.gov.nationalarchives.preingestpaimporter

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import fs2.interop.reactivestreams.*
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.{Decoder, Encoder}
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.s3.model.*
import software.amazon.awssdk.services.sqs.model.{DeleteMessageResponse, GetQueueAttributesResponse, QueueAttributeName, SendMessageResponse}
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CompletedUpload}
import uk.gov.nationalarchives.preingestpaimporter.Lambda.{*, given}
import uk.gov.nationalarchives.{DAS3Client, DASQSClient}

import java.nio.ByteBuffer
import java.util.UUID
import scala.jdk.CollectionConverters.*

object TestUtils {

  class TestS3Client extends DAS3Client[IO] {
    override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] = IO.stub

    override def download(bucket: String, key: String): IO[Publisher[ByteBuffer]] = IO.stub

    override def upload(bucket: String, key: String, publisher: Publisher[ByteBuffer]): IO[CompletedUpload] = IO.stub

    override def listObjects(bucket: String, potentialPrefix: Option[String]): IO[ListObjectsV2Response] = IO.stub

    override def headObject(bucket: String, key: String): IO[HeadObjectResponse] = IO.stub

    override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = IO.stub

    override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[SdkPublisher[String]] = IO.stub
  }

  case class TestCopy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String)

  case class Output(handlerResponse: Either[Throwable, List[Unit]], metadataMap: Map[String, String], copy: List[TestCopy], messages: List[Message])

  case class Errors(download: Boolean = false, upload: Boolean = false, copy: Boolean = false, sendMessage: Boolean = false)

  def error(isError: Boolean, message: String): IO[Unit] =
    IO.raiseWhen(isError)(new Exception(message))

  def testDR2S3Client(ref: Ref[IO, Map[String, String]], potentialErrors: Option[Errors]): TestS3Client = new TestS3Client:

    override def upload(bucket: String, key: String, publisher: Publisher[ByteBuffer]): IO[CompletedUpload] =
      for
        _ <- error(potentialErrors.exists(_.upload), "Upload failed")
        jsonString <- publisher.toStreamBuffered[IO](1024).map(_.array().map(_.toChar).mkString).compile.string
        json <- IO.fromEither(decode[List[Data]](jsonString))
        _ <- ref.update(currentMap => currentMap + (key -> json.asJson.noSpaces))
      yield CompletedUpload.builder.response(PutObjectResponse.builder.build).build

  def testExternalS3Client(ref: Ref[IO, List[TestCopy]], metadata: Map[String, String], potentialErrors: Option[Errors]): TestS3Client = new TestS3Client:
    override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] =
      error(potentialErrors.exists(_.copy), "Copy failed") >> ref
        .update(current => TestCopy(sourceBucket, sourceKey, destinationBucket, destinationKey) :: current)
        .map(_ => CompletedCopy.builder.response(CopyObjectResponse.builder.build).build)

    override def download(bucket: String, key: String): IO[Publisher[ByteBuffer]] = {
      error(potentialErrors.exists(_.download), "Download failed") >>
        IO.pure(Flux.just(ByteBuffer.wrap(metadata(key).getBytes)))
    }

  def testSqsClient(ref: Ref[IO, List[Message]], potentialErrors: Option[Errors]): DASQSClient[IO] = new DASQSClient[IO] {
    override def sendMessage[T <: Product](queueUrl: String)(message: T, potentialFifoConfiguration: Option[DASQSClient.FifoQueueConfiguration], delaySeconds: Int)(using
        enc: Encoder[T]
    ): IO[SendMessageResponse] =
      error(potentialErrors.exists(_.sendMessage), "Send message failed") >> ref
        .update(current => message.asInstanceOf[Message] :: current)
        .map(_ => SendMessageResponse.builder.build)

    override def receiveMessages[T](queueUrl: String, maxNumberOfMessages: Int)(using dec: Decoder[T]): IO[List[DASQSClient.MessageResponse[T]]] = IO.stub

    override def deleteMessage(queueUrl: String, receiptHandle: String): IO[DeleteMessageResponse] = IO.stub

    override def getQueueAttributes(queueUrl: String, attributeNames: List[QueueAttributeName]): IO[GetQueueAttributesResponse] = IO.stub
  }

  def runLambda(metadata: String, potentialErrors: Option[Errors] = None): Output = {
    val sqsEvent = new SQSEvent()
    val sqsMessage = new SQSMessage()
    val s3Key = UUID.randomUUID
    sqsMessage.setBody(s"""{"metadataLocation":"s3://bucket/$s3Key"}""")
    sqsEvent.setRecords(List(sqsMessage).asJava)
    val config = Config("outputBucketName", "outputQueueUrl", "roleToAssume", "filesBucket")
    val keyToMetadata = Map(s3Key.toString -> metadata)
    for
      metadataRef <- Ref.of[IO, Map[String, String]](keyToMetadata)
      copyRef <- Ref.of[IO, List[TestCopy]](Nil)
      messageRef <- Ref.of[IO, List[Message]](Nil)
      dependencies = Dependencies(
        testExternalS3Client(copyRef, keyToMetadata, potentialErrors),
        testDR2S3Client(metadataRef, potentialErrors),
        testSqsClient(messageRef, potentialErrors)
      )
      handlerResponse <- new Lambda().handler(sqsEvent, config, dependencies).attempt
      metadataMap <- metadataRef.get
      copy <- copyRef.get
      message <- messageRef.get
    yield Output(handlerResponse, metadataMap, copy, message)
  }.unsafeRunSync()

}
