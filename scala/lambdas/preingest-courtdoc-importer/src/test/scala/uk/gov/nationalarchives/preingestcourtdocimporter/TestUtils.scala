package uk.gov.nationalarchives.preingestcourtdocimporter

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import io.circe.{Decoder, Encoder}
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.s3.model.{DeleteObjectsResponse, HeadObjectResponse, ListObjectsV2Response, PutObjectResponse}
import software.amazon.awssdk.services.sqs.model.{DeleteMessageResponse, GetQueueAttributesResponse, QueueAttributeName, SendMessageResponse}
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CompletedUpload}
import uk.gov.nationalarchives.{DAS3Client, DASQSClient}
import uk.gov.nationalarchives.preingestcourtdocimporter.Lambda.*
import uk.gov.nationalarchives.utils.ExternalUtils.*

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.UUID
import scala.jdk.CollectionConverters.*

object TestUtils:
  case class S3Object(bucket: String, key: String, content: ByteBuffer)

  extension (errors: Option[Errors]) def raise(fn: Errors => Boolean, errorMessage: String): IO[Unit] = IO.raiseWhen(errors.exists(fn))(new Exception(errorMessage))

  case class Errors(download: Boolean = false, upload: Boolean = false, sendMessage: Boolean = false)

  def sqsClient(ref: Ref[IO, List[Message]], errors: Option[Errors] = None): DASQSClient[IO] = new DASQSClient[IO] {
    override def sendMessage[T <: Product](queueUrl: String)(message: T, potentialFifoConfiguration: Option[DASQSClient.FifoQueueConfiguration], delaySeconds: Int)(using
        enc: Encoder[T]
    ): IO[SendMessageResponse] = errors.raise(_.sendMessage, "Error sending messages") >>
      ref
        .update { existing =>
          message.asInstanceOf[Message] :: existing
        }
        .map(_ => SendMessageResponse.builder.build)

    override def receiveMessages[T](queueUrl: String, maxNumberOfMessages: Int)(using dec: Decoder[T]): IO[List[DASQSClient.MessageResponse[T]]] = IO.never

    override def deleteMessage(queueUrl: String, receiptHandle: String): IO[DeleteMessageResponse] = IO.never

    override def getQueueAttributes(queueUrl: String, attributeNames: List[QueueAttributeName]): IO[GetQueueAttributesResponse] = IO.never
  }

  def s3Client(ref: Ref[IO, List[S3Object]], errors: Option[Errors] = None): DAS3Client[IO] = new DAS3Client[IO]:

    override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] = IO.never

    override def download(bucket: String, key: String): IO[Publisher[ByteBuffer]] = errors.raise(_.download, "Error downloading files") >>
      (for {
        existing <- ref.get
        s3Object <- IO.fromOption(existing.find(obj => obj.key == key && obj.bucket == bucket))(new Exception("Object not found"))
      } yield Flux.just(s3Object.content))

    override def upload(bucket: String, key: String, publisher: Publisher[ByteBuffer]): IO[CompletedUpload] = errors.raise(_.upload, "Upload failed") >> ref
      .update { existing =>
        val content = Flux
          .from(publisher)
          .toIterable
          .asScala
          .toList
          .head
        S3Object(bucket, key, content) :: existing
      }
      .map(_ => CompletedUpload.builder.response(PutObjectResponse.builder.build).build)

    override def headObject(bucket: String, key: String): IO[HeadObjectResponse] = IO.never

    override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = ref
      .update { existing =>
        existing.filterNot(obj => obj.bucket == bucket && keys.contains(obj.key))
      }
      .map(_ => DeleteObjectsResponse.builder.build)

    override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[SdkPublisher[String]] = IO.never

    override def listObjects(bucket: String, prefix: Option[String]): IO[ListObjectsV2Response] = IO.never

  val reference = "TEST-REFERENCE"

  val config: Config = Config("bucket", "queueUrl")

  val inputBucket = "inputBucket"

  def inputMetadata(tdrUuid: UUID = UUID.randomUUID(), potentialCite: Option[String] = None, suffix: String = "2023/abc"): TREMetadata = TREMetadata(
    TREMetadataParameters(
      Parser(s"https://example.com/id/court/$suffix".some, potentialCite, "test".some, Nil, Nil),
      TREParams(reference, Payload("Test.docx")),
      TDRParams("checksum", "Source", "identifier", OffsetDateTime.parse("2024-11-07T15:29:54Z"), None, tdrUuid)
    )
  )

  def uuids: List[UUID] = List(
    "cee5851e-813f-4a9d-ae9c-577f9eb601e0",
    "fd03992b-7e10-4454-8381-0be4e6c0c1b5",
    "e59fa46d-2c07-42dc-9d46-98af0fd38217"
  ).map(UUID.fromString)

  def runLambda(
      initialS3State: List[S3Object],
      event: SQSEvent,
      errors: Option[Errors] = None
  ): (Either[Throwable, Unit], List[S3Object], List[Message]) =
    val uuidIterator = uuids.iterator
    (for
      s3Ref <- Ref.of[IO, List[S3Object]](initialS3State)
      sqsRef <- Ref.of[IO, List[Message]](Nil)
      dependencies = Dependencies(s3Client(s3Ref, errors), sqsClient(sqsRef, errors), () => uuidIterator.next())
      res <- new Lambda().handler(event, config, dependencies).attempt
      s3FinalState <- s3Ref.get
      sqsFinalState <- sqsRef.get
    yield (res, s3FinalState, sqsFinalState)).unsafeRunSync()

  def packageAvailable(s3Key: String, messageId: Option[String]): TREInput = TREInput(
    TREInputParameters("status", "TEST-REFERENCE", skipSeriesLookup = false, inputBucket, s3Key),
    Option(TREInputProperties(messageId))
  )

  def event(messageId: Option[String] = None): SQSEvent = {
    val sqsEvent = new SQSEvent()
    val record = new SQSMessage()
    record.setBody(packageAvailable("test.tar.gz", messageId).asJson.noSpaces)
    sqsEvent.setRecords(List(record).asJava)
    sqsEvent
  }

  def fileBytes(metadata: TREMetadata, batchReference: String): ByteBuffer = fileBytes(metadata.asJson.noSpaces, batchReference)

  def fileBytes(content: String, batchReference: String): ByteBuffer = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val gzipOut = new GzipCompressorOutputStream(byteArrayOutputStream)
    val tarOut = new TarArchiveOutputStream(gzipOut)
    val directoryEntry = new TarArchiveEntry(s"$batchReference/")
    tarOut.putArchiveEntry(directoryEntry)
    val files = Map("Test.docx" -> Array.fill(100)("a").mkString, "unused.txt" -> "", s"TRE-$batchReference-metadata.json" -> content)
    files.foreach { (fileName, content) =>
      val entry = new TarArchiveEntry(s"$batchReference/$fileName")
      entry.setSize(content.length)
      tarOut.putArchiveEntry(entry)
      tarOut.write(content.getBytes)
      tarOut.closeArchiveEntry()
    }
    tarOut.close()
    gzipOut.close()
    byteArrayOutputStream.close()
    ByteBuffer.wrap(byteArrayOutputStream.toByteArray)
  }
