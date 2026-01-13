package uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import io.circe.Encoder
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.reactivestreams.Publisher
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import reactor.core.publisher.Flux
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.s3.model.{DeleteObjectsResponse, HeadObjectResponse, ListObjectsV2Response, PutObjectResponse}
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CompletedUpload}
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbWriteItemRequest
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.FileProcessor.*
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.Lambda.*
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.SeriesMapper.Court
import uk.gov.nationalarchives.utils.ExternalUtils.{Parser, Payload, TDRParams, TREMetadata, TREMetadataParameters, TREParams}
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client, DASFNClient}

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.time.{Instant, OffsetDateTime}
import java.util.UUID
import scala.jdk.CollectionConverters.*

object TestUtils:
  case class S3Object(bucket: String, key: String, content: ByteBuffer)

  extension (errors: Option[Errors]) def raise(fn: Errors => Boolean, errorMessage: String): IO[Unit] = IO.raiseWhen(errors.exists(fn))(new Exception(errorMessage))

  def notImplemented[T]: IO[T] = IO.raiseError(new Exception("Not implemented"))

  case class Errors(download: Boolean = false, upload: Boolean = false, write: Boolean = false)

  case class SFNExecutions(sfnArn: String, input: Output)

  def dynamoClient(dynamoRef: Ref[IO, List[DADynamoDbWriteItemRequest]], errors: Option[Errors]): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = errors.raise(_.write, "Error writing to Dynamo") >>
      dynamoRef.update(items => dynamoDbWriteRequest :: items).map(_ => 1)

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      notImplemented

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = notImplemented

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = notImplemented

  def sfnClient(sfnRef: Ref[IO, List[SFNExecutions]]): DASFNClient[IO] = new DASFNClient[IO]:
    override def listStepFunctions(stepFunctionArn: String, status: DASFNClient.Status): IO[List[String]] = notImplemented

    override def sendTaskSuccess[T: Encoder](taskToken: String, potentialOutput: Option[T]): IO[Unit] = notImplemented

    override def startExecution[T <: Product](stateMachineArn: String, input: T, name: Option[String])(using enc: Encoder[T]): IO[StartExecutionResponse] =
      sfnRef
        .update { existingArgs =>
          SFNExecutions(stateMachineArn, input.asInstanceOf[Output]) :: existingArgs
        }
        .map(_ => StartExecutionResponse.builder.build)

  def s3Client(ref: Ref[IO, List[S3Object]], errors: Option[Errors] = None): DAS3Client[IO] = new DAS3Client[IO]:

    override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] = notImplemented

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

    override def headObject(bucket: String, key: String): IO[HeadObjectResponse] = notImplemented

    override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = ref
      .update { existing =>
        existing.filterNot(obj => obj.bucket == bucket && keys.contains(obj.key))
      }
      .map(_ => DeleteObjectsResponse.builder.build)

    override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[SdkPublisher[String]] = notImplemented

    override def listObjects(bucket: String, prefix: Option[String]): IO[ListObjectsV2Response] = notImplemented

  val reference = "TEST-REFERENCE"

  val config: Config = Config("bucket", "sfnArn", "lockTable")

  val testOutputBucket = "outputBucket"
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
    "e59fa46d-2c07-42dc-9d46-98af0fd38217",
    "1217a675-092d-473c-95bd-f93d0a970322",
    "fd1557f5-8f98-4152-a2a8-726c4a484447"
  ).map(UUID.fromString)

  def runLambda(
      initialS3State: List[S3Object],
      event: SQSEvent,
      errors: Option[Errors] = None,
      instantGenerator: () => Instant = () => Instant.now()
  ): (Either[Throwable, Unit], List[S3Object], List[DADynamoDbWriteItemRequest], List[SFNExecutions]) =
    val uuidIterator = uuids.iterator
    (for {
      s3Ref <- Ref.of[IO, List[S3Object]](initialS3State)
      dynamoRef <- Ref.of[IO, List[DADynamoDbWriteItemRequest]](Nil)
      sfnRef <- Ref.of[IO, List[SFNExecutions]](Nil)
      seriesMapper = new SeriesMapper(Set(Court("COURT", "TEST", "TEST SERIES")))
      dependencies = Dependencies(s3Client(s3Ref, errors), sfnClient(sfnRef), dynamoClient(dynamoRef, errors), () => uuidIterator.next(), seriesMapper, instantGenerator)
      res <- new Lambda().handler(event, config, dependencies).attempt
      s3FinalState <- s3Ref.get
      dynamoFinalState <- dynamoRef.get
      sfnFinalState <- sfnRef.get
    } yield (res, s3FinalState, dynamoFinalState, sfnFinalState)).unsafeRunSync()

  def packageAvailable(s3Key: String): TREInput = TREInput(
    TREInputParameters("status", "TEST-REFERENCE", skipSeriesLookup = false, inputBucket, s3Key),
    None
  )

  def event(s3Key: String = "test.tar.gz", body: Option[String] = None): SQSEvent = {
    val sqsEvent = new SQSEvent()
    val record = new SQSMessage()
    record.setBody(body.getOrElse(packageAvailable(s3Key).asJson.noSpaces))
    sqsEvent.setRecords(List(record).asJava)
    sqsEvent
  }

  def fileBytes(metadata: TREMetadata, fileSize: Int = 100): ByteBuffer = fileBytes(metadata.asJson.noSpaces, fileSize)

  def fileBytes(content: String, fileSize: Int): ByteBuffer = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val gzipOut = new GzipCompressorOutputStream(byteArrayOutputStream)
    val tarOut = new TarArchiveOutputStream(gzipOut)
    val directoryEntry = new TarArchiveEntry(s"$reference/")
    tarOut.putArchiveEntry(directoryEntry)
    val files = Map("Test.docx" -> Array.fill(fileSize)("a").mkString, "unused.txt" -> "", s"TRE-$reference-metadata.json" -> content)
    files.foreach { (fileName, content) =>
      val entry = new TarArchiveEntry(s"$reference/$fileName")
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

end TestUtils
