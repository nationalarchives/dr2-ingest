package uk.gov.nationalarchives.preingestrestorepackagebuilder

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import fs2.interop.reactivestreams.*
import org.reactivestreams.Publisher
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import reactor.core.publisher.Flux
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.s3.model.{DeleteObjectsResponse, HeadObjectResponse, ListObjectsV2Response, PutObjectResponse, PutObjectTaggingResponse}
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CompletedUpload}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.IngestLockTableItem
import uk.gov.nationalarchives.preingestrestorepackagebuilder.Lambda.*
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}

import java.nio.ByteBuffer
import java.util.UUID

object TestUtils:
  def dynamoClient(items: List[IngestLockTableItem]): DADynamoDBClient[IO] = new DADynamoDBClient[IO] {
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.stub

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = IO.stub

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.stub

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      IO.pure(items.map(_.asInstanceOf[U]))

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = IO.stub

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = IO.stub
  }

  def s3Client(ref: Ref[IO, Map[String, String]]): DAS3Client[IO] = new DAS3Client[IO] {
    override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] = IO.stub

    override def download(bucket: String, key: String): IO[Publisher[ByteBuffer]] = ref.get.flatMap { m =>
      IO.fromOption(m.get(key))(new Exception(s"Error downloading $key")).map { fileContent =>
        Flux.just(ByteBuffer.wrap(fileContent.getBytes))
      }
    }

    override def upload(bucket: String, key: String, publisher: Publisher[ByteBuffer]): IO[CompletedUpload] = for {
      fileValidationResults <- publisher
        .toStreamBuffered[IO](1024)
        .map(_.array().map(_.toChar).mkString)
        .compile
        .toList
      _ <- ref.update(_ + (key -> fileValidationResults.head))
    } yield CompletedUpload.builder.response(PutObjectResponse.builder.build).build

    override def listObjects(bucket: String, potentialPrefix: Option[String]): IO[ListObjectsV2Response] = IO.stub

    override def headObject(bucket: String, key: String): IO[HeadObjectResponse] = IO.stub

    override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = IO.stub

    override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[SdkPublisher[String]] = IO.stub

    override def updateObjectTags(bucket: String, key: String, newTags: Map[String, String], potentialVersionId: Option[String]): IO[PutObjectTaggingResponse] = IO.stub
  }

  case class TestResult(output: Either[Throwable, Output], s3Map: Map[String, String])

  def runLambda(items: List[IngestLockTableItem], s3Map: Map[String, String]): TestResult = {
    for
      s3Ref <- Ref.of[IO, Map[String, String]](s3Map)
      deps = Dependencies(dynamoClient(items), s3Client(s3Ref), () => UUID.randomUUID)
      res <- Lambda().handler(Input("TEST", "TEST_0", 0), Config("", "", "output-bucket"), Dependencies(dynamoClient(items), s3Client(s3Ref), () => UUID.randomUUID)).attempt
      s3Map <- s3Ref.get
    yield TestResult(res, s3Map)
  }.unsafeRunSync()

  def lockTableItem: IngestLockTableItem =
    val id = UUID.randomUUID
    IngestLockTableItem(id, "TEST", s"""{"id":"$id","location":"s3://bucket/key"}""", "")
