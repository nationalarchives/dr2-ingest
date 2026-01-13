package uk.gov.nationalarchives.preingestcourtdocpackagebuilder

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import org.reactivestreams.Publisher
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import reactor.core.publisher.Flux
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.s3.model.{DeleteObjectsResponse, HeadObjectResponse, ListObjectsV2Response, PutObjectResponse}
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CompletedUpload}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.IngestLockTableItem
import uk.gov.nationalarchives.utils.ExternalUtils.{MetadataObject, StepFunctionInput}
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import Lambda.*

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.*

object TestUtils:

  case class S3Object(bucket: String, key: String, content: ByteBuffer)

  def s3Client(existingContents: List[S3Object], ref: Ref[IO, List[S3Object]]): DAS3Client[IO] = new DAS3Client[IO] {
    override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] = IO.stub

    override def download(bucket: String, key: String): IO[Publisher[ByteBuffer]] =
      for s3Object <- IO.fromOption(existingContents.find(obj => obj.key == key && obj.bucket == bucket))(new Exception("Object not found"))
      yield Flux.just(s3Object.content)

    override def upload(bucket: String, key: String, publisher: Publisher[ByteBuffer]): IO[CompletedUpload] = ref
      .update { existing =>
        val content = Flux
          .from(publisher)
          .toIterable
          .asScala
          .toList
          .flatMap(_.array())
          .toArray

        S3Object(bucket, key, ByteBuffer.wrap(content)) :: existing
      }
      .map(_ => CompletedUpload.builder.response(PutObjectResponse.builder.build).build)

    override def listObjects(bucket: String, potentialPrefix: Option[String]): IO[ListObjectsV2Response] = IO.stub

    override def headObject(bucket: String, key: String): IO[HeadObjectResponse] =
      IO.fromOption(existingContents.find(obj => obj.key == key && obj.bucket == bucket))(new Exception("Object not found")).map { obj =>
        HeadObjectResponse.builder.contentLength(obj.content.array().length.toLong).build
      }

    override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = IO.stub

    override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[SdkPublisher[String]] = IO.stub
  }

  def dynamoClient(items: List[IngestLockTableItem]): DADynamoDBClient[IO] = new DADynamoDBClient[IO] {
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.stub

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = IO.stub

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.stub

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      IO.pure {
        items.map(_.asInstanceOf[U])
      }

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = IO.stub

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = IO.stub
  }

  case class LambdaOutput(res: Either[Throwable, StepFunctionInput], builderArgs: List[IngestLockTableItem], uploads: List[S3Object])

  def runLambda(ingestMetadata: Map[IngestLockTableItem, List[MetadataObject]], items: List[IngestLockTableItem]): LambdaOutput = {
    val input = Input("groupId", "batchId", 0)
    val config = Config("", "", "bucket")

    def metadataBuilder(ref: Ref[IO, List[IngestLockTableItem]]) = new MetadataBuilder {
      override def createMetadata(item: IngestLockTableItem): IO[List[MetadataObject]] = ref
        .update(existing => item :: existing)
        .map(_ => ingestMetadata.getOrElse(item, Nil))
    }

    for
      builderArgRef <- Ref.of[IO, List[IngestLockTableItem]](Nil)
      uploadRef <- Ref.of[IO, List[S3Object]](Nil)
      res <- new Lambda().handler(input, config, Dependencies(dynamoClient(items), s3Client(Nil, uploadRef), metadataBuilder(builderArgRef))).attempt
      builderArgs <- builderArgRef.get
      uploads <- uploadRef.get
    yield LambdaOutput(res, builderArgs, uploads)
  }.unsafeRunSync()
