package uk.gov.nationalarchives.postprocesscleanup

import cats.effect.*
import cats.effect.unsafe.implicits.global
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import org.reactivestreams.Publisher
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.s3.model.{DeleteObjectsResponse, HeadObjectResponse, ListObjectsV2Response, PutObjectTaggingResponse}
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CompletedUpload}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{ArchiveFolderDynamoItem, AssetDynamoItem, ContentFolderDynamoItem, DynamoItem, FileDynamoItem}
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import uk.gov.nationalarchives.postprocesscleanup.Lambda.*

import java.nio.ByteBuffer

object Helper {
  def notImplemented[T]: IO[Nothing] = IO.raiseError(new Exception("Not implemented"))
  val config: Config = Config("files-table", "dynamo-gsi", "raw-cache-bucket")

  def runLambda(
      sqsEvent: SQSEvent,
      initialItemsInTable: List[DynamoItem],
      initialS3Objects: Map[String, Map[String, String]] = Map.empty
  ): LambdaRunResults = {

    for {
      dynamoRef <- Ref.of[IO, List[DynamoItem]](initialItemsInTable)
      s3Ref <- Ref.of[IO, Map[String, Map[String, String]]](initialS3Objects)
      dependencies = Dependencies(dynamoClient(dynamoRef), s3Client(s3Ref))
      result <- new Lambda().handler(sqsEvent, config, dependencies).attempt
      dynamoResult <- dynamoRef.get
      s3Result <- s3Ref.get
    } yield LambdaRunResults(result, dynamoResult, s3Result)
  }.unsafeRunSync()

  case class LambdaRunResults(result: Either[Throwable, Unit], finalItemsInTable: List[DynamoItem], finalsObjectsInS3: Map[String, Map[String, String]])

  def dynamoClient(ref: Ref[IO, List[DynamoItem]]): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    private def normaliseConditions(conditions: Map[String, String]): Map[String, String] =
      if conditions.keys.exists(_.startsWith("conditionAttributeValue")) then
        conditions.map { case (key, value) =>
          if key.startsWith("conditionAttributeValue") then "id" -> value
          else key -> value
        }
      else conditions

    private def satisfiesCondition(item: DynamoItem, conditions: Map[String, String]): Boolean =
      conditions.forall {
        case ("id", value)         => item.id.toString.equals(value)
        case ("batchId", value)    => item.batchId.equals(value)
        case ("parentPath", value) => item.potentialParentPath.contains(value)
        case _                     => false
      }

    override def queryItems[U](
        tableName: String,
        requestCondition: RequestCondition,
        potentialGsiName: Option[String]
    )(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =

      ref.get.map: existingItems =>
        (for
          values <- Option(requestCondition.attributes.values)
          queryConditionsMap <- values.toMap[String].toOption
          conditionsMap = normaliseConditions(queryConditionsMap)
        yield existingItems
          .filter(eachItem => satisfiesCondition(eachItem, conditionsMap))
          .map(_.asInstanceOf[U])).getOrElse(Nil)

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = {
      val newTtl = dynamoDbRequest.attributeNamesAndValuesToUpdate.get("ttl").map(_.n()).get
      val assetIdToUpdate = dynamoDbRequest.primaryKeyAndItsValue.get("id").map(_.s()).get
      ref
        .update: existingItems =>
          existingItems.map: item =>
            if item.id.toString.equals(assetIdToUpdate) then { 
              item match {
                case file: FileDynamoItem => file.copy(ttl = newTtl.toLong)
                case asset: AssetDynamoItem => asset.copy(ttl = newTtl.toLong)
                case archiveFolder: ArchiveFolderDynamoItem => archiveFolder.copy(ttl = newTtl.toLong)
                case contentFolder: ContentFolderDynamoItem => contentFolder.copy(ttl = newTtl.toLong)
              }
            } else item
        .map(_ => 1)
    }

    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = notImplemented

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = notImplemented

  def s3Client(ref: Ref[IO, Map[String, Map[String, String]]]): DAS3Client[IO] = new DAS3Client[IO]:
    override def updateObjectTags(
        bucket: String,
        key: String,
        newTags: Map[String, String],
        potentialVersionId: Option[String]
    ): IO[PutObjectTaggingResponse] =
      ref
        .update { existing =>
          val mergedTags = existing.getOrElse(key, Map.empty) ++ newTags
          existing.updated(key, mergedTags)
        }
        .as(PutObjectTaggingResponse.builder.build)

    override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] = notImplemented

    override def download(bucket: String, key: String): IO[Publisher[ByteBuffer]] = notImplemented

    override def upload(bucket: String, key: String, publisher: Publisher[ByteBuffer]): IO[CompletedUpload] = notImplemented

    override def listObjects(bucket: String, potentialPrefix: Option[String]): IO[ListObjectsV2Response] = notImplemented

    override def headObject(bucket: String, key: String): IO[HeadObjectResponse] = notImplemented

    override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = notImplemented

    override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[SdkPublisher[String]] = notImplemented
}
