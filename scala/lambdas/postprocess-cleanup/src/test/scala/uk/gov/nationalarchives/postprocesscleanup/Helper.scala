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
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{ArchiveFolderDynamoItem, AssetDynamoItem, Checksum, ContentFolderDynamoItem, DynamoItem, FileDynamoItem}
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import uk.gov.nationalarchives.postprocesscleanup.Lambda.*

import java.nio.ByteBuffer
import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.UUID

object Helper {

  def toDate(ttl: Long): LocalDate =
    Instant
      .ofEpochSecond(ttl)
      .atZone(ZoneOffset.UTC)
      .toLocalDate

  case class InitialData(dynamoItems: List[DynamoItem], s3Objects: Map[String, Map[String, String]])

  def createDynamoItem(id: UUID, name: String, location: String, parentPath: String, dynamoItemType: DynamoFormatters.Type): DynamoFormatters.DynamoItem = 
    dynamoItemType match {
      case DynamoFormatters.Type.Asset =>
        AssetDynamoItem(
          batchId = "some_batchId",
          id = id,
          potentialParentPath = Some(parentPath),
          `type` = DynamoFormatters.Type.Asset,
          potentialTitle = None,
          potentialDescription = None,
          transferringBody = None,
          transferCompleteDatetime = None,
          upstreamSystem = "some_upstream_system",
          digitalAssetSource = "some_digital_asset_source",
          potentialDigitalAssetSubtype = None,
          originalMetadataFiles = Nil,
          identifiers = Nil,
          childCount = 0,
          skipIngest = false,
          correlationId = None,
          filePath = "some_file_path",
          ttl = 1779382126L
        )
      case DynamoFormatters.Type.ArchiveFolder =>
        ArchiveFolderDynamoItem(
          batchId = "some_batchId",
          id = id,
          potentialParentPath = Some(parentPath),
          name = name,
          `type` = DynamoFormatters.Type.ArchiveFolder,
          potentialTitle = None,
          potentialDescription = None,
          identifiers = Nil,
          childCount = 1,
          ttl = 1779382126L
        )
      case DynamoFormatters.Type.ContentFolder =>
        ArchiveFolderDynamoItem(
          batchId = "some_batchId",
          id = id,
          potentialParentPath = None,
          name = name,
          `type` = DynamoFormatters.Type.ContentFolder,
          potentialTitle = None,
          potentialDescription = None,
          identifiers = Nil,
          childCount = 1,
          ttl = 1779382126L
        )
      case DynamoFormatters.Type.File =>
        FileDynamoItem(
          batchId = "some_batchId",
          id = id,
          potentialParentPath = Some(parentPath),
          name = name,
          `type` = DynamoFormatters.Type.File,
          potentialTitle = None,
          potentialDescription = None,
          sortOrder = 1,
          fileSize = 46L,
          checksums = List(Checksum("some_algorithm", "some_fingerprint")),
          potentialFileExtension = Some("txt"),
          representationType = DynamoFormatters.FileRepresentationType.PreservationRepresentationType,
          representationSuffix = 1,
          identifiers = Nil,
          childCount = 0,
          location = new java.net.URI(location),
          ttl = 1779382126L
        )
    }
  
  //   The initial data contains a hierarchy of 6 items in total, with the following structure:
  //   * 1 asset with a parent path of pattern "ContentFolder/Grandparent/Parent"
  //   * its 2 children, each has a valid location and corresponding object in s3
  //   * 3 ancestors each with correct parent path hierarchy and no location
  def createInitialData: InitialData =
    val assetId = "d5c74859-b4fa-403e-a11c-0c7652265f03"
    val contentFolderId = "d1ad2270-1711-47db-b663-c530bc518e87"
    val parentId = "d086e29a-83ed-4129-b20c-8e2041bac4f7"
    val grandparentId = "9385ad5c-e205-40fd-8cb2-c157d1331167"
    val file1Id = "a5788834-3b45-491e-91d8-fd008351a3ad"
    val file2Id = "d665011c-f6b6-4bf9-9df1-9218b7429cd5"
    val parentArchiveFolderPath = s"$contentFolderId/$grandparentId"
    val assetItem = createDynamoItem(
      UUID.fromString(assetId),
      assetId,
      "",
      s"$parentArchiveFolderPath/$parentId",
      DynamoFormatters.Type.Asset
    )
    val childOne = createDynamoItem(
      UUID.fromString(file1Id),
      "child-one.json",
      s"s3://some-bucket/$file1Id",
      s"$parentArchiveFolderPath/$parentId/$assetId",
      DynamoFormatters.Type.File
    )
    val childTwo = createDynamoItem(
      UUID.fromString(file2Id),
      "child-two.json",
      s"s3://some-bucket/$file2Id",
      s"$parentArchiveFolderPath/$parentId/$assetId",
      DynamoFormatters.Type.File
    )
    val ancestor1 = createDynamoItem(
      UUID.fromString(parentId),
      "parent",
      "",
      parentArchiveFolderPath,
      DynamoFormatters.Type.ArchiveFolder
    )
    val ancestor2 = createDynamoItem(UUID.fromString(grandparentId), "grandparent", "", contentFolderId, DynamoFormatters.Type.ArchiveFolder)
    val ancestor3 = createDynamoItem(UUID.fromString(contentFolderId), "content", "", "", DynamoFormatters.Type.ContentFolder)
    InitialData(
      List(assetItem, childOne, childTwo, ancestor1, ancestor2, ancestor3),
      Map(
        file1Id -> Map("TAG_ONE" -> "ONE", "TAG_TWO" -> "TWO"),
        file2Id -> Map.empty
      )
    )


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
