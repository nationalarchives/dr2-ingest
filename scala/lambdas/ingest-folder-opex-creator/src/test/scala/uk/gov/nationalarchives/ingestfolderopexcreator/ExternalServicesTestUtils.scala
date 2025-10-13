package uk.gov.nationalarchives.ingestfolderopexcreator

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import fs2.interop.reactivestreams.*
import org.reactivestreams.Publisher
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.model.{BatchWriteItemResponse, ResourceNotFoundException}
import software.amazon.awssdk.services.s3.model.{DeleteObjectsResponse, HeadObjectResponse, ListObjectsV2Response, PutObjectResponse}
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CompletedUpload}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.ingestfolderopexcreator.Lambda.{AssetItem, Config, Dependencies, Input}
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}

import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.xml.Elem

object ExternalServicesTestUtils:

  val batchId: String = "TEST-ID"

  val ingestDateTime: OffsetDateTime = OffsetDateTime.now

  case class FolderWithChildren(folder: ArchiveFolderDynamoItem, children: List[AssetDynamoItem])
  case class S3Object(bucket: String, key: String, content: String)

  def generateInput(assetId: UUID): Input = Input(assetId, batchId, batchId)

  val config: Config = Config("", "sourceBucket", "destinationBucket", "roleArn")

  def resourceNotFound(dynamoError: Boolean): IO[Unit] = IO.raiseWhen(dynamoError)(ResourceNotFoundException.builder.message("Error getting items from Dynamo").build)
  def itemsNotFound(dynamoError: Boolean): IO[Unit] = IO.raiseWhen(dynamoError)(ResourceNotFoundException.builder.message("Error querying Dynamo").build)
  def s3HeadObjectError(s3Error: Boolean): IO[Unit] = IO.raiseWhen(s3Error)(new Exception("Error calling head object"))
  def s3UploadError(s3Error: Boolean): IO[Unit] = IO.raiseWhen(s3Error)(new Exception("Error uploading s3 objects"))

  def notImplemented[T]: IO[T] = IO.raiseError(new Exception("Not implemented"))

  def dynamoClient(folders: List[FolderWithChildren], errors: Option[Errors]): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = notImplemented

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      itemsNotFound(errors.exists(_.dynamoQueryError)) >> IO {
        (for {
          dynamoValues <- Option(requestCondition.attributes.values)
          value <- dynamoValues.toExpressionAttributeValues
          parentPath <- value.asScala.get(":parentPath")
          batchId <- value.asScala.get(":batchId")
        } yield {
          folders
            .filter(each => each.folder.id.toString == parentPath.s() && each.folder.batchId == batchId.s())
            .flatMap(_.children)
            .map(child =>
              AssetItem(child.batchId, child.id, child.potentialParentPath, child.`type`, child.potentialTitle, child.potentialDescription, child.identifiers, child.skipIngest)
            )
            .map(_.asInstanceOf[U])
        }).getOrElse(Nil)
      }

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] =
      val key = primaryKeys.head.asInstanceOf[FilesTablePrimaryKey]
      resourceNotFound(errors.exists(_.dynamoGetError)) >> IO {
        folders.map(_.folder).filter(_.id == key.partitionKey.id).map(_.asInstanceOf[T])
      }

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = notImplemented

  def s3Client(ref: Ref[IO, List[S3Object]], errors: Option[Errors]): DAS3Client[IO] = new DAS3Client[IO]:
    override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] = notImplemented

    override def download(bucket: String, key: String): IO[Publisher[ByteBuffer]] = notImplemented

    override def upload(bucket: String, key: String, publisher: Publisher[ByteBuffer]): IO[CompletedUpload] =
      s3UploadError(errors.exists(_.s3UploadError)) >>
        (for {
          content <- publisher
            .toStreamBuffered[IO](1024)
            .map(_.array().map(_.toChar).mkString)
            .compile
            .string
          _ <- ref.update(existing => S3Object(bucket, key, content) :: existing)
        } yield CompletedUpload.builder.response(PutObjectResponse.builder.build).build)

    override def headObject(bucket: String, key: String): IO[HeadObjectResponse] =
      s3HeadObjectError(errors.exists(_.s3HeadObjectError)) >>
        (for {
          existing <- ref.get
          s3Object <- IO.fromOption(existing.find(s3Object => s3Object.bucket == bucket && s3Object.key == key))(new Exception("Object not found"))
        } yield HeadObjectResponse.builder.contentLength(s3Object.content.length.toLong).build)

    override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = notImplemented

    override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[SdkPublisher[String]] = notImplemented

    override def listObjects(bucket: String, prefix: Option[String]): IO[ListObjectsV2Response] = notImplemented

  case class Errors(dynamoGetError: Boolean = false, dynamoQueryError: Boolean = false, s3HeadObjectError: Boolean = false, s3UploadError: Boolean = false)

  def runLambda(input: Input, dynamoItems: List[FolderWithChildren], s3Objects: List[S3Object], errors: Option[Errors] = None): (Either[Throwable, Unit], List[S3Object]) = (for {
    s3Ref <- Ref.of[IO, List[S3Object]](s3Objects)
    dependencies = Dependencies(dynamoClient(dynamoItems, errors), s3Client(s3Ref, errors), XMLCreator())
    result <- new Lambda().handler(input, config, dependencies).attempt
    s3Objects <- s3Ref.get
  } yield (result, s3Objects)).unsafeRunSync()

  def generateAsset: AssetDynamoItem = AssetDynamoItem(
    batchId,
    UUID.randomUUID,
    Option("a/parent/path"),
    Asset,
    None,
    None,
    Option("Body"),
    Option(OffsetDateTime.parse("2023-06-01T00:00Z")),
    "upstreamSystem",
    "digitalAssetSource",
    None,
    Nil,
    Nil,
    true,
    true,
    List(Identifier("UpstreamSystemReference", "upstreamSystemReference")),
    1,
    false,
    None,
    "/a/file/path"
  )

  def generateFolder: ArchiveFolderDynamoItem = ArchiveFolderDynamoItem(
    batchId,
    UUID.randomUUID,
    None,
    "Test Name",
    ArchiveFolder,
    None,
    None,
    List(Identifier("Code", "Code")),
    1
  )

  def generateExpectedOpex(assets: List[AssetDynamoItem]): Elem = {
    <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.2">
      <opex:Transfer>
        <opex:SourceID>Test Name</opex:SourceID>
        <opex:Manifest>
          <opex:Files>
            {assets.map(asset => <opex:File type="metadata" size="4">{asset.id}.pax.opex</opex:File>)}
          </opex:Files>
          <opex:Folders>
            {assets.map(asset => <opex:Folder>{asset.id}.pax</opex:Folder>)}
          </opex:Folders>
        </opex:Manifest>
      </opex:Transfer>
      <opex:Properties>
        <opex:Title>Test Name</opex:Title>
        <opex:Description></opex:Description>
        <opex:SecurityDescriptor>unknown</opex:SecurityDescriptor>
        <opex:Identifiers>
          <opex:Identifier type="Code">Code</opex:Identifier>
        </opex:Identifiers>
      </opex:Properties>
    </opex:OPEXMetadata>
  }
end ExternalServicesTestUtils
