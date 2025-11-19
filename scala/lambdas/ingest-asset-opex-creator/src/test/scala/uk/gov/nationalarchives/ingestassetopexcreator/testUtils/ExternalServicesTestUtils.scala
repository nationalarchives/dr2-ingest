package uk.gov.nationalarchives.ingestassetopexcreator.testUtils

import fs2.interop.reactivestreams.*
import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import org.reactivestreams.Publisher
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.model.{BatchWriteItemResponse, ResourceNotFoundException}
import software.amazon.awssdk.services.s3.model.{CopyObjectResponse, DeleteObjectsResponse, HeadObjectResponse, ListObjectsV2Response, PutObjectResponse}
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CompletedUpload}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.FileRepresentationType.PreservationRepresentationType
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{AssetDynamoItem, Checksum, FileDynamoItem, FilesTablePrimaryKey, Identifier}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.ingestassetopexcreator.Lambda.{Config, Dependencies, Input, InputAsset}
import uk.gov.nationalarchives.ingestassetopexcreator.{Lambda, XMLCreator}

import java.net.URI
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import scala.jdk.CollectionConverters.*
import java.util.UUID
import scala.xml.Elem

object ExternalServicesTestUtils:

  val batchId: String = "TEST-ID"

  val ingestDateTime: OffsetDateTime = OffsetDateTime.now

  case class AssetWithChildren(asset: AssetDynamoItem, children: List[FileDynamoItem])
  case class S3Object(bucket: String, key: String, content: String)

  def generateInput(assetId: UUID): Input = Input(List(InputAsset(assetId, batchId, false)))

  val config: Config = Config("", "", "destinationBucket", "roleArn")

  def resourceNotFound(dynamoError: Boolean): IO[Unit] = IO.raiseWhen(dynamoError)(ResourceNotFoundException.builder.message("Error getting items from Dynamo").build)
  def itemsNotFound(dynamoError: Boolean): IO[Unit] = IO.raiseWhen(dynamoError)(ResourceNotFoundException.builder.message("Error querying Dynamo").build)
  def s3CopyError(s3Error: Boolean): IO[Unit] = IO.raiseWhen(s3Error)(new Exception("Error copying s3 objects"))
  def s3UploadError(s3Error: Boolean): IO[Unit] = IO.raiseWhen(s3Error)(new Exception("Error uploading s3 objects"))

  def notImplemented[T]: IO[T] = IO.raiseError(new Exception("Not implemented"))

  def dynamoClient(assets: List[AssetWithChildren], errors: Option[Errors]): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
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
          assets
            .filter(each => each.asset.id.toString == parentPath.s() && each.asset.batchId == batchId.s())
            .flatMap(_.children)
            .map(_.asInstanceOf[U])
        }).getOrElse(Nil)
      }

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] =
      val key = primaryKeys.head.asInstanceOf[FilesTablePrimaryKey]
      resourceNotFound(errors.exists(_.dynamoGetError)) >> IO {
        assets.map(_.asset).filter(_.id == key.partitionKey.id).map(_.asInstanceOf[T])
      }

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = notImplemented

  def s3Client(ref: Ref[IO, List[S3Object]], errors: Option[Errors]): DAS3Client[IO] = new DAS3Client[IO]:
    override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] =
      s3CopyError(errors.exists(_.s3CopyError)) >>
        ref
          .update { existing =>
            existing
              .find(obj => obj.bucket == sourceBucket && obj.key == sourceKey)
              .map(obj => obj.copy(bucket = destinationBucket, key = destinationKey))
              .toList
          }
          .map(_ => CompletedCopy.builder.response(CopyObjectResponse.builder.build).build)

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

    override def headObject(bucket: String, key: String): IO[HeadObjectResponse] = notImplemented

    override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = notImplemented

    override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[SdkPublisher[String]] = notImplemented

    override def listObjects(bucket: String, prefix: Option[String]): IO[ListObjectsV2Response] = notImplemented

  case class Errors(dynamoGetError: Boolean = false, dynamoQueryError: Boolean = false, s3CopyError: Boolean = false, s3UploadError: Boolean = false)

  def runLambda(input: Input, dynamoItems: List[AssetWithChildren], s3Objects: List[S3Object], errors: Option[Errors] = None): (Either[Throwable, Unit], List[S3Object]) = (for {
    s3Ref <- Ref.of[IO, List[S3Object]](s3Objects)
    dependencies = Dependencies(dynamoClient(dynamoItems, errors), s3Client(s3Ref, errors), XMLCreator(ingestDateTime))
    result <- new Lambda().handler(input, config, dependencies).attempt
    s3Objects <- s3Ref.get
  } yield (result, s3Objects)).unsafeRunSync()

  def generateAsset: AssetDynamoItem = AssetDynamoItem(
    batchId,
    UUID.randomUUID,
    None,
    Asset,
    None,
    None,
    Option("Body"),
    Option(OffsetDateTime.parse("2023-06-01T00:00Z")),
    "upstreamSystem",
    "digitalAssetSource",
    None,
    Nil,
    true,
    true,
    List(Identifier("UpstreamSystemReference", "upstreamSystemReference")),
    1,
    false,
    None,
    "/a/file/path"
  )

  def generateFile: FileDynamoItem = FileDynamoItem(
    batchId,
    UUID.randomUUID(),
    None,
    "name",
    File,
    None,
    None,
    1,
    2,
    List(Checksum("SHA256", "testChecksumAlgo1")),
    Option("ext"),
    PreservationRepresentationType,
    1,
    false,
    true,
    Nil,
    1,
    URI.create("s3://bucket/key")
  )

  def generateExpectedOpex(asset: AssetDynamoItem, files: List[FileDynamoItem]): Elem = {
    val fileOne = files.head
    val fileTwo = files.last
    <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.2">
        <opex:Transfer>
          <opex:SourceID>{asset.id}</opex:SourceID>
          <opex:Manifest>
            <opex:Files>
              <opex:File type="metadata" size="3055">{asset.id}.xip</opex:File>
              <opex:File type="content" size={fileOne.fileSize.toString}>Representation_Preservation/{fileOne.id}/Generation_1/{fileOne.id}.ext</opex:File>
              <opex:File type="content" size={fileTwo.fileSize.toString}>Representation_Preservation/{fileTwo.id}/Generation_1/{fileTwo.id}.ext</opex:File>
            </opex:Files>
            <opex:Folders>
              <opex:Folder>Representation_Preservation</opex:Folder>
              <opex:Folder>Representation_Preservation/{fileOne.id}</opex:Folder>
              <opex:Folder>Representation_Preservation/{fileOne.id}/Generation_1</opex:Folder>
              <opex:Folder>Representation_Preservation/{fileTwo.id}</opex:Folder>
              <opex:Folder>Representation_Preservation/{fileTwo.id}/Generation_1</opex:Folder>
            </opex:Folders>
          </opex:Manifest>
        </opex:Transfer>
        <opex:Properties>
          <opex:Title>{asset.id}</opex:Title>
          <opex:Description/>
          <opex:SecurityDescriptor>unknown</opex:SecurityDescriptor>
          <opex:Identifiers>
            <opex:Identifier type="UpstreamSystemReference">upstreamSystemReference</opex:Identifier>
          </opex:Identifiers>
        </opex:Properties>
        <opex:DescriptiveMetadata>
          <Source xmlns="http://dr2.nationalarchives.gov.uk/source">
            <DigitalAssetSource>{asset.digitalAssetSource}</DigitalAssetSource>
            <DigitalAssetSubtype/>
            <IngestDateTime>{ingestDateTime}</IngestDateTime>
            <OriginalMetadataFiles></OriginalMetadataFiles>
            <TransferDateTime>{asset.transferCompleteDatetime.getOrElse("")}</TransferDateTime>
            <TransferringBody>{asset.transferringBody.getOrElse("")}</TransferringBody>
            <UpstreamSystem>{asset.upstreamSystem}</UpstreamSystem>
            <UpstreamSystemRef>upstreamSystemReference</UpstreamSystemRef>
            <UpstreamPath>{asset.filePath}</UpstreamPath>
          </Source>
        </opex:DescriptiveMetadata>
      </opex:OPEXMetadata>
  }
end ExternalServicesTestUtils
