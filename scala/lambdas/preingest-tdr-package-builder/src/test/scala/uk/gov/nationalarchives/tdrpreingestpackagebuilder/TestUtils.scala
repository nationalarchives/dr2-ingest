package uk.gov.nationalarchives.tdrpreingestpackagebuilder

import cats.effect.{IO, Ref}
import fs2.interop.reactivestreams.*
import io.circe.derivation.Configuration
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.*
import org.reactivestreams.Publisher
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import reactor.core.publisher.Flux
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.s3.model.{DeleteObjectsResponse, HeadObjectResponse, PutObjectResponse}
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CompletedUpload}
import uk.gov.nationalarchives.DADynamoDBClient.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.IngestLockTable
import uk.gov.nationalarchives.tdrpreingestpackagebuilder.Lambda.*
import uk.gov.nationalarchives.utils.ExternalUtils.*
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}

import java.net.URI
import java.nio.ByteBuffer
import java.util.UUID

object TestUtils:

  given Encoder[LockTableMessage] = deriveEncoder[LockTableMessage]

  given Encoder[TDRMetadata] = deriveEncoder[TDRMetadata]

  type S3Objects = TDRMetadata | List[MetadataObject]

  def mockDynamoClient(ref: Ref[IO, List[IngestLockTable]], failedQuery: Boolean = false): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.pure(Nil)

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = IO.pure(1)

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.pure(Nil)

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      if failedQuery then IO.raiseError(new Exception("Dynamo has returned an error"))
      else
        val groupId = for {
          dynamoValues <- requestCondition.dynamoValues
          conditionValue <- dynamoValues.values.headOption
          value <- conditionValue.asString
        } yield value

        ref.get.map(_.filter(table => groupId.contains(table.groupId)).asInstanceOf[List[U]])

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = IO.pure(Nil)

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = IO.pure(1)

  def mockS3(ref: Ref[IO, Map[String, S3Objects]], downloadError: Boolean = false, uploadError: Boolean = false): DAS3Client[IO] = {
    new DAS3Client[IO]() {
      override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = IO.pure(DeleteObjectsResponse.builder.build)

      override def download(bucket: String, key: String): IO[Publisher[ByteBuffer]] =
        given Encoder[TDRMetadata] = deriveEncoder[TDRMetadata]
        if downloadError then IO.raiseError(new Exception(s"Error downloading $key from S3 $bucket"))
        else
          for {
            fileMap <- ref.get
            metadata <- fileMap(key) match
              case metadata: TDRMetadata => IO.pure(metadata.asJson.noSpaces.getBytes)
              case _                     => IO.raiseError(new Exception("Expecting TDR Metadata Json"))
          } yield Flux.just(ByteBuffer.wrap(metadata))

      override def headObject(bucket: String, key: String): IO[HeadObjectResponse] = IO.pure(HeadObjectResponse.builder.build)

      override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[SdkPublisher[String]] = IO.pure(SdkPublisher.fromIterable(java.util.List.of()))

      override def upload(bucket: String, key: String, publisher: Publisher[ByteBuffer]): IO[CompletedUpload] = {
        if uploadError then IO.raiseError(new Exception(s"Error uploading $key to $bucket"))
        else
          for {
            jsonString <- publisher.toStreamBuffered[IO](1024).map(_.array().map(_.toChar).mkString).compile.string
            json <- IO.fromEither(decode[List[MetadataObject]](jsonString))
            _ <- ref.update(currentMap => currentMap + (key -> json))
          } yield CompletedUpload.builder.response(PutObjectResponse.builder.build).build

      }

      override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] = IO.pure(CompletedCopy.builder.build)
    }
  }

  given Configuration = Configuration.default.withDefaults

  given Decoder[RepresentationType] = (c: HCursor) => c.as[String].map(RepresentationType.valueOf)

  given Decoder[IdField] = deriveDecoder[IdField]

  given Decoder[ArchiveFolderMetadataObject] = Decoder.derivedConfigured[ArchiveFolderMetadataObject]

  given Decoder[ContentFolderMetadataObject] = Decoder.derivedConfigured[ContentFolderMetadataObject]

  given Decoder[AssetMetadataObject] = Decoder.derivedConfigured[AssetMetadataObject]

  given Decoder[FileMetadataObject] = (c: HCursor) =>
    for {
      id <- c.downField("id").as[UUID]
      parentId <- c.downField("parentId").as[Option[UUID]]
      title <- c.downField("title").as[String]
      sortOrder <- c.downField("sortOrder").as[Int]
      name <- c.downField("name").as[String]
      fileSize <- c.downField("fileSize").as[Long]
      representationType <- c.downField("representationType").as[RepresentationType]
      representationSuffix <- c.downField("representationSuffix").as[Int]
      location <- c.downField("location").as[URI]
      checksum <- c.downField("checksum_sha256").as[String]
    } yield FileMetadataObject(id, parentId, title, sortOrder, name, fileSize, representationType, representationSuffix, location, checksum)

  given Decoder[MetadataObject] = (c: HCursor) =>
    for {
      objectType <- c.downField("type").as[String]
      metadataObject <- objectType match
        case "ArchiveFolder" => c.as[ArchiveFolderMetadataObject]
        case "ContentFolder" => c.as[ContentFolderMetadataObject]
        case "Asset"         => c.as[AssetMetadataObject]
        case "File"          => c.as[FileMetadataObject]
    } yield metadataObject
