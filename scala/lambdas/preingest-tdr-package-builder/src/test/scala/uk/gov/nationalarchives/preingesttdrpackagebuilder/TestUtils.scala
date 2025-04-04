package uk.gov.nationalarchives.preingesttdrpackagebuilder

import cats.effect.{IO, Ref}
import io.circe.*
import io.circe.derivation.Configuration
import io.circe.generic.semiauto.deriveEncoder
import io.circe.parser.decode
import io.circe.syntax.*
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.s3.model.{CopyObjectResponse, DeleteObjectsResponse, HeadObjectResponse, PutObjectResponse}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.IngestLockTableItem
import uk.gov.nationalarchives.preingesttdrpackagebuilder.Lambda.*
import uk.gov.nationalarchives.utils.ExternalUtils.*
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}

import java.io.ByteArrayOutputStream
import java.net.URI
import java.nio.ByteBuffer

object TestUtils:

  given Encoder[LockTableMessage] = deriveEncoder[LockTableMessage]

  given Encoder[TDRMetadata] = deriveEncoder[TDRMetadata]

  case class MockTdrFile(fileSize: Long)

  type S3Objects = TDRMetadata | List[MetadataObject] | MockTdrFile

  def mockDynamoClient(ref: Ref[IO, List[IngestLockTableItem]], failedQuery: Boolean = false): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.pure(Nil)

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = IO.pure(1)

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.pure(Nil)

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      if failedQuery then IO.raiseError(new Exception("Dynamo has returned an error"))
      else
        val groupId = for {
          dynamoValues <- Option(requestCondition.attributes.values)
          conditionValue <- dynamoValues.values.headOption
          value <- conditionValue.asString
        } yield value

        ref.get.map(_.filter(table => groupId.contains(table.groupId)).asInstanceOf[List[U]])

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = IO.pure(Nil)

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = IO.pure(1)

  def mockS3(ref: Ref[IO, Map[String, S3Objects]], downloadError: Boolean = false, uploadError: Boolean = false): DAS3Client[IO] = {
    new DAS3Client[IO]() {
      override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = IO.pure(DeleteObjectsResponse.builder.build)

      override def download(bucket: String, key: String): IO[ByteArrayOutputStream] =
        given Encoder[TDRMetadata] = deriveEncoder[TDRMetadata]
        if downloadError then IO.raiseError(new Exception(s"Error downloading $key from S3 $bucket"))
        else
          for {
            fileMap <- ref.get
            metadata <- fileMap(key) match
              case metadata: TDRMetadata => IO.pure(metadata.asJson.noSpaces.getBytes)
              case _                     => IO.raiseError(new Exception("Expecting TDR Metadata Json"))
          } yield {
            val baos = new ByteArrayOutputStream()
            baos.write(metadata)
            baos
          }

      override def headObject(bucket: String, key: String): IO[HeadObjectResponse] = ref.get.map { objectsMap =>
        val fileSize = objectsMap.get(key).collect({ case mockTdrFile: MockTdrFile => mockTdrFile }).get.fileSize
        HeadObjectResponse.builder.contentLength(fileSize).build
      }

      override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[java.util.stream.Stream[String]] = IO.pure(java.util.stream.Stream.empty())

      override def upload(bucket: String, key: String, byteBuffer: ByteBuffer): IO[PutObjectResponse] = {
        if uploadError then IO.raiseError(new Exception(s"Error uploading $key to $bucket"))
        else
          for {
            json <- IO.fromEither(decode[List[MetadataObject]](byteBuffer.array().map(_.toChar).mkString))
            _ <- ref.update(currentMap => currentMap + (key -> json))
          } yield PutObjectResponse.builder.build

      }

      override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CopyObjectResponse] = IO.pure(CopyObjectResponse.builder.build)
    }
  }
