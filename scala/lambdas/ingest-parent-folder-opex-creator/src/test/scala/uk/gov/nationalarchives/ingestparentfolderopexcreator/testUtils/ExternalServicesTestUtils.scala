package uk.gov.nationalarchives.ingestparentfolderopexcreator.testUtils

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import software.amazon.awssdk.services.s3.model.{CopyObjectResponse, DeleteObjectsResponse, HeadObjectResponse, PutObjectResponse}
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.ingestparentfolderopexcreator.Lambda
import uk.gov.nationalarchives.ingestparentfolderopexcreator.Lambda.*

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

object ExternalServicesTestUtils {
  case class S3Object(bucket: String, key: String, content: String)

  case class Errors(listPrefix: Boolean = false, upload: Boolean = false)

  def notImplemented[T]: IO[T] = IO.raiseError(new Exception("Not implemented"))

  def s3Client(ref: Ref[IO, List[S3Object]], errors: Option[Errors]): DAS3Client[IO] = new DAS3Client[IO]:
    def generateError(errorFn: Errors => Boolean, message: String): IO[Unit] = if errors.exists(errorFn) then IO.raiseError(new Exception(message))
    else IO.unit

    override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CopyObjectResponse] = notImplemented

    override def download(bucket: String, key: String): IO[ByteArrayOutputStream] = notImplemented

    override def upload(bucket: String, key: String, byteBuffer: ByteBuffer): IO[PutObjectResponse] = generateError(e => e.upload, "Upload has failed") >>
      ref
        .update { existing =>
          S3Object(bucket, key, byteBuffer.array().map(_.toChar).mkString) :: existing
        }
        .map(_ => PutObjectResponse.builder.build)

    override def headObject(bucket: String, key: String): IO[HeadObjectResponse] = notImplemented

    override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = notImplemented

    override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[java.util.stream.Stream[String]] = generateError(e => e.listPrefix, "List prefixes failed") >>
      ref.get.map { existing =>
        val filteredObjects = existing.filter(_.key.startsWith(keysPrefixedWith))
        val builder = java.util.stream.Stream.builder[String]()
        filteredObjects.map(_.key).foreach(builder.add)
        builder.build()
      }

  def runLambda(initialS3State: List[S3Object], errors: Option[Errors] = None): (Either[Throwable, Unit], List[S3Object]) =
    (for {
      ref <- Ref.of[IO, List[S3Object]](initialS3State)
      res <- new Lambda().handler(Input("executionId"), Config("bucketName", "roleArn"), Dependencies(s3Client(ref, errors))).attempt
      finalS3State <- ref.get
    } yield res -> finalS3State).unsafeRunSync()
}
