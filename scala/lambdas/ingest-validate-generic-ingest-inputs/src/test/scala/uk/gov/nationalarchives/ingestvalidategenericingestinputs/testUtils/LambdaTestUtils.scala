package uk.gov.nationalarchives.ingestvalidategenericingestinputs.testUtils

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.interop.reactivestreams.*
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.{Decoder, HCursor, Json}
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.s3.model.{DeleteObjectsResponse, HeadObjectResponse, PutObjectResponse}
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CompletedUpload}
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Utils.ErrorMessage.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Utils.LambdaConfiguration.*

import java.net.URI
import java.nio.ByteBuffer
import scala.io.Source
import scala.jdk.CollectionConverters.*

object LambdaTestUtils {
  case class TestInput(input: String, expectedErrors: List[String])

  given Decoder[TestInput] = (c: HCursor) =>
    for {
      input <- c.downField("input").as[Json]
      expectedErrors <- c.downField("expectedErrors").as[List[String]]
    } yield TestInput(input.noSpaces, expectedErrors)

  def notImplemented[T]: IO[T] = IO.raiseError(new Exception("Not implemented"))

  def s3Client(ref: Ref[IO, List[WholeFileValidationResult]], inputFile: TestInput, filesFoundInS3: Boolean): DAS3Client[IO] = new DAS3Client[IO]:
    override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] = notImplemented

    override def download(bucket: String, key: String): IO[Publisher[ByteBuffer]] =
      val fileContent = inputFile.input
      IO.pure(Flux.just(ByteBuffer.wrap(fileContent.getBytes)))

    override def upload(bucket: String, key: String, publisher: Publisher[ByteBuffer]): IO[CompletedUpload] = for {
      fileValidationResults <- publisher
        .toStreamBuffered[IO](1024)
        .map(_.array().map(_.toChar).mkString)
        .evalMap[IO, WholeFileValidationResult](str => IO.fromEither(decode[WholeFileValidationResult](str)))
        .compile
        .toList
      _ <- ref.update(_ ++ fileValidationResults)
    } yield CompletedUpload.builder.response(PutObjectResponse.builder.build).build

    override def headObject(bucket: String, key: String): IO[HeadObjectResponse] =
      if filesFoundInS3 then IO.pure(HeadObjectResponse.builder.build) else IO.raiseError(new Exception("Missing object"))

    override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = notImplemented

    override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[SdkPublisher[String]] = notImplemented

  def runLambda(inputFile: String, filesFoundInS3: Boolean = true): (List[String], List[String]) = {
    val inputString = Source.fromResource(s"inputjson/$inputFile").getLines().mkString
    val input = Input("batchId", URI.create("s3://bucket/key"))
    val config = Config()
    for {
      testInput <- IO.fromEither(decode[TestInput](inputString))
      validationRef <- Ref.of[IO, List[WholeFileValidationResult]](Nil)
      dependencies = Dependencies(s3Client(validationRef, testInput, filesFoundInS3))
      _ <- new Lambda().handler(input, config, dependencies).attempt
      validationResults <- validationRef.get
    } yield {
      val firstResultAsList = validationResults.headOption.toList
      val allErrors = firstResultAsList.flatMap(_.errors.map(_.show)) ++ firstResultAsList.flatMap(_.singleResults.flatMap(_.errors))
      (testInput.expectedErrors, allErrors.sorted)
    }
  }.unsafeRunSync()
}
