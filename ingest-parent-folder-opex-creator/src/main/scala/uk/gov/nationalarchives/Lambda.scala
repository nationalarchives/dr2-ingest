package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import fs2._
import org.reactivestreams.{FlowAdapters, Publisher}
import org.typelevel.log4cats.slf4j.Slf4jFactory
import org.typelevel.log4cats.{LoggerName, SelfAwareStructuredLogger}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.Lambda.{Config, PublisherToStream, StepFnInput}
import upickle.default
import upickle.default._

import java.io.{InputStream, OutputStream}
import scala.io.Source
import scala.xml.PrettyPrinter

class Lambda extends RequestStreamHandler {
  val dAS3Client: DAS3Client[IO] = DAS3Client[IO]()
  implicit val inputReader: Reader[StepFnInput] = macroR[StepFnInput]
  implicit val loggerName: LoggerName = LoggerName("Ingest Parent Folder Opex Creator")
  private val logger: SelfAwareStructuredLogger[IO] = Slf4jFactory.create[IO].getLogger

  def handleRequest(input: InputStream, output: OutputStream, context: Context): Unit = {
    val rawInput: String = Source.fromInputStream(input).mkString
    val stepFnInput = default.read[StepFnInput](rawInput)
    val keyPrefix = s"opex/${stepFnInput.executionId}/"
    val opexFileName = s"$keyPrefix${stepFnInput.executionId}.opex"
    val batchRef = stepFnInput.executionId.split("-").take(3).mkString("-")
    val log = logger.info(Map("batchRef" -> batchRef))(_)
    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      publisher <- dAS3Client.listCommonPrefixes(config.stagingCacheBucket, keyPrefix)
      _ <- log(s"Retrieved prefixes for key $keyPrefix from bucket ${config.stagingCacheBucket}")
      completedUpload <- publisher.publisherToStream
        .through(accumulatePrefixes)
        .map(generateOpexWithManifest)
        .flatMap { opexXmlString => uploadToS3(opexXmlString, opexFileName, config.stagingCacheBucket) }
        .compile
        .toList
      _ <- log(s"Uploaded opex file $opexFileName")
      _ <- IO.raiseWhen(completedUpload.isEmpty)(new Exception(s"No uploads were attempted for '$keyPrefix'"))
    } yield completedUpload.head
  }.onError(logLambdaError).unsafeRunSync()

  private def logLambdaError(error: Throwable): IO[Unit] =
    logger.error(error)("Error running ingest parent folder opex creator")

  private def accumulatePrefixes(s: fs2.Stream[IO, String]): fs2.Stream[IO, List[String]] =
    s.fold[List[String]](Nil) { case (acc, path) =>
      path :: acc
    }.filter(_.nonEmpty)

  def generateOpexWithManifest(paths: List[String]): String = {
    val folderElems = paths.map { path => <opex:Folder>{path.split("/").last}</opex:Folder> }
    val opex =
      <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.2">
        <opex:Transfer>
          <opex:Manifest>
            <opex:Folders>
              {folderElems}
            </opex:Folders>
          </opex:Manifest>
        </opex:Transfer>
      </opex:OPEXMetadata>
    new PrettyPrinter(80, 2).format(opex)
  }

  private def uploadToS3(
      opexXmlContent: String,
      fileName: String,
      bucketName: String
  ): Stream[IO, CompletedUpload] = Stream.eval {
    Stream
      .emits[IO, Byte](opexXmlContent.getBytes)
      .chunks
      .map(_.toByteBuffer)
      .toPublisherResource
      .use { publisher =>
        dAS3Client.upload(bucketName, fileName, opexXmlContent.getBytes.length, FlowAdapters.toPublisher(publisher))
      }
  }
}

object Lambda extends App {

  implicit class PublisherToStream(publisher: Publisher[String]) {
    def publisherToStream: Stream[IO, String] = Stream.eval(IO.delay(publisher)).flatMap { publisher =>
      fs2.interop.flow.fromPublisher[IO](FlowAdapters.toFlowPublisher(publisher), chunkSize = 16)
    }
  }

  case class StepFnInput(executionId: String)
  private case class Config(stagingCacheBucket: String)
}
