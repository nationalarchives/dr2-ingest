package uk.gov.nationalarchives.ingestparentfolderopexcreator

import cats.effect.IO
import fs2.*
import io.circe.generic.auto.*
import org.reactivestreams.{FlowAdapters, Publisher}
import pureconfig.ConfigReader
import software.amazon.awssdk.services.s3.model.PutObjectResponse
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.ingestparentfolderopexcreator.Lambda.*
import uk.gov.nationalarchives.utils.LambdaRunner

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.annotation.static

class Lambda extends LambdaRunner[Input, Unit, Config, Dependencies] {

  private def accumulatePrefixes(s: fs2.Stream[IO, String]): fs2.Stream[IO, List[String]] =
    s.fold[List[String]](Nil) { case (acc, path) =>
      path :: acc
    }.filter(_.nonEmpty)

  private def generateOpexWithManifest(paths: List[String]): String = {
    val folderElems = paths.map { path => <opex:Folder>{path.split('/').last}</opex:Folder> }
    <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.2">
      <opex:Transfer>
        <opex:Manifest>
          <opex:Folders>
            {folderElems}
          </opex:Folders>
        </opex:Manifest>
      </opex:Transfer>
    </opex:OPEXMetadata>.toString
  }

  override def handler: (
      Input,
      Config,
      Dependencies
  ) => IO[Unit] = (input, config, dependencies) => {

    def uploadToS3(opexXmlContent: String, fileName: String): Stream[IO, PutObjectResponse] = Stream.eval {
      dependencies.s3Client.upload(config.destinationBucket, fileName, ByteBuffer.wrap(opexXmlContent.getBytes))
    }

    val keyPrefix = s"opex/${input.executionId}/"
    val opexFileName = s"$keyPrefix${input.executionId}.opex"
    val batchRef = input.executionId.split('-').take(3).mkString("-")
    val logger = log(Map("batchRef" -> batchRef))(_)
    for {
      prefixStream <- dependencies.s3Client.listCommonPrefixes(config.destinationBucket, keyPrefix)
      _ <- logger(s"Retrieved prefixes for key $keyPrefix from bucket ${config.destinationBucket}")
      completedUpload <- javaStreamToFs2(prefixStream)
        .through(accumulatePrefixes)
        .map(generateOpexWithManifest)
        .flatMap { opexXmlString => uploadToS3(opexXmlString, opexFileName) }
        .compile
        .toList
      _ <- logger(s"Uploaded opex file $opexFileName")
      _ <- IO.raiseWhen(completedUpload.isEmpty)(new Exception(s"No uploads were attempted for '$keyPrefix'"))
    } yield completedUpload.head
  }
  
  private def javaStreamToFs2[T](javaStream: java.util.stream.Stream[T]): Stream[IO, T] = 
    Stream.fromIterator(javaStream.iterator().asScala, 64)

  override def dependencies(config: Config): IO[Dependencies] = IO(Dependencies(DAS3Client[IO](config.roleArn, lambdaName)))
}
object Lambda {
  @static def main(args: Array[String]): Unit = new Lambda().run()
  
  extension (publisher: Publisher[String])
    def publisherToStream: Stream[IO, String] = Stream.eval(IO.delay(publisher)).flatMap { publisher =>
      fs2.interop.flow.fromPublisher[IO](FlowAdapters.toFlowPublisher(publisher), chunkSize = 16)
    }

  case class Input(executionId: String)
  case class Config(destinationBucket: String, roleArn: String) derives ConfigReader

  case class Dependencies(s3Client: DAS3Client[IO])
}
