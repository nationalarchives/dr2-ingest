package uk.gov.nationalarchives.ingestparentfolderopexcreator

import cats.effect.IO
import fs2.*
import io.circe.generic.auto.*
import org.reactivestreams.{FlowAdapters, Publisher}
import pureconfig.ConfigReader
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.ingestparentfolderopexcreator.Lambda.*
import uk.gov.nationalarchives.utils.LambdaRunner

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

    def uploadToS3(opexXmlContent: String, fileName: String): Stream[IO, CompletedUpload] = Stream.eval {
      Stream
        .emits[IO, Byte](opexXmlContent.getBytes)
        .chunks
        .map(_.toByteBuffer)
        .toPublisherResource
        .use { publisher =>
          dependencies.s3Client.upload(config.destinationBucket, fileName, FlowAdapters.toPublisher(publisher))
        }
    }

    val keyPrefix = s"opex/${input.executionId}/"
    val opexFileName = s"$keyPrefix${input.executionId}.opex"
    val batchRef = input.executionId.split('-').take(3).mkString("-")
    val log = logger.info(Map("batchRef" -> batchRef))(_)
    for {
      publisher <- dependencies.s3Client.listCommonPrefixes(config.destinationBucket, keyPrefix)
      _ <- log(s"Retrieved prefixes for key $keyPrefix from bucket ${config.destinationBucket}")
      completedUpload <- publisher.publisherToStream
        .through(accumulatePrefixes)
        .map(generateOpexWithManifest)
        .flatMap { opexXmlString => uploadToS3(opexXmlString, opexFileName) }
        .compile
        .toList
      _ <- log(s"Uploaded opex file $opexFileName")
      _ <- IO.raiseWhen(completedUpload.isEmpty)(new Exception(s"No uploads were attempted for '$keyPrefix'"))
    } yield completedUpload.head
  }

  override def dependencies(config: Config): IO[Dependencies] = IO(Dependencies(DAS3Client[IO](config.roleArn, lambdaName)))
}
object Lambda {

  extension (publisher: Publisher[String])
    def publisherToStream: Stream[IO, String] = Stream.eval(IO.delay(publisher)).flatMap { publisher =>
      fs2.interop.flow.fromPublisher[IO](FlowAdapters.toFlowPublisher(publisher), chunkSize = 16)
    }

  case class Input(executionId: String)
  case class Config(destinationBucket: String, roleArn: String) derives ConfigReader

  case class Dependencies(s3Client: DAS3Client[IO])
}
