package uk.gov.nationalarchives.preingestcourtdocimporter

import cats.effect.IO
import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import org.reactivestreams.{FlowAdapters, Publisher}
import pureconfig.ConfigReader
import uk.gov.nationalarchives.{DAS3Client, DASQSClient}
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.utils.EventCodecs.given
import uk.gov.nationalarchives.preingestcourtdocimporter.Lambda.*
import fs2.{Chunk, Stream, text}
import io.circe.generic.auto.*
import io.circe.{Decoder, HCursor}

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.*
import io.circe.parser.decode
import uk.gov.nationalarchives.utils.ExternalUtils.TREMetadata

import java.util.UUID

class Lambda extends LambdaRunner[SQSEvent, Unit, Config, Dependencies]:
  override def handler: (SQSEvent, Config, Dependencies) => IO[Unit] = (sqsEvent, config, dependencies) => {

    val dataFileFolder = s"/data"

    def readJsonFromS3Location(
        bucket: String,
        metadataId: String
    ): IO[TREMetadata] =
      for
        s3Publisher <- dependencies.s3.download(bucket, metadataId)
        contentJson <- s3Publisher.publisherToStream
          .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
          .through(text.utf8.decode)
          .compile
          .string
        parsedJson <- IO.fromEither(decode[TREMetadata](contentJson))
      yield parsedJson

    sqsEvent.getRecords.asScala.toList.parTraverse { record =>
      for
        treInput <- IO.fromEither(decode[TREInput](record.getBody))
        batchRef = treInput.parameters.reference
        _ <- log(Map("batchRef" -> batchRef))(s"Processing batchRef $batchRef")

        metadataSourceKey = s"${treInput.parameters.s3FolderName}/TRE-$batchRef-metadata.json"
        treMetadata <- readJsonFromS3Location(treInput.parameters.s3Bucket, metadataSourceKey).onError { err =>
          log(Map("error" -> err.getMessage, "s3FolderName" -> treInput.parameters.s3FolderName))(err.getMessage)
        }
        treFileKey = s"${treInput.parameters.s3FolderName}$dataFileFolder/${treMetadata.parameters.TRE.payload.filename}"
        dr2FileKey = dependencies.uuidGenerator()

        outputBucket = config.outputBucket
        _ <- dependencies.s3.copy(treInput.parameters.s3Bucket, treFileKey, outputBucket, dr2FileKey.toString).onError { err =>
          log(Map("error" -> err.getMessage, "fileId" -> dr2FileKey.toString))(err.getMessage)
        }
        metadataDestinationKey = s"${dependencies.uuidGenerator()}.metadata"
        _ <- dependencies.s3.copy(treInput.parameters.s3Bucket, metadataSourceKey, outputBucket, metadataDestinationKey).onError { err =>
          log(Map("error" -> err.getMessage, "metadataFileId" -> metadataSourceKey))(err.getMessage)
        }

        tdrId = treMetadata.parameters.TDR.`UUID`
        _ <- dependencies.sqsClient.sendMessage(config.outputQueueUrl)(
          Message(tdrId, dr2FileKey, s"s3://$outputBucket/$metadataDestinationKey", treInput.parameters.skipSeriesLookup)
        )
        _ <- log(Map("batchRef" -> batchRef))(s"Finished processing batch $batchRef and imported file $dr2FileKey using metadata $metadataDestinationKey")
      yield ()
    }.void
  }

  override def dependencies(config: Config): IO[Dependencies] =
    IO.pure(Dependencies(DAS3Client[IO](), DASQSClient[IO](), () => UUID.randomUUID))

object Lambda:

  case class Dependencies(s3: DAS3Client[IO], sqsClient: DASQSClient[IO], uuidGenerator: () => UUID)

  case class Message(id: UUID, fileId: UUID, location: String, skipSeriesLookup: Boolean)

  given Decoder[TREInputParameters] = (c: HCursor) =>
    for {
      reference <- c.downField("reference").as[String]
      s3FolderName <- c.downField("s3FolderName").as[String]
      originator <- c.downField("originator").as[String]
      s3Bucket <- c.downField("s3Bucket").as[String]
      status <- c.downField("status").as[String]
      skipSeriesLookup <- c.getOrElse("skipSeriesLookup")(false)
    } yield TREInputParameters(reference, s3FolderName, originator, s3Bucket, status, skipSeriesLookup)

  extension (publisher: Publisher[ByteBuffer])
    def publisherToStream: Stream[IO, ByteBuffer] = Stream.eval(IO.delay(publisher)).flatMap { publisher =>
      fs2.interop.flow.fromPublisher[IO](FlowAdapters.toFlowPublisher(publisher), chunkSize = 16)
    }

  case class Config(outputBucket: String, outputQueueUrl: String) derives ConfigReader

  case class TREInputParameters(reference: String, s3FolderName: String, originator: String, s3Bucket: String, status: String, skipSeriesLookup: Boolean = false)

  case class TREInput(parameters: TREInputParameters)
