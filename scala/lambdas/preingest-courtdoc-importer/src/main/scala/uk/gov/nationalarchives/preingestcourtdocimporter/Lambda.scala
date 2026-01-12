package uk.gov.nationalarchives.preingestcourtdocimporter

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.reactivestreams.{FlowAdapters, Publisher}
import pureconfig.ConfigReader
import uk.gov.nationalarchives.{DAS3Client, DASQSClient}
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.utils.EventCodecs.given
import uk.gov.nationalarchives.preingestcourtdocimporter.Lambda.*
import fs2.{Chunk, Pipe, Stream, text}
import fs2.compression.Compression
import fs2.io.*
import io.circe.generic.auto.*
import io.circe.{Decoder, HCursor}

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.*
import io.circe.parser.decode
import uk.gov.nationalarchives.utils.ExternalUtils.TREMetadata

import java.io.{BufferedInputStream, InputStream}
import java.util.UUID

class Lambda  extends LambdaRunner[SQSEvent, Unit, Config, Dependencies]:
  override def handler: (SQSEvent, Config, Dependencies) => IO[Unit] = (sqsEvent, config, dependencies) => {

    def copyFilesFromDownloadToUploadBucket(bucket: String, key: String): IO[Map[String, String]] =
      dependencies.s3.download(bucket, key)
        .flatMap(
          _.publisherToStream
            .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
            .through(Compression[IO].gunzip())
            .flatMap(_.content)
            .through(unarchiveToS3)
            .compile
            .toList
        )
        .map(_.toMap)

    def unarchiveToS3: Pipe[IO, Byte, (String, String)] = { stream =>
      stream
        .through(toInputStream[IO])
        .map(new BufferedInputStream(_, chunkSize))
        .flatMap(is => Stream.resource(Resource.fromAutoCloseable(IO.blocking(new TarArchiveInputStream(is)))))
        .flatMap(unarchiveAndUploadToS3)
    }

    def unarchiveAndUploadToS3(tarInputStream: TarArchiveInputStream): Stream[IO, (String, String)] =
      Stream
        .eval(IO.blocking(Option(tarInputStream.getNextEntry)))
        .flatMap(Stream.fromOption[IO](_))
        .flatMap { tarEntry =>
          Stream
            .eval(IO(readInputStream(IO.pure[InputStream](tarInputStream), chunkSize, closeAfterUse = false)))
            .flatMap { stream =>
              if (!tarEntry.isDirectory) {
                val id = dependencies.uuidGenerator()
                Stream.eval[IO, (String, String)](
                  stream.compile.toList.flatMap { bytes =>
                    val byteArray = bytes.toArray
                    Stream
                      .emit[IO, ByteBuffer](ByteBuffer.wrap(byteArray))
                      .toPublisherResource
                      .use(pub => dependencies.s3.upload(config.outputBucket, id.toString, FlowAdapters.toPublisher(pub)))
                      .map { _ =>
                        tarEntry.getName -> id.toString
                      }
                  }
                )
              } else Stream.empty
            } ++
            unarchiveAndUploadToS3(tarInputStream)
        }

    def extractMetadataFromJson(str: Stream[IO, Byte]): Stream[IO, TREMetadata] =
      str
        .through(text.utf8.decode)
        .flatMap { jsonString =>
          Stream.fromEither[IO](decode[TREMetadata](jsonString))
        }

    def readJsonFromPackage(metadataId: String): IO[TREMetadata] = {
      for
        s3Publisher <- dependencies.s3.download(config.outputBucket, metadataId)
        contentString <- s3Publisher.publisherToStream
          .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
          .through(extractMetadataFromJson)
          .compile
          .toList
        parsedJson <- IO.fromOption(contentString.headOption)(
          new RuntimeException(
            "Error parsing metadata.json.\nPlease check that the JSON is valid and that all required fields are present"
          )
        )
      yield parsedJson
    }

    sqsEvent.getRecords.asScala.toList.parTraverse { record =>
      for
        treInput <- IO.fromEither(decode[TREInput](record.getBody))
        batchRef = treInput.parameters.reference
        logCtx = Map("batchRef" -> batchRef)
        _ <- log(logCtx)(s"Processing batchRef $batchRef")

        outputBucket = config.outputBucket
        fileNameToFileIds <- copyFilesFromDownloadToUploadBucket(treInput.parameters.s3Bucket, treInput.parameters.s3Key)
        _ <- log(logCtx)(s"Copied ${treInput.parameters.s3Key} from ${treInput.parameters.s3Bucket} to $outputBucket")
        metadataFileId <- IO.fromOption(fileNameToFileIds.get(s"$batchRef/TRE-$batchRef-metadata.json"))(
          new RuntimeException(s"Cannot find metadata for $batchRef")
        )
        treMetadata <- readJsonFromPackage(metadataFileId)
        fileId <- IO.fromOption(fileNameToFileIds.get(s"$batchRef/${treMetadata.parameters.TRE.payload.filename}"))(
          new RuntimeException(s"Document not found for file belonging to $batchRef")
        )
        irrelevantFilesFromTre = fileNameToFileIds.values.filter(!List(fileId, metadataFileId).contains(_))
        _ <- IO.whenA(irrelevantFilesFromTre.nonEmpty) {
          dependencies.s3.deleteObjects(outputBucket, irrelevantFilesFromTre.toList) >>
            log(Map("fileReference" -> treMetadata.parameters.TDR.`File-Reference`.orNull))("Deleted unused TRE objects from the root of S3")
        }
        tdrId = treMetadata.parameters.TDR.`UUID`
        _ <- dependencies.sqsClient.sendMessage(config.outputQueueUrl)(Message(tdrId, UUID.fromString(fileId), s"s3://${config.outputBucket}/$metadataFileId"))
      yield ()
    }.void
  }

  override def dependencies(config: Config): IO[Dependencies] =
    IO.pure(Dependencies(DAS3Client[IO](), DASQSClient[IO](), () => UUID.randomUUID))



object Lambda:

  private val chunkSize: Int = 1024 * 64
  case class Dependencies(s3: DAS3Client[IO], sqsClient: DASQSClient[IO], uuidGenerator: () => UUID)

  case class Message(id: UUID, fileId: UUID, location: String)

  given Decoder[TREInputProperties] = (c: HCursor) => for (messageId <- c.downField("messageId").as[Option[String]]) yield TREInputProperties(messageId)

  given Decoder[TREInputParameters] = (c: HCursor) =>
    for {
      status <- c.downField("status").as[String]
      reference <- c.downField("reference").as[String]
      s3Bucket <- c.downField("s3Bucket").as[String]
      s3Key <- c.downField("s3Key").as[String]
      skipSeriesLookup <- c.getOrElse("skipSeriesLookup")(false)
    } yield TREInputParameters(status, reference, skipSeriesLookup, s3Bucket, s3Key)



  extension (publisher: Publisher[ByteBuffer])
    def publisherToStream: Stream[IO, ByteBuffer] = Stream.eval(IO.delay(publisher)).flatMap { publisher =>
      fs2.interop.flow.fromPublisher[IO](FlowAdapters.toFlowPublisher(publisher), chunkSize = 16)
    }

  case class Config(outputBucket: String, outputQueueUrl: String) derives ConfigReader

  case class TREInputProperties(messageId: Option[String])

  case class TREInputParameters(status: String, reference: String, skipSeriesLookup: Boolean, s3Bucket: String, s3Key: String)

  case class TREInput(parameters: TREInputParameters, properties: Option[TREInputProperties] = None)