package uk.gov.nationalarchives.preingesttdrpackagebuilder

import cats.effect.IO
import fs2.Collector.string
import fs2.hashing.{HashAlgorithm, Hashing}
import fs2.interop.reactivestreams.*
import fs2.{Chunk, Stream}
import io.circe
import io.circe.Json
import io.circe.fs2.{decoder as fs2Decoder, *}
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.reactivestreams.FlowAdapters
import org.scanamo.syntax.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import uk.gov.nationalarchives.DADynamoDBClient.given
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.IngestLockTableItem
import uk.gov.nationalarchives.preingesttdrpackagebuilder.Lambda.*
import uk.gov.nationalarchives.utils.ExternalUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.RepresentationType.Preservation
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}

import java.net.URI
import java.nio.ByteBuffer
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID

class Lambda extends LambdaRunner[Input, Output, Config, Dependencies]:
  lazy private val bufferSize = 1024 * 5

  private def stripFileExtension(title: String) = if title.contains(".") then title.substring(0, title.lastIndexOf('.')) else title

  override def handler: (Input, Config, Dependencies) => IO[Output] = (input, config, dependencies) => {

    def processNonMetadataObjects(tdrMetadataJsonStream: Stream[IO, Json], fileLocation: URI, metadataId: UUID, messageId: String): Stream[IO, MetadataObject] = {
      tdrMetadataJsonStream
        .through(fs2Decoder[IO, TDRMetadata])
        .flatMap { tdrMetadata =>
          val contentFolderId = dependencies.uuidGenerator()
          val assetId = tdrMetadata.UUID
          val fileId = dependencies.uuidGenerator()
          val contentFolderMetadata = ContentFolderMetadataObject(contentFolderId, None, None, tdrMetadata.ConsignmentReference, tdrMetadata.Series, Nil)
          val assetMetadata = AssetMetadataObject(
            assetId,
            Option(contentFolderId),
            stripFileExtension(tdrMetadata.Filename),
            assetId.toString,
            List(fileId),
            List(metadataId),
            None,
            tdrMetadata.TransferringBody,
            LocalDateTime.parse(tdrMetadata.TransferInitiatedDatetime.replace(" ", "T")).atOffset(ZoneOffset.UTC),
            "TDR",
            "Born Digital",
            "TDR",
            Option(messageId),
            List(
              IdField("Code", s"${tdrMetadata.Series}/${tdrMetadata.FileReference}"),
              IdField("UpstreamSystemReference", tdrMetadata.FileReference),
              IdField("BornDigitalRef", tdrMetadata.FileReference),
              IdField("ConsignmentReference", tdrMetadata.ConsignmentReference),
              IdField("RecordID", assetId.toString)
            )
          )
          Stream.evals {
            dependencies.s3Client
              .headObject(fileLocation.getHost, fileLocation.getPath.drop(1))
              .map { headObjectResponse =>
                val fileMetadata = FileMetadataObject(
                  fileId,
                  Option(assetId),
                  stripFileExtension(tdrMetadata.Filename),
                  1,
                  tdrMetadata.Filename,
                  headObjectResponse.contentLength(),
                  Preservation,
                  1,
                  fileLocation,
                  tdrMetadata.SHA256ServerSideChecksum
                )
                List(contentFolderMetadata, assetMetadata, fileMetadata)
              }
          }
        }
    }

    def metadataChecksum(metadataFileBytes: Array[Byte]) = Stream
      .emits(metadataFileBytes)
      .through(fs2.hashing.Hashing[IO].hash(HashAlgorithm.SHA256))
      .flatMap(hash => Stream.emits(hash.bytes.toList))
      .through(fs2.text.hex.encode)
      .compile
      .to(string)

    def processMetadataFiles(tdrMetadataJsonStream: Stream[IO, Json], fileLocation: URI, metadataId: UUID): Stream[IO, MetadataObject] = {
      tdrMetadataJsonStream
        .flatMap { tdrMetadataJson =>
          Stream.evals {
            val fileBytes = tdrMetadataJson.noSpaces.getBytes
            for {
              tdrMetadata <- IO.fromEither(tdrMetadataJson.as[TDRMetadata])
              checksum <- metadataChecksum(fileBytes)
            } yield {
              val metadataFileSize = fileBytes.length
              val metadata = FileMetadataObject(
                metadataId,
                Option(tdrMetadata.UUID),
                s"${tdrMetadata.UUID}-metadata",
                2,
                s"${tdrMetadata.UUID}-metadata.json",
                metadataFileSize,
                Preservation,
                1,
                getMetadataUri(fileLocation),
                checksum
              )
              List(metadata)
            }
          }
        }
    }

    def processTdrMetadata(tdrMetadataJsonStream: Stream[IO, Json], fileLocation: URI, messageId: UUID): Stream[IO, MetadataObject] = {
      val metadataId = dependencies.uuidGenerator()
      tdrMetadataJsonStream.broadcastThrough(
        jsonStream => processNonMetadataObjects(jsonStream, fileLocation, metadataId, messageId.toString),
        jsonStream => processMetadataFiles(jsonStream, fileLocation, metadataId)
      )
    }

    def downloadMetadataFile(lockTableMessage: LockTableMessage) = {
      val fileLocation = lockTableMessage.location
      val metadataUri = getMetadataUri(fileLocation)
      val messageId = lockTableMessage.id
      dependencies.s3Client
        .download(metadataUri.getHost, metadataUri.getPath.drop(1))
        .map { pub =>
          pub
            .toStreamBuffered[IO](bufferSize)
            .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
            .through(byteStreamParser[IO])
            .through(metadataJsonStream => processTdrMetadata(metadataJsonStream, fileLocation, messageId))
        }
    }

    def processLockTableItems(lockTableItems: List[IngestLockTableItem]): IO[Unit] = {
      Stream
        .emits(lockTableItems)
        .map(_.message)
        .through(stringStreamParser[IO])
        .through(fs2Decoder[IO, LockTableMessage])
        .parEvalMap(config.concurrency)(downloadMetadataFile)
        .parJoin(config.concurrency)
        .compile
        .toList
        .flatMap { metadata =>
          IO.raiseWhen(metadata.isEmpty)(new Exception(s"Metadata list for ${input.groupId} is empty")) >> {
            val metadataBytes = metadata.asJson.noSpaces.getBytes
            Stream.emits(metadataBytes).chunks.map(_.toByteBuffer).toPublisherResource[IO, ByteBuffer].use { publisher =>
              dependencies.s3Client.upload(config.rawCacheBucket, s"${input.batchId}/metadata.json", FlowAdapters.toPublisher(publisher)) >> IO.unit
            }
          }
        }
    }

    dependencies.dynamoDbClient
      .queryItems(config.lockTableName, "groupId" === input.groupId, Option(config.lockTableGsiName))
      .flatMap(processLockTableItems)
      .map(_ => Output(input.groupId, input.batchId, input.retryCount, URI.create(s"s3://${config.rawCacheBucket}/${input.batchId}/metadata.json")))
  }

  private def getMetadataUri(fileLocation: URI): URI = {
    val bucket = fileLocation.getHost
    val fileKey = fileLocation.getPath.drop(1)
    val metadataKey = s"$fileKey.metadata"
    URI.create(s"s3://$bucket/$metadataKey")
  }

  override def dependencies(config: Config): IO[Dependencies] =
    IO(Dependencies(DADynamoDBClient[IO](), DAS3Client[IO](), () => UUID.randomUUID()))
end Lambda

object Lambda:
  case class TDRMetadata(
      Series: String,
      UUID: UUID,
      description: Option[String],
      TransferringBody: String,
      TransferInitiatedDatetime: String,
      ConsignmentReference: String,
      Filename: String,
      SHA256ServerSideChecksum: String,
      FileReference: String
  )

  case class LockTableMessage(id: UUID, location: URI)

  case class Config(lockTableName: String, lockTableGsiName: String, rawCacheBucket: String, concurrency: Int) derives ConfigReader

  case class Dependencies(dynamoDbClient: DADynamoDBClient[IO], s3Client: DAS3Client[IO], uuidGenerator: () => UUID)

  case class Input(groupId: String, batchId: String, waitFor: Int, retryCount: Int = 0)

  case class Output(groupId: String, batchId: String, retryCount: Int, packageMetadata: URI)
