package uk.gov.nationalarchives.preingestpaimporter

import cats.effect.IO
import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.networknt.schema.{InputFormat, SchemaRegistry, SpecificationVersion}
import fs2.*
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.*
import org.reactivestreams.{FlowAdapters, Publisher}
import pureconfig.ConfigReader
import uk.gov.nationalarchives.preingestpaimporter.Lambda.*
import uk.gov.nationalarchives.utils.EventCodecs.given
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.{DAS3Client, DASQSClient}

import java.net.URI
import java.nio.ByteBuffer
import java.util.UUID
import scala.jdk.CollectionConverters.*

class Lambda extends LambdaRunner[SQSEvent, List[Unit], Config, Dependencies]:

  override def handler: (SQSEvent, Config, Dependencies) => IO[List[Unit]] = (event, config, dependencies) => {

    def uploadMetadata(metadata: List[Data]): IO[Unit] = {
      val metadataBytes = metadata.asJson.noSpaces.getBytes
      Stream.emits(metadataBytes).chunks.map(_.toByteBuffer).toPublisherResource[IO, ByteBuffer].use { publisher =>
        dependencies.s3Client.upload(config.outputBucketName, s"${metadata.head.uuid}.metadata", FlowAdapters.toPublisher(publisher)) >> IO.unit
      }
    }

    event.getRecords.asScala.toList.traverse { record =>
      for
        body <- IO.fromEither(decode[Body](record.getBody))
        download <- dependencies.s3Client
          .download(body.metadataLocation.getHost, body.metadataLocation.getPath.drop(1))
        metadataString <- download.publisherToStream
          .flatMap(b => Stream.chunk(Chunk.byteBuffer(b)))
          .through(text.utf8.decode)
          .compile
          .toList
          .map(_.head)
        metadata <- IO.fromEither(decode[List[Data]](metadataString))
        modifiedMetadata <- metadata.parTraverse { data =>
          for
            _ <- validate(data)
            _ <- dependencies.s3Client.copy(body.bucket, s"${data.uuid}/${data.fileId}", config.outputBucketName, s"${data.uuid}/${data.fileId}")
          yield modifySeriesAndFileReference(data)
        }
        _ <- uploadMetadata(modifiedMetadata)
        _ <- dependencies.sqsClient.sendMessage(config.outputQueueUrl)(Message(metadata.head.uuid, s"s3://${config.outputBucketName}/${metadata.head.uuid}.metadata"))
      yield ()
    }
  }

  private def modifySeriesAndFileReference(data: Data): Data = {
    data.copy(series = modifyReference(data.series), fileReference = modifyReference(data.fileReference))
  }

  private def modifyReference(ref: String) = {
    val fieldElements = if ref.contains(" ") then ref.split(" ") else ref.split("/")
    val firstElement = fieldElements.head
    val modifiedFirstElement = if firstElement.length == 4 then firstElement.dropRight(1) else firstElement
    (s"Y$modifiedFirstElement" :: fieldElements.tail.toList).mkString("/")
  }

  private def validate(data: Data): IO[Unit] = {
    val schemaRegistry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
    val schema = schemaRegistry.getSchema(getClass.getResourceAsStream("/metadata-schema.json"))
    val res = schema.validate(data.asJson.noSpaces, InputFormat.JSON)
    IO.raiseWhen(res.size() > 0)(new RuntimeException(s"There are validation errors ${res.asScala.map(_.getMessage).mkString("\n")}"))
  }

  override def dependencies(config: Config): IO[Dependencies] = IO.pure {
    Dependencies(DAS3Client[IO](), DASQSClient[IO]())
  }

object Lambda:
  given Decoder[Body] = (c: HCursor) =>
    for
      location <- c.downField("metadataLocation").as[String]
      bucket <- c.downField("bucket").as[String]
      assetId <- c.downField("assetId").as[String]
    yield Body(URI.create(location), bucket, UUID.fromString(assetId))

  given Encoder[Message] = (message: Message) =>
    Json.fromJsonObject(
      JsonObject("id" -> Json.fromString(message.id.toString), "location" -> Json.fromString(message.location))
    )

  given Encoder[Data] = (data: Data) => {
    Json.fromJsonObject(
      JsonObject(
        "Series" -> Json.fromString(data.series),
        "UUID" -> Json.fromString(data.uuid.toString),
        "fileId" -> Json.fromString(data.fileId.toString),
        "description" -> data.description.map(Json.fromString).getOrElse(Json.Null),
        "Filename" -> Json.fromString(data.fileName),
        "FileReference" -> Json.fromString(data.fileReference),
        "digitalAssetSource" -> Json.fromString(data.digitalAssetSource),
        "ClientSideOriginalFilepath" -> Json.fromString(data.clientSideOriginalFilepath),
        "IAID" -> Json.fromString(data.iaid),
        "checksum_sha1" -> Json.fromString(data.checksum)
      )
    )
  }

  given Decoder[Data] = (c: HCursor) =>
    for
      series <- c.downField("Series").as[String]
      uuid <- c.downField("UUID").as[UUID]
      fileId <- c.downField("fileId").as[UUID]
      description <- c.downField("description").as[Option[String]]
      filename <- c.downField("Filename").as[String]
      fileReference <- c.downField("FileReference").as[String]
      clientSideOriginalFilepath <- c.downField("ClientSideOriginalFilepath").as[String]
      iaid <- c.downField("IAID").as[String]
      digitalAssetSource <- c.downField("digitalAssetSource").as[String]
      checksum <- c.downField("checksum_sha1").as[String]

    yield Data(
      series,
      uuid,
      fileId,
      description,
      filename,
      fileReference,
      clientSideOriginalFilepath,
      iaid,
      digitalAssetSource,
      checksum
    )

  case class Data(
      series: String,
      uuid: UUID,
      fileId: UUID,
      description: Option[String],
      fileName: String,
      fileReference: String,
      clientSideOriginalFilepath: String,
      iaid: String,
      digitalAssetSource: String,
      checksum: String
  )

  case class Body(metadataLocation: URI, bucket: String, assetId: UUID)

  case class Message(id: UUID, location: String)

  case class Config(outputBucketName: String, outputQueueUrl: String) derives ConfigReader

  case class Dependencies(s3Client: DAS3Client[IO], sqsClient: DASQSClient[IO])

  extension (publisher: Publisher[ByteBuffer])
    def publisherToStream: Stream[IO, ByteBuffer] = Stream.eval(IO.delay(publisher)).flatMap { publisher =>
      fs2.interop.flow.fromPublisher[IO](FlowAdapters.toFlowPublisher(publisher), chunkSize = 16)
    }
