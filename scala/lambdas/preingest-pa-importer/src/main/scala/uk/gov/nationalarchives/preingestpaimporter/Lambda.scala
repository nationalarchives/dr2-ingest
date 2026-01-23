package uk.gov.nationalarchives.preingestpaimporter

import cats.effect.IO
import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.networknt.schema.{InputFormat, SchemaRegistry, SpecificationVersion}
import io.circe.{Decoder, Encoder, HCursor, Json, JsonObject}
import io.circe.parser.decode
import io.circe.syntax.*
import org.reactivestreams.{FlowAdapters, Publisher}
import pureconfig.ConfigReader
import uk.gov.nationalarchives.{DAS3Client, DASQSClient}
import uk.gov.nationalarchives.preingestpaimporter.Lambda.*
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.utils.EventCodecs.given
import fs2.*
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import java.net.URI
import java.nio.ByteBuffer
import java.util.UUID
import scala.jdk.CollectionConverters.*

class Lambda extends LambdaRunner[SQSEvent, List[Unit], Config, Dependencies]:

  override def handler: (SQSEvent, Config, Dependencies) => IO[List[Unit]] = (event, config, dependencies) => {

    def uploadMetadata(metadata: List[Data]): IO[Unit] = {
      val metadataBytes = metadata.asJson.noSpaces.getBytes
      Stream.emits(metadataBytes).chunks.map(_.toByteBuffer).toPublisherResource[IO, ByteBuffer].use { publisher =>
        dependencies.dr2S3Client.upload(config.outputBucketName, s"${metadata.head.uuid}.metadata", FlowAdapters.toPublisher(publisher)) >> IO.unit
      }
    }

    event.getRecords.asScala.toList.traverse { record =>
      for
        body <- IO.fromEither(decode[Body](record.getBody))
        download <- dependencies.externalS3Client
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
            _ <- dependencies.externalS3Client.copy(config.filesBucket, data.fileId.toString, config.outputBucketName, s"${data.uuid}/${data.fileId}")
          yield modifySeriesAndFileReference(data)
        }
        _ <- uploadMetadata(modifiedMetadata)
        _ <- dependencies.sqsClient.sendMessage(config.outputQueueUrl)(Message(metadata.head.uuid, s"s3://${config.outputBucketName}/${metadata.head.uuid}.metadata"))
      yield ()
    }
  }

  private def createExternalS3Client(roleToAssume: String, sessionName: String): DAS3Client[IO] = {
    val stsClient = StsClient.builder
      .credentialsProvider(DefaultCredentialsProvider.builder.build)
      .build
    val assumeRoleRequest: AssumeRoleRequest = AssumeRoleRequest.builder
      .roleArn(roleToAssume)
      .roleSessionName(sessionName)
      .build

    val credentialsProvider = StsAssumeRoleCredentialsProvider.builder
      .stsClient(stsClient)
      .refreshRequest(assumeRoleRequest)
      .build

    val asyncClient = S3AsyncClient
      .crtBuilder()
      .region(Region.EU_WEST_2)
      .credentialsProvider(credentialsProvider)
      .crossRegionAccessEnabled(true)
      .targetThroughputInGbps(20.0)
      .minimumPartSizeInBytes(10 * 1024 * 1024)
      .build()

    DAS3Client[IO](asyncClient)

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
    Dependencies(createExternalS3Client(config.roleToAssume, "dr2-pa-importer"), DAS3Client[IO](), DASQSClient[IO]())
  }

object Lambda:
  given Decoder[Body] = (c: HCursor) =>
    for location <- c.downField("metadataLocation").as[String]
    yield Body(URI.create(location))

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
        "TransferInitiatedDatetime" -> Json.fromString(data.transferInitiatedDatetime),
        "Filename" -> Json.fromString(data.fileName),
        "FileReference" -> Json.fromString(data.fileReference),
        "metadata" -> Json.fromString(data.metadata),
        "digitalAssetSource" -> Json.fromString(data.digitalAssetSource),
        "ClientSideOriginalFilepath" -> Json.fromString(data.clientSideOriginalFilepath),
        "ConsignmentReference" -> Json.fromString(data.consignmentReference),
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
      transferInitiatedDatetime <- c.downField("TransferInitiatedDatetime").as[String]
      filename <- c.downField("Filename").as[String]
      fileReference <- c.downField("FileReference").as[String]
      metadata <- c.downField("metadata").as[String]
      digitalAssetSource <- c.downField("digitalAssetSource").as[String]
      clientSideOriginalFilepath <- c.downField("ClientSideOriginalFilepath").as[String]
      consignmentReference <- c.downField("ConsignmentReference").as[String]
      checksum <- c.downField("checksum_sha1").as[String]
      iaid <- c.downField("IAID").as[String]
    yield Data(
      series,
      uuid,
      fileId,
      description,
      transferInitiatedDatetime,
      filename,
      fileReference,
      metadata,
      digitalAssetSource,
      clientSideOriginalFilepath,
      consignmentReference,
      checksum,
      iaid
    )

  case class Data(
      series: String,
      uuid: UUID,
      fileId: UUID,
      description: Option[String],
      transferInitiatedDatetime: String,
      fileName: String,
      fileReference: String,
      metadata: String,
      digitalAssetSource: String,
      clientSideOriginalFilepath: String,
      consignmentReference: String,
      checksum: String,
      iaid: String
  )

  case class Body(metadataLocation: URI)

  case class Message(id: UUID, location: String)

  case class Config(outputBucketName: String, outputQueueUrl: String, roleToAssume: String, filesBucket: String) derives ConfigReader

  case class Dependencies(externalS3Client: DAS3Client[IO], dr2S3Client: DAS3Client[IO], sqsClient: DASQSClient[IO])

  extension (publisher: Publisher[ByteBuffer])
    def publisherToStream: Stream[IO, ByteBuffer] = Stream.eval(IO.delay(publisher)).flatMap { publisher =>
      fs2.interop.flow.fromPublisher[IO](FlowAdapters.toFlowPublisher(publisher), chunkSize = 16)
    }
