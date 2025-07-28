package uk.gov.nationalarchives.preingesttdrpackagebuilder

import cats.effect.IO
import cats.effect.std.AtomicCell
import fs2.Collector.string
import fs2.Stream
import fs2.hashing.{HashAlgorithm, Hashing}
import fs2.interop.reactivestreams.*
import io.circe
import io.circe.fs2.{decoder as fs2Decoder, *}
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.{Decoder, HCursor}
import org.reactivestreams.FlowAdapters
import org.scanamo.syntax.*
import pureconfig.ConfigReader
import uk.gov.nationalarchives.DADynamoDBClient.given
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{Checksum, IngestLockTableItem}
import uk.gov.nationalarchives.preingesttdrpackagebuilder.Lambda.*
import uk.gov.nationalarchives.utils.ExternalUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.given
import uk.gov.nationalarchives.utils.ExternalUtils.RepresentationType.Preservation
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}

import java.net.URI
import java.nio.ByteBuffer
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID

class Lambda extends LambdaRunner[Input, Output, Config, Dependencies]:
  lazy private val bufferSize = 1024 * 5

  override def handler: (Input, Config, Dependencies) => IO[Output] = (input, config, dependencies) => {

    def processNonMetadataObjects(
        metadataArr: Array[Byte],
        fileLocation: URI,
        metadataId: UUID,
        potentialMessageId: Option[String],
        contentFolderCell: AtomicCell[IO, Map[String, ContentFolderMetadataObject]]
    ): IO[List[MetadataObject]] = {
      val jsonString = new String(metadataArr, "utf-8")
      IO.fromEither(decode[PackageMetadata](jsonString))
        .flatMap { packageMetadata =>
          val assetId = packageMetadata.UUID
          val fileId = packageMetadata.fileId.getOrElse(dependencies.uuidGenerator())
          val sourceSpecificIdentifiers = config.sourceSystem match {
            case SourceSystem.TDR => List(IdField("BornDigitalRef", packageMetadata.fileReference), IdField("UpstreamSystemReference", packageMetadata.fileReference))
            case SourceSystem.DRI =>
              List(IdField("UpstreamSystemReference", s"${packageMetadata.series}/${packageMetadata.fileReference}")) ++
                packageMetadata.driBatchReference.map(driBatchRef => List(IdField("DRIBatchReference", driBatchRef))).getOrElse(Nil)
            case _ => Nil
          }
          val assetMetadata = AssetMetadataObject(
            assetId,
            None,
            packageMetadata.filename,
            assetId.toString,
            List(fileId),
            List(metadataId),
            packageMetadata.description,
            packageMetadata.transferringBody,
            LocalDateTime.parse(packageMetadata.transferInitiatedDatetime.replace(" ", "T")).atOffset(ZoneOffset.UTC),
            config.sourceSystem,
            "Born Digital",
            None,
            packageMetadata.originalFilePath,
            potentialMessageId,
            List(
              IdField("Code", s"${packageMetadata.series}/${packageMetadata.fileReference}"),
              IdField("RecordID", assetId.toString)
            ) ++ sourceSpecificIdentifiers ++ packageMetadata.consignmentReference.map(consignmentRef => List(IdField("ConsignmentReference", consignmentRef))).getOrElse(Nil)
          )
          for {
            headObjectResponse <- dependencies.s3Client
              .headObject(fileLocation.getHost, fileLocation.getPath.drop(1))
            contentFolderKey <- IO.fromOption[String](packageMetadata.consignmentReference.orElse(packageMetadata.driBatchReference))(
              new Exception(s"We need either a consignment reference or DRI batch reference for $assetId")
            )
            res <- contentFolderCell.modify[List[MetadataObject]] { contentFolderMap =>
              val fileMetadata = FileMetadataObject(
                fileId,
                Option(assetId),
                packageMetadata.filename,
                1,
                packageMetadata.filename,
                headObjectResponse.contentLength(),
                Preservation,
                1,
                fileLocation,
                packageMetadata.checksums
              )
              val contentFolder = contentFolderMap.get(contentFolderKey)
              if contentFolder.isDefined then (contentFolderMap, List(assetMetadata.copy(parentId = contentFolder.map(_.id)), fileMetadata))
              else
                val contentFolderId = dependencies.uuidGenerator()
                val contentFolderMetadata = ContentFolderMetadataObject(contentFolderId, None, None, contentFolderKey, Option(packageMetadata.series), Nil)
                val updatedMap = contentFolderMap + (contentFolderKey -> contentFolderMetadata)
                val allMetadata = List(contentFolderMetadata, assetMetadata.copy(parentId = Option(contentFolderMetadata.id)), fileMetadata)
                (updatedMap, allMetadata)
            }
          } yield res
        }
    }

    def metadataSha256Fingerprint(metadataFileBytes: Array[Byte]) = Stream
      .emits(metadataFileBytes)
      .through(fs2.hashing.Hashing[IO].hash(HashAlgorithm.SHA256))
      .flatMap(hash => Stream.emits(hash.bytes.toList))
      .through(fs2.text.hex.encode)
      .compile
      .to(string)

    def processMetadataFiles(metadataArr: Array[Byte], fileLocation: URI, metadataId: UUID): IO[List[MetadataObject]] = {
      val metadataJsonString = new String(metadataArr, "utf-8")
      for {
        packageMetadata <- IO.fromEither(decode[PackageMetadata](metadataJsonString))
        sha256Fingerprint <- metadataSha256Fingerprint(metadataArr)
      } yield {
        val metadataFileSize = metadataArr.length
        val metadata = FileMetadataObject(
          metadataId,
          Option(packageMetadata.UUID),
          s"${packageMetadata.UUID}-metadata",
          2,
          s"${packageMetadata.UUID}-metadata.json",
          metadataFileSize,
          Preservation,
          1,
          getMetadataUri(fileLocation),
          List(Checksum("sha256", sha256Fingerprint))
        )
        List(metadata)
      }
    }

    def processPackageMetadata(
        metadataArr: Array[Byte],
        fileLocation: URI,
        potentialMessageId: Option[String],
        contentFolderCell: AtomicCell[IO, Map[String, ContentFolderMetadataObject]]
    ): IO[List[MetadataObject]] = {
      val metadataId = dependencies.uuidGenerator()
      for {
        nonMetadataObjects <- processNonMetadataObjects(metadataArr, fileLocation, metadataId, potentialMessageId, contentFolderCell)
        metadataFiles <- processMetadataFiles(metadataArr, fileLocation, metadataId)
      } yield nonMetadataObjects ++ metadataFiles
    }

    def downloadMetadataFile(lockTableMessage: LockTableMessage, contentFolderCell: AtomicCell[IO, Map[String, ContentFolderMetadataObject]]): IO[List[MetadataObject]] = {
      val fileLocation = lockTableMessage.location
      val metadataUri = getMetadataUri(fileLocation)
      val potentialMessageId = lockTableMessage.messageId
      dependencies.s3Client
        .download(metadataUri.getHost, metadataUri.getPath.drop(1))
        .map(_.toStreamBuffered[IO](bufferSize))
        .flatMap(_.compile.toList)
        .map(_.toArray.flatMap(_.array()))
        .flatMap { metadataArr =>
          processPackageMetadata(metadataArr, fileLocation, potentialMessageId, contentFolderCell)
        }
    }

    def processLockTableItems(lockTableItems: List[IngestLockTableItem]): IO[Unit] = {
      AtomicCell[IO].of[Map[String, ContentFolderMetadataObject]](Map()).flatMap { contentFolderCell =>
        Stream
          .emits(lockTableItems)
          .map(_.message)
          .through(stringStreamParser[IO])
          .through(fs2Decoder[IO, LockTableMessage])
          .parEvalMap[IO, List[MetadataObject]](config.concurrency)(lockTableMessage => downloadMetadataFile(lockTableMessage, contentFolderCell))
          .compile
          .toList
          .map(_.flatten)
          .flatMap { metadata =>
            IO.raiseWhen(metadata.isEmpty)(new Exception(s"Metadata list for ${input.groupId} is empty")) >> {
              val metadataBytes = metadata.asJson.noSpaces.getBytes
              Stream.emits(metadataBytes).chunks.map(_.toByteBuffer).toPublisherResource[IO, ByteBuffer].use { publisher =>
                dependencies.s3Client.upload(config.rawCacheBucket, s"${input.batchId}/metadata.json", FlowAdapters.toPublisher(publisher)) >> IO.unit
              }
            }
          }
      }
    }

    dependencies.dynamoDbClient
      .queryItems(config.lockTableName, "groupId" === input.groupId, Option(config.lockTableGsiName))
      .flatMap(processLockTableItems)
      .map(_ => new Output(input.batchId, input.groupId, URI.create(s"s3://${config.rawCacheBucket}/${input.batchId}/metadata.json"), input.retryCount, ""))
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

  given Decoder[PackageMetadata] = (c: HCursor) =>
    for {
      series <- c.downField("Series").as[String]
      uuid <- c.downField("UUID").as[UUID]
      fileId <- c.downField("fileId").as[Option[UUID]]
      description <- c.downField("description").as[Option[String]]
      transferringBody <- c.downField("TransferringBody").as[Option[String]]
      transferInitiatedDatetime <- c.downField("TransferInitiatedDatetime").as[String]
      consignmentReference <- c.downField("ConsignmentReference").as[Option[String]]
      fileName <- c.downField("Filename").as[String]
      checksums <- getChecksums(c)
      fileReference <- c.downField("FileReference").as[String]
      filePath <- c.downField("ClientSideOriginalFilepath").as[String]
      driBatchReference <- c.downField("driBatchReference").as[Option[String]]
    } yield PackageMetadata(
      series,
      uuid,
      fileId,
      description,
      transferringBody,
      transferInitiatedDatetime,
      consignmentReference,
      fileName,
      checksums,
      fileReference,
      filePath,
      driBatchReference
    )

  case class PackageMetadata(
      series: String,
      UUID: UUID,
      fileId: Option[UUID],
      description: Option[String],
      transferringBody: Option[String],
      transferInitiatedDatetime: String,
      consignmentReference: Option[String],
      filename: String,
      checksums: List[Checksum],
      fileReference: String,
      originalFilePath: String,
      driBatchReference: Option[String]
  )

  type LockTableMessage = NotificationMessage

  case class Config(lockTableName: String, lockTableGsiName: String, rawCacheBucket: String, concurrency: Int, sourceSystem: SourceSystem) derives ConfigReader

  case class Dependencies(dynamoDbClient: DADynamoDBClient[IO], s3Client: DAS3Client[IO], uuidGenerator: () => UUID)

  case class Input(groupId: String, batchId: String, waitFor: Int, retryCount: Int = 0)

  type Output = StepFunctionInput
