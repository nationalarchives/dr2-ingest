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
import software.amazon.awssdk.services.s3.model.S3Object
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
import java.nio.file.Path
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.jdk.CollectionConverters.*

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
      decodePackageMetadata(jsonString)
        .flatMap { packageMetadataList =>
          def createMetadataObjects(firstPackageMetadata: PackageMetadata, fileName: String, originalFilePath: String) = for {
            assetMetadata <- createAsset(firstPackageMetadata, fileName, originalFilePath, metadataId, potentialMessageId, packageMetadataList.map(_.fileId))
            s3FilesMap <- listS3Objects(fileLocation.getHost, assetMetadata.id)
            contentFolderKey <- IO.fromOption[String](firstPackageMetadata.consignmentReference.orElse(firstPackageMetadata.driBatchReference))(
              new Exception(s"We need either a consignment reference or DRI batch reference for ${assetMetadata.id}")
            )
            metadataObjects <- contentFolderCell.modify[List[MetadataObject]] { contentFolderMap =>
              val fileMetadataObjs: List[FileMetadataObject] = packageMetadataList.zipWithIndex.map { (packageMetadata, idx) =>
                val s3File = s3FilesMap(packageMetadata.fileId)
                FileMetadataObject(
                  packageMetadata.fileId,
                  Option(assetMetadata.id),
                  packageMetadata.filename,
                  packageMetadata.sortOrder.getOrElse(idx + 1),
                  packageMetadata.filename,
                  s3File.size(),
                  Preservation,
                  1,
                  URI.create(s"s3://${fileLocation.getHost}/${s3File.key()}"),
                  packageMetadata.checksums
                )
              }

              val potentialContentFolder = contentFolderMap.get(contentFolderKey)
              if potentialContentFolder.isDefined then (contentFolderMap, assetMetadata.copy(parentId = potentialContentFolder.map(_.id)) :: fileMetadataObjs)
              else
                val contentFolderId = dependencies.uuidGenerator()
                val contentFolderMetadata = ContentFolderMetadataObject(contentFolderId, None, None, contentFolderKey, Option(firstPackageMetadata.series), Nil)
                val updatedMap = contentFolderMap + (contentFolderKey -> contentFolderMetadata)
                val allMetadata = List(contentFolderMetadata, assetMetadata.copy(parentId = Option(contentFolderMetadata.id))) ++ fileMetadataObjs
                (updatedMap, allMetadata)
            }
          } yield metadataObjects

          packageMetadataList match {
            case head :: Nil  => createMetadataObjects(head, head.filename, head.originalFilePath)
            case head :: rest => createMetadataObjects(head, descriptionToFileName(head.description), getParentPath(head.originalFilePath))
            case Nil          => IO.raiseError(new Exception("The metadata list is empty"))
          }
        }
    }

    def metadataSha256Fingerprint(metadataFileBytes: Array[Byte]) = Stream
      .emits(metadataFileBytes)
      .through(fs2.hashing.Hashing[IO].hash(HashAlgorithm.SHA256))
      .flatMap(hash => Stream.emits(hash.bytes.toList))
      .through(fs2.text.hex.encode)
      .compile
      .to(string)

    def processMetadataFiles(metadataArr: Array[Byte], fileLocation: URI, metadataId: UUID): IO[MetadataObject] = {
      val metadataJsonString = new String(metadataArr, "utf-8")
      for {
        packageMetadataList <- decodePackageMetadata(metadataJsonString)
        sha256Fingerprint <- metadataSha256Fingerprint(metadataArr)
        firstPackageMetadata <- IO.fromOption(packageMetadataList.headOption)(new Exception("The metadata list is empty"))
      } yield {
        val metadataFileSize = metadataArr.length
        FileMetadataObject(
          metadataId,
          Option(firstPackageMetadata.UUID),
          s"${firstPackageMetadata.UUID}-metadata",
          packageMetadataList.flatMap(_.sortOrder).maxOption.getOrElse(packageMetadataList.length + 1),
          s"${firstPackageMetadata.UUID}-metadata.json",
          metadataFileSize,
          Preservation,
          1,
          fileLocation,
          List(Checksum("sha256", sha256Fingerprint))
        )
      }
    }

    def processPackageMetadata(
        metadataByteArr: Array[Byte],
        fileLocation: URI,
        potentialMessageId: Option[String],
        contentFolderCell: AtomicCell[IO, Map[String, ContentFolderMetadataObject]]
    ): IO[List[MetadataObject]] = {
      val metadataId = dependencies.uuidGenerator()
      for {
        nonMetadataObjects <- processNonMetadataObjects(metadataByteArr, fileLocation, metadataId, potentialMessageId, contentFolderCell)
        metadataFiles <- processMetadataFiles(metadataByteArr, fileLocation, metadataId)
      } yield metadataFiles :: nonMetadataObjects
    }

    def downloadMetadataFile(lockTableMessage: LockTableMessage, contentFolderCell: AtomicCell[IO, Map[String, ContentFolderMetadataObject]]): IO[List[MetadataObject]] = {
      val fileLocation = lockTableMessage.location
      val potentialMessageId = lockTableMessage.messageId
      dependencies.s3Client
        .download(fileLocation.getHost, fileLocation.getPath.drop(1))
        .map(_.toStreamBuffered[IO](bufferSize))
        .flatMap(_.compile.toList)
        .map(_.toArray.flatMap(_.array()))
        .flatMap { metadataByteArray =>
          processPackageMetadata(metadataByteArray, fileLocation, potentialMessageId, contentFolderCell)
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

    def listS3Objects(bucket: String, id: UUID): IO[Map[UUID, S3Object]] = {
      dependencies.s3Client.listObjects(bucket, Option(id.toString)).map { res =>
        res
          .contents()
          .asScala
          .toList
          .filterNot(_.key().endsWith(".metadata"))
          .map { s3Object =>
            UUID.fromString(s3Object.key.split("/").last) -> s3Object
          }
          .toMap
      }
    }

    def createAsset(
        packageMetadata: PackageMetadata,
        fileName: String,
        originalFilePath: String,
        metadataId: UUID,
        potentialMessageId: Option[String],
        originalFiles: List[UUID]
    ): IO[AssetMetadataObject] = IO.pure {
      val assetId = packageMetadata.UUID
      val sourceSpecificIdentifiers = config.sourceSystem match {
        case SourceSystem.TDR => List(IdField("BornDigitalRef", packageMetadata.fileReference), IdField("UpstreamSystemReference", packageMetadata.fileReference))
        case SourceSystem.DRI =>
          List(IdField("UpstreamSystemReference", s"${packageMetadata.series}/${packageMetadata.fileReference}")) ++
            packageMetadata.driBatchReference.map(driBatchRef => List(IdField("DRIBatchReference", driBatchRef))).getOrElse(Nil)
        case _ => Nil
      }
      val digitalAssetSource = packageMetadata.digitalAssetSource.getOrElse("Born Digital")
      AssetMetadataObject(
        assetId,
        None,
        fileName,
        assetId.toString,
        if digitalAssetSource == "Surrogate" then Nil else originalFiles,
        List(metadataId),
        packageMetadata.description,
        packageMetadata.transferringBody,
        LocalDateTime.parse(packageMetadata.transferInitiatedDatetime.replace(" ", "T")).atOffset(ZoneOffset.UTC),
        config.sourceSystem,
        digitalAssetSource,
        None,
        originalFilePath,
        potentialMessageId,
        List(
          IdField("Code", s"${packageMetadata.series}/${packageMetadata.fileReference}"),
          IdField("RecordID", assetId.toString)
        ) ++ sourceSpecificIdentifiers ++ packageMetadata.consignmentReference.map(consignmentRef => List(IdField("ConsignmentReference", consignmentRef))).getOrElse(Nil)
      )
    }

    dependencies.dynamoDbClient
      .queryItems(config.lockTableName, "groupId" === input.groupId, Option(config.lockTableGsiName))
      .flatMap(processLockTableItems)
      .map(_ => new Output(input.batchId, input.groupId, URI.create(s"s3://${config.rawCacheBucket}/${input.batchId}/metadata.json"), input.retryCount, ""))
  }

  private def descriptionToFileName(description: Option[String]) =
    description match
      case Some(value) => value.split(" ").slice(0, 14).mkString(" ") + "..."
      case None        => "Untitled"

  private def getParentPath(path: String) =
    Path.of(path).getParent.toString

  private def decodePackageMetadata(json: String): IO[List[PackageMetadata]] =
    IO.fromEither(decode[List[PackageMetadata]](json))

  override def dependencies(config: Config): IO[Dependencies] =
    IO(Dependencies(DADynamoDBClient[IO](), DAS3Client[IO](), () => UUID.randomUUID()))
end Lambda

object Lambda:

  given Decoder[PackageMetadata] = (c: HCursor) =>
    for
      series <- c.downField("Series").as[String]
      uuid <- c.downField("UUID").as[UUID]
      fileId <- c.downField("fileId").as[UUID]
      description <- c.downField("description").as[Option[String]]
      transferringBody <- c.downField("TransferringBody").as[Option[String]]
      transferInitiatedDatetime <- c.downField("TransferInitiatedDatetime").as[String]
      consignmentReference <- c.downField("ConsignmentReference").as[Option[String]]
      fileName <- c.downField("Filename").as[String]
      checksums <- getChecksums(c)
      fileReference <- c.downField("FileReference").as[String]
      filePath <- c.downField("ClientSideOriginalFilepath").as[String]
      driBatchReference <- c.downField("driBatchReference").as[Option[String]]
      sortOrder <- c.downField("sortOrder").as[Option[Int]]
      digitalAssetSource <- c.downField("digitalAssetSource").as[Option[String]]
    yield PackageMetadata(
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
      driBatchReference,
      sortOrder,
      digitalAssetSource
    )

  case class PackageMetadata(
      series: String,
      UUID: UUID,
      fileId: UUID,
      description: Option[String],
      transferringBody: Option[String],
      transferInitiatedDatetime: String,
      consignmentReference: Option[String],
      filename: String,
      checksums: List[Checksum],
      fileReference: String,
      originalFilePath: String,
      driBatchReference: Option[String],
      sortOrder: Option[Int],
      digitalAssetSource: Option[String]
  )

  type LockTableMessage = NotificationMessage

  case class Config(lockTableName: String, lockTableGsiName: String, rawCacheBucket: String, concurrency: Int, sourceSystem: SourceSystem) derives ConfigReader

  case class Dependencies(dynamoDbClient: DADynamoDBClient[IO], s3Client: DAS3Client[IO], uuidGenerator: () => UUID)

  case class Input(groupId: String, batchId: String, waitFor: Int, retryCount: Int = 0)

  type Output = StepFunctionInput
