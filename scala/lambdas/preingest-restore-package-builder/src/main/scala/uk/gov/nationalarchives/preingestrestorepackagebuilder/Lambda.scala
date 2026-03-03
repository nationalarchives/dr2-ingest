package uk.gov.nationalarchives.preingestrestorepackagebuilder

import cats.effect.IO
import cats.syntax.all.*
import io.circe
import io.circe.Decoder
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.scanamo.syntax.*
import pureconfig.ConfigReader
import uk.gov.nationalarchives.DADynamoDBClient.given
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{Checksum, IngestLockTableItem}
import uk.gov.nationalarchives.preingestrestorepackagebuilder.Lambda.*
import uk.gov.nationalarchives.utils.ExternalUtils.{*, given}
import uk.gov.nationalarchives.utils.{ExternalUtils, LambdaRunner}
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}

import java.net.URI
import java.util.UUID
import fs2.Stream
import fs2.interop.reactivestreams.*
import io.circe.fs2.{decoder as fs2Decoder, *}
import org.reactivestreams.FlowAdapters

import scala.xml.{Elem, Node, NodeSeq, XML}
import java.nio.ByteBuffer
import java.time.OffsetDateTime

class Lambda extends LambdaRunner[Input, Output, Config, Dependencies]:

  override def handler: (Input, Config, Dependencies) => IO[Output] = (input, config, dependencies) => {

    val contentFolderId = dependencies.uuidGenerator()

    def createFiles(metadata: Elem, assetId: UUID): IO[List[FileMetadataObject]] = {
      (metadata \ "CCContentObjects" \ "CCContentObject").zipWithIndex
        .traverse { case (co, sortOrder) =>
          for
            coRef <- co.pathValue("ContentObject", "Ref").map(UUID.fromString).liftF("Cannot get a Ref from a CO")
            fileId <- co.pathValue("Bitstreams", "Bitstream").flatMap(_.split("\\.").headOption).map(UUID.fromString).liftF("Cannot extract id from bitstream name")
            title <- co.pathValue("ContentObject", "Title").liftF(s"Title not found for file $fileId")
            fileSize <- co.pathValue("Bitstream", "FileSize").map(_.toInt).liftF(s"File size not found for file $fileId")
          yield FileMetadataObject(
            fileId,
            assetId.some,
            title,
            sortOrder + 1,
            title,
            fileSize,
            RepresentationType.Preservation,
            1,
            URI.create(s"s3://${config.rawCacheBucket}/$assetId/$coRef"),
            co.pathList("Bitstream", "Fixities", "Fixity").map(f => Checksum((f \ "FixityAlgorithmRef").text, (f \ "FixityValue").text))
          )
        }
        .map(_.toList)
    }

    def xmlToJson(metadata: Elem): Stream[IO, MetadataObject] = {
      def identifierByName(name: String) =
        (metadata \ "Identifier").find(e => (e \ "Type").text == name).map(e => (e \ "Value").text).liftF(s"Identifier $name not found")

      val contentFolder = ContentFolderMetadataObject(
        contentFolderId,
        None,
        None,
        "Restored records",
        Option("Unknown"),
        Nil
      )
      Stream.evals {
        for
          assetRef <- metadata.pathValue("InformationObject", "Ref").map(UUID.fromString).liftF("Preservation system Ref not found")
          assetId <- identifierByName("SourceID").map(UUID.fromString)
          title <- metadata.pathValue("InformationObject", "Title").liftF("Title not found")
          upstreamSystem <- metadata.metadataSource("UpstreamSystem").flatMap(SourceSystem.fromDisplayName).liftF("Source system not found")
          digitalAssetSource <- metadata.metadataSource("DigitalAssetSource").liftF("Asset source not found")
          files <- createFiles(metadata, assetId)
        yield
          val asset = AssetMetadataObject(
            assetId,
            contentFolderId.some,
            title,
            assetId.toString,
            metadata.pathList("Metadata", "Content", "Source", "OriginalMetadataFiles", "File").map(e => UUID.fromString(e.text)),
            metadata.pathValue("InformationObject", "Description"),
            metadata.metadataSource("TransferringBody"),
            metadata.metadataSource("TransferDateTime").map(OffsetDateTime.parse),
            upstreamSystem,
            digitalAssetSource,
            metadata.metadataSource("DigitalAssetSubtype"),
            metadata.metadataSource("UpstreamPath").getOrElse(""),
            None,
            (metadata \ "Identifier")
              .map(e => IdField((e \ "Type").text, (e \ "Value").text))
              .toList
              .filter(_.name != "SourceID")
          )
          List(contentFolder, asset) ++ files
      }
    }

    def generateMetadata(message: NotificationMessage): Stream[IO, MetadataObject] = {
      val bucket = message.location.getHost
      val key = message.location.getPath.drop(1)

      Stream
        .eval(dependencies.s3Client.download(bucket, key))
        .flatMap(_.toStreamBuffered[IO](1024 * 5))
        .map(bb => XML.loadString(bb.array().map(_.toChar).mkString))
        .flatMap(xmlToJson)

    }

    def processLockTableItems(lockTableItems: List[IngestLockTableItem]): IO[Unit] = {
      Stream
        .emits(lockTableItems)
        .map(_.message)
        .through(stringStreamParser[IO])
        .through(fs2Decoder[IO, NotificationMessage])
        .flatMap(generateMetadata)
        .compile
        .toList
        .flatMap { metadata =>
          IO.whenA(metadata.nonEmpty) {
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
      .map(_ => new Output(input.batchId, input.groupId, URI.create(s"s3://${config.rawCacheBucket}/${input.batchId}/metadata.json"), input.retryCount, ""))
  }

  override def dependencies(config: Config): IO[Dependencies] = IO(Dependencies(DADynamoDBClient[IO](), DAS3Client[IO](), () => UUID.randomUUID()))
object Lambda:
  case class Config(lockTableName: String, lockTableGsiName: String, rawCacheBucket: String) derives ConfigReader

  case class Dependencies(dynamoDbClient: DADynamoDBClient[IO], s3Client: DAS3Client[IO], uuidGenerator: () => UUID)

  case class Input(groupId: String, batchId: String, waitFor: Int, retryCount: Int = 0)

  private[preingestrestorepackagebuilder] type Output = StepFunctionInput

  extension [T](o: Option[T]) def liftF(msg: String): IO[T] = IO.fromOption(o)(new Exception(msg))

  extension (xml: NodeSeq)
    def pathList(path: String*): List[Node] = path.foldLeft(xml)(_ \ _).toList

    def metadataSource(path: String): Option[String] = pathValue("Metadata", "Content", "Source", path)

    def pathValue(path: String*): Option[String] = {
      val value = pathList(path*).headOption.map(_.text)
      if value.contains("") then None else value
    }
