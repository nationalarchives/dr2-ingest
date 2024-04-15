package uk.gov.nationalarchives.dp

import cats.effect.IO
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import fs2.{Chunk, text}
import fs2.interop.reactivestreams.PublisherOps
import org.reactivestreams.Publisher
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.dp.client.AdminClient
import uk.gov.nationalarchives.dp.client.FileInfo._
import cats.implicits._
import io.circe.{Decoder, HCursor}
import io.circe.parser.decode
import org.typelevel.log4cats.SelfAwareStructuredLogger

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.xml.XML

object FileProcessors {

  extension (s: String)
    def getName: String = s.split("/").last

    private def withoutFileExtension: String = s.getName.split("\\.").head

    private def documentType: String = s.split("/").head

  case class S3Objects(objects: List[S3Object])
  case class S3Object(bucket: String, key: String)

  given Decoder[S3Object] = (c: HCursor) =>
    for {
      key <- c.downField("s3").downField("object").downField("key").as[String]
      bucket <- c.downField("s3").downField("bucket").downField("name").as[String]
    } yield S3Object(bucket, key)

  given Decoder[S3Objects] = (c: HCursor) =>
    for {
      objects <- c.downField("Records").as[List[S3Object]]
    } yield S3Objects(objects)

  private[dp] def processSchemas(key: String, xmlData: String): SchemaFileInfo =
    SchemaFileInfo(key.withoutFileExtension, "", key.getName, xmlData)

  private[dp] def processTransforms(key: String, xmlData: String): TransformFileInfo = {
    val name = key.withoutFileExtension
    val fromNameSpace = XML.loadString(xmlData).getNamespace("tns")
    val purpose = if name.contains("view") then "view" else "edit"
    TransformFileInfo(name, fromNameSpace, "http://www.w3.org/1999/xhtml", purpose, key.getName, xmlData)
  }

  private[dp] def processIndexDefinitions(key: String, xmlData: String): IndexDefinitionInfo =
    IndexDefinitionInfo(key.withoutFileExtension, xmlData)

  private[dp] def processMetadataTemplates(key: String, xmlData: String): MetadataTemplateInfo =
    MetadataTemplateInfo(key.withoutFileExtension, xmlData)

  private[dp] def processFiles(
      client: AdminClient[IO],
      s3Client: DAS3Client[IO],
      s3Objects: List[S3Object]
  )(using logger: SelfAwareStructuredLogger[IO]): IO[List[Unit]] = {
    s3Objects
      .map(event => {
        val key = event.key
        for {
          publisher <- s3Client.download(event.bucket, key)
          xml <- xmlFromPublisher(publisher)
          _ <- logger.info(s"Adding or updating $key with document type ${key.documentType}")
          _ <- key.documentType match {
            case "schemas" =>
              val schemas = processSchemas(key, xml) :: Nil
              client.addOrUpdateSchemas(schemas)
            case "index_definitions" =>
              val indexDefinitions = processIndexDefinitions(key, xml) :: Nil
              client.addOrUpdateIndexDefinitions(indexDefinitions)
            case "metadata_templates" =>
              val metadataTemplates = processMetadataTemplates(key, xml) :: Nil
              client.addOrUpdateMetadataTemplates(metadataTemplates)
            case "transforms" =>
              val transforms = processTransforms(key, xml) :: Nil
              client.addOrUpdateTransforms(transforms)
          }
        } yield ()
      })
      .sequence
  }

  private def xmlFromPublisher(pub: Publisher[ByteBuffer]) = {
    pub
      .toStreamBuffered[IO](1024 * 1024)
      .map(f => Chunk.byteBuffer(f))
      .through(text.utf8.decodeC)
      .compile
      .fold(List[String]())((acc, str) => {
        str :: acc
      })
      .map(_.mkString("\n"))
  }

  private[dp] def s3ObjectsFromEvent(input: SQSEvent): IO[List[S3Object]] =
    input.getRecords.asScala
      .map(record => {
        for {
          _ <- IO.println(record.getBody)
          decoded <- IO.fromEither(decode[S3Objects](record.getBody))
        } yield decoded

      })
      .toList
      .sequence
      .map(_.flatMap(_.objects))
}
