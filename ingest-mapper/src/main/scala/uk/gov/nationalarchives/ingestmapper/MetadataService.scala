package uk.gov.nationalarchives.ingestmapper

import cats.effect.IO
import cats.implicits.*
import fs2.interop.reactivestreams.*
import ujson.*
import fs2.{Chunk, Pipe, Stream, text}
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.ingestmapper.Lambda.Input
import uk.gov.nationalarchives.ingestmapper.MetadataService.*
import uk.gov.nationalarchives.ingestmapper.MetadataService.Type.{Asset, File}

import java.util.UUID
import scala.util.Try

class MetadataService(s3: DAS3Client[IO]) {
  lazy private val bufferSize = 1024 * 5

  private def getParentPaths(json: Value.Value): Map[UUID, String] = {
    val idToParentId = json.arr.toList.map { eachEntry =>
      UUID.fromString(eachEntry("id").str) -> eachEntry("parentId").strOpt.map(UUID.fromString)
    }.toMap

    def searchParentIds(parentIdOpt: Option[UUID]): String = {
      val parentId = parentIdOpt.map(_.toString).getOrElse("")
      val parentIdOfParent = parentIdOpt.flatMap(idToParentId.get).flatten
      if (parentIdOfParent.isEmpty) parentId else s"${searchParentIds(parentIdOfParent)}/$parentId"
    }
    idToParentId.map { case (id, parentIdOpt) =>
      id -> searchParentIds(parentIdOpt)
    }
  }

  private def addChildCountAttributes(metadataJson: List[Obj]): List[Obj] = {
    metadataJson.map { row =>
      val rowMap = row.value.toMap
      val parentPathOfChildren = s"${rowMap.get("parentPath").map(path => s"${path.str}/").getOrElse("")}${rowMap("id").str}"
      val childCount = metadataJson.count(row => row.value.toMap.get("parentPath").map(_.str).contains(parentPathOfChildren))
      Obj.from(("childCount", Num(childCount)) :: rowMap.toList)
    }
  }

  def parseMetadataJson(
      input: Input,
      departmentAndSeriesItems: DepartmentAndSeriesTableItems
  ): IO[List[Obj]] =
    parseFileFromS3(
      input,
      s =>
        s.flatMap { metadataJson =>
          val json = read(metadataJson)
          val departmentId = departmentAndSeriesItems.departmentItem("id").str
          val pathPrefix = departmentAndSeriesItems.potentialSeriesItem
            .map(series => s"$departmentId/${series("id").str}")
            .getOrElse(departmentId)
          val parentPaths = getParentPaths(json)

          Stream.emits {
            val updatedJson = json.arr.toList.map { metadataEntry =>
              val id = UUID.fromString(metadataEntry("id").str)
              val name = metadataEntry("name").str
              val parentPath = parentPaths(id)
              val path = if (parentPath.isEmpty) pathPrefix else s"$pathPrefix/${parentPath.stripPrefix("/")}"
              val checksum: Value = Try(metadataEntry("checksum_sha256")).toOption
                .map(_.str)
                .map(Str.apply)
                .getOrElse(Null)
              val entryType = metadataEntry("type").str
              val fileExtension =
                if (entryType == File.toString)
                  name.split('.').toList.reverse match {
                    case ext :: _ :: _ => Str(ext)
                    case _             => Null
                  }
                else Null
              val metadataMap: Map[String, Value] =
                Map("batchId" -> Str(input.batchId), "parentPath" -> Str(path), "checksum_sha256" -> checksum, "fileExtension" -> fileExtension) ++ metadataEntry.obj.view
                  .filterKeys(_ != "parentId")
                  .toMap

              Obj.from(metadataMap)
            } ++ departmentAndSeriesItems.potentialSeriesItem.toList ++ List(departmentAndSeriesItems.departmentItem)
            addChildCountAttributes(updatedJson)
          }
        }
    )

  private def parseFileFromS3[T](input: Input, decoderPipe: Pipe[IO, String, T]): IO[List[T]] =
    for {
      pub <- s3.download(input.packageMetadata.getHost, input.packageMetadata.getPath.drop(1))
      s3FileString <- pub
        .toStreamBuffered[IO](bufferSize)
        .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
        .through(text.utf8.decode)
        .through(decoderPipe)
        .compile
        .toList
    } yield s3FileString
}

object MetadataService {
  def typeFromString(typeString: String): Type = typeString match {
    case "ArchiveFolder" => Type.ArchiveFolder
    case "ContentFolder" => Type.ContentFolder
    case "Asset"         => Type.Asset
    case "File"          => Type.File
  }

  enum Type:
    override def toString: String = this match {
      case ArchiveFolder => "ArchiveFolder"
      case ContentFolder => "ContentFolder"
      case Asset         => "Asset"
      case File          => "File"
    }
    case ArchiveFolder, ContentFolder, Asset, File

  case class BagitManifestRow(checksum: String, filePath: String)

  case class DepartmentAndSeriesTableItems(departmentItem: Obj, potentialSeriesItem: Option[Obj]) {
    def show = s"Department: ${departmentItem.value.get("title").orNull} Series ${potentialSeriesItem.flatMap(_.value.get("title")).orNull}"
  }

  def apply(): MetadataService = {
    val s3 = DAS3Client[IO]()
    new MetadataService(s3)
  }
}
