package uk.gov.nationalarchives.ingestmapper

import cats.effect.IO
import cats.implicits.*
import fs2.interop.reactivestreams.*
import fs2.{Chunk, Pipe, Stream, text}
import ujson.*
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.ingestmapper.Lambda.Input
import uk.gov.nationalarchives.ingestmapper.MetadataService.*
import uk.gov.nationalarchives.ingestmapper.MetadataService.Type.{Asset, File}

import java.util.UUID
import scala.util.Try

class MetadataService(s3: DAS3Client[IO], discoveryService: DiscoveryService) {
  lazy private val bufferSize = 1024 * 5

  private def getParentPaths(json: Value.Value, departmentSeriesMap: Map[UUID, DepartmentAndSeriesTableItems]): Map[UUID, String] = {
    val idToParentId = json.arr.toList.map { eachEntry =>
      eachEntry.id -> eachEntry.parentId
    }.toMap

    def getPathPrefix(id: UUID): String = departmentSeriesMap
      .get(id)
      .map { departmentAndSeriesItems =>
        val departmentId = departmentAndSeriesItems.departmentItem.id
        departmentAndSeriesItems.potentialSeriesItem
          .map(series => s"$departmentId/${series("id").str}")
          .getOrElse(departmentId.toString)
      }
      .getOrElse("")

    def searchParentIds(id: UUID): String = {
      val parentIdOpt = idToParentId.get(id).flatten
      val parentId = parentIdOpt.map(_.toString).getOrElse("")
      val pathPrefix = getPathPrefix(id)
      if parentIdOpt.isEmpty then s"$pathPrefix$parentId"
      else s"${searchParentIds(parentIdOpt.get)}/$parentId"
    }
    idToParentId.map { case (id, _) =>
      id -> searchParentIds(id)
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

  extension (value: Value)
    def parentId: Option[UUID] = value("parentId").strOpt.map(UUID.fromString)
    def series: Option[String] = Try(value("series")).toOption.map(_.str)
    def id: UUID = UUID.fromString(value("id").str)

  def parseMetadataJson(
      input: Input
  ): IO[List[Obj]] =
    parseFileFromS3(
      input,
      s =>
        s.flatMap { metadataJson =>
          val json = read(metadataJson)
          val jsonArr = json.arr.toList
          val topLevelObjects = jsonArr.filter(_.parentId.isEmpty)
          val topLevelItemToDepartmentSeries: IO[Map[UUID, DepartmentAndSeriesTableItems]] = topLevelObjects
            .groupBy(_.series)
            .view
            .mapValues(_.map(_.id))
            .toMap
            .map { case (series, parentIds) =>
              discoveryService.getDiscoveryCollectionAssets(series).map { assets =>
                parentIds.map(parentId => parentId -> discoveryService.getDepartmentAndSeriesItems(input.batchId, assets))
              }
            }
            .toList
            .sequence
            .map(_.flatten.toMap)
          Stream.evals {
            topLevelItemToDepartmentSeries.map { itemToDepartmentSeries =>
              val parentPaths = getParentPaths(json, itemToDepartmentSeries)
              val updatedJson = json.arr.toList.map { metadataEntry =>
                val id = UUID.fromString(metadataEntry("id").str)
                val name = metadataEntry("name").str
                val parentPath = parentPaths(id)
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
                  Map("batchId" -> Str(input.batchId), "parentPath" -> Str(parentPath), "checksum_sha256" -> checksum, "fileExtension" -> fileExtension) ++ metadataEntry.obj.view
                    .filterKeys(_ != "parentId")
                    .toMap
                Obj.from(metadataMap)
              } ++ itemToDepartmentSeries.values.toList.flatMap(_.potentialSeriesItem) ++ itemToDepartmentSeries.values.toList.map(_.departmentItem)
              addChildCountAttributes(updatedJson)
            }
          }

        }
    )

  private def parseFileFromS3[T](input: Input, decoderPipe: Pipe[IO, String, T]): IO[List[T]] =
    for {
      pub <- s3.download(input.metadataPackage.getHost, input.metadataPackage.getPath.drop(1))
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

  def apply(discoveryService: DiscoveryService): MetadataService = {
    val s3 = DAS3Client[IO]()
    new MetadataService(s3, discoveryService)
  }
}
