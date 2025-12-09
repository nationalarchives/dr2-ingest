package uk.gov.nationalarchives.ingestmapper

import cats.effect.IO
import cats.implicits.*
import fs2.interop.reactivestreams.*
import fs2.{Chunk, Stream, text}
import ujson.*
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.ingestmapper.Lambda.Input
import uk.gov.nationalarchives.ingestmapper.MetadataService.*
import uk.gov.nationalarchives.ingestmapper.MetadataService.Type.File

import java.util.UUID
import scala.util.Try

class MetadataService(s3: DAS3Client[IO], discoveryService: DiscoveryService[IO]) {
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
    def series: Option[String] = value("series").strOpt
    def id: UUID = UUID.fromString(value("id").str)

  def parseMetadataJson(
      input: Input
  ): IO[List[Obj]] = {
    metadataFromS3(input).flatMap { metadataJson =>
      val json = read(metadataJson)
      val jsonArr = json.arr.toList
      val topLevelObjects = jsonArr.filter(_.parentId.isEmpty)
      val seriesToParentIds = topLevelObjects
        .groupBy(_.series)
        .view
        .mapValues(_.map(_.id))

      def departmentFromSeries(series: String) = (if series.contains("/") then series.split("/") else series.split(" ")).headOption

      val allSeries = seriesToParentIds.keys.toList.flatten

      val allDepartmentSeries = for
        allSeriesAssets <- allSeries.traverse(discoveryService.getAssetFromDiscoveryApi)
        allDepartmentAssets <- allSeries.flatMap(departmentFromSeries).distinct.traverse(discoveryService.getAssetFromDiscoveryApi)
      yield
        lazy val unknownDep = DepartmentAndSeriesTableItems(discoveryService.departmentItem(input.batchId, None), None)
        allDepartmentAssets.flatMap { dep =>
          val depJson = discoveryService.departmentItem(input.batchId, Option(dep))
          if dep.citableReference == "Unknown" then List(Option("Unknown") -> unknownDep)
          else
            allSeriesAssets
              .filter(s => departmentFromSeries(s.citableReference).contains(dep.citableReference))
              .map { s =>
                val json = discoveryService.seriesItem(input.batchId, depJson, s)
                Option(json("name").str) -> DepartmentAndSeriesTableItems(depJson, Option(json))
              }
        }.toMap + (None -> unknownDep)

      val topLevelIdsToDepartmentSeries = allDepartmentSeries.map { lookup =>
        seriesToParentIds.toMap.flatMap { case (potentialSeries, parentIds) =>
          parentIds.map(p => p -> lookup(potentialSeries))
        }
      }

      topLevelIdsToDepartmentSeries.map { idToDepartmentSeries =>
        val idToParent = getParentPaths(json, idToDepartmentSeries)
        val depSeriesCount = idToDepartmentSeries.values.toList
          .groupBy(_.departmentItem("id").str)
          .view
          .mapValues(_.distinct.length)
          .toMap
        val parentToChildCount = idToParent.groupBy(_._2).view.mapValues(_.size).toMap ++ depSeriesCount

        def addChildCount(obj: Obj) = {
          val attributes = obj.value.toMap
          val id = attributes("id").str
          val parentPath = attributes.get("parentPath").map(parentPath => s"${parentPath.str}/").getOrElse("")
          val childParentPath = s"$parentPath$id"
          val childCount = parentToChildCount.getOrElse(childParentPath, 1)
          Obj.from(attributes + ("childCount" -> Num(childCount)))
        }

        val departmentSeriesObjects = idToDepartmentSeries.values.toList
          .distinctBy(item => item.potentialSeriesItem.map(obj => obj("name")))
          .flatMap { departmentSeries =>
            List(
              departmentSeries.potentialSeriesItem.map(addChildCount),
              Option(addChildCount(departmentSeries.departmentItem))
            ).flatten
          }
          .distinct
        jsonArr.map { metadataEntry =>
          val id = UUID.fromString(metadataEntry("id").str)
          val name = metadataEntry("name").str
          val parentPath = idToParent(id)
          val checksum: Value = Try(metadataEntry("checksum_sha256")).toOption
            .map(_.str)
            .map(Str.apply)
            .getOrElse(Null)
          val entryType = metadataEntry("type").str
          val fileExtension =
            if (entryType == File.toString)
              name.split('.').toList.reverse match {
                case ext :: _ :: _ => Str(ext)
                case _ => Null
              }
            else Null
          val childCount = Num(parentToChildCount.getOrElse(s"$parentPath/$id", 0))
          val metadataMap: Map[String, Value] =
            Map(
              "batchId" -> Str(input.batchId),
              "parentPath" -> Str(parentPath),
              "checksum_sha256" -> checksum,
              "fileExtension" -> fileExtension,
              "childCount" -> childCount
            ) ++ metadataEntry.obj.view
              .filterKeys(_ != "parentId")
              .toMap
          Obj.from(metadataMap)
        } ++ departmentSeriesObjects
      }
    }

  }

  private def metadataFromS3[T](input: Input): IO[String] =
    for {
      pub <- s3.download(input.metadataPackage.getHost, input.metadataPackage.getPath.drop(1))
      s3FileString <- pub
        .toStreamBuffered[IO](bufferSize)
        .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
        .through(text.utf8.decode)
        .compile
        .toList
    } yield s3FileString.mkString
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

  def apply(discoveryService: DiscoveryService[IO]): MetadataService = {
    val s3 = DAS3Client[IO]()
    new MetadataService(s3, discoveryService)
  }
}
