package uk.gov.nationalarchives

import cats.effect.IO
import cats.implicits._
import fs2.interop.reactivestreams._
import fs2.{Chunk, Pipe, Stream, text}
import uk.gov.nationalarchives.Lambda.Input
import uk.gov.nationalarchives.MetadataService._
import ujson._

import java.util.UUID

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

  def parseBagInfoJson(input: Input): IO[List[Obj]] = parseFileFromS3(input, "bag-info.json", _.map(bagInfoJson => Obj.from(read(bagInfoJson).obj)))

  def parseMetadataJson(input: Input, departmentAndSeries: DepartmentAndSeriesTableData, bagitManifests: List[BagitManifestRow], bagInfoJson: Obj): IO[List[Obj]] = {
    parseFileFromS3(
      input,
      "metadata.json",
      s =>
        s.flatMap { metadataJson =>
          val fileIdToChecksum: Map[UUID, String] = bagitManifests.map(bm => UUID.fromString(bm.filePath.stripPrefix("data/")) -> bm.checksum).toMap
          val json = read(metadataJson)
          val pathPrefix = departmentAndSeries.series
            .map(series => s"${departmentAndSeries.department("id").str}/${series("id").str}")
            .getOrElse(s"${departmentAndSeries.department("id").str}")
          val parentPaths = getParentPaths(json)
          Stream.emits {
            json.arr.toList.map { metadataEntry =>
              val id = UUID.fromString(metadataEntry("id").str)
              val name = metadataEntry("name").strOpt
              val parentPath = parentPaths(id)
              val path = if (parentPath.isEmpty) pathPrefix else s"$pathPrefix/${parentPath.stripPrefix("/")}"
              val checksum = fileIdToChecksum.get(id).map(Str).getOrElse(Null)
              val fileExtension =
                if (metadataEntry("type").str == "File")
                  name
                    .flatMap(n => n.split("\\.").lastOption)
                    .map(Str)
                    .getOrElse(Null)
                else Null
              val metadataFromBagInfo: Obj = if (metadataEntry("type").str == "Asset") bagInfoJson else Obj()
              val metadataMap =
                Map("batchId" -> Str(input.batchId), "parentPath" -> Str(path), "checksum_sha256" -> checksum, "fileExtension" -> fileExtension) ++ metadataEntry.obj.view
                  .filterKeys(_ != "parentId")
                  .toMap
              Obj.from(metadataFromBagInfo.value ++ metadataMap)
            } ++ departmentAndSeries.series.toList ++ List(departmentAndSeries.department)
          }
        }
    )
  }

  def parseBagManifest(input: Input): IO[List[BagitManifestRow]] = {
    parseFileFromS3(
      input,
      "manifest-sha256.txt",
      _.flatMap { bagitManifestString =>
        Stream.evalSeq {
          bagitManifestString
            .split("\n")
            .map { rowAsString =>
              val rowAsArray = rowAsString.split(" ")
              if (rowAsArray.size != 2) {
                IO.raiseError(new Exception(s"Expecting 2 columns in manifest-sha256.txt, found ${rowAsArray.size}"))
              } else {
                IO(BagitManifestRow(rowAsArray.head, rowAsArray.last))
              }
            }
            .toList
            .sequence
        }
      }
    )
  }

  private def parseFileFromS3[T](input: Input, name: String, decoderPipe: Pipe[IO, String, T]): IO[List[T]] = {
    for {
      pub <- s3.download(input.s3Bucket, s"${input.s3Prefix}$name")
      s3FileString <- pub
        .toStreamBuffered[IO](bufferSize)
        .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
        .through(text.utf8.decode)
        .through(decoderPipe)
        .compile
        .toList
    } yield s3FileString
  }
}
object MetadataService {
  def typeFromString(typeString: String): Type = typeString match {
    case "ArchiveFolder" => ArchiveFolder
    case "ContentFolder" => ContentFolder
    case "Asset"         => Asset
    case "File"          => File
  }

  sealed trait Type {
    override def toString: String = this match {
      case ArchiveFolder => "ArchiveFolder"
      case ContentFolder => "ContentFolder"
      case Asset         => "Asset"
      case File          => "File"
    }
  }
  case object ArchiveFolder extends Type
  case object ContentFolder extends Type
  case object Asset extends Type
  case object File extends Type

  sealed trait Metadata {
    def id: UUID
    def parentPath: String
    def title: String
  }

  case class BagitManifestRow(checksum: String, filePath: String)

  case class DepartmentAndSeriesTableData(department: Obj, series: Option[Obj]) {
    def show = s"Department: ${department.value.get("title").orNull} Series ${series.flatMap(_.value.get("title")).orNull}"
  }

  def apply(): MetadataService = {
    val s3 = DAS3Client[IO]()
    new MetadataService(s3)
  }
}
