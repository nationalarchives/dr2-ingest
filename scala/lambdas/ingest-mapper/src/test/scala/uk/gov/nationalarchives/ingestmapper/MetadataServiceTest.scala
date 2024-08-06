package uk.gov.nationalarchives.ingestmapper

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.*
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import org.scalatestplus.mockito.MockitoSugar
import reactor.core.publisher.Flux
import ujson.*
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.ingestmapper.DiscoveryService.{DepartmentAndSeriesCollectionAssets, DiscoveryCollectionAsset, DiscoveryScopeContent}
import uk.gov.nationalarchives.ingestmapper.Lambda.Input
import uk.gov.nationalarchives.ingestmapper.MetadataService.*
import uk.gov.nationalarchives.ingestmapper.MetadataService.Type.*
import uk.gov.nationalarchives.ingestmapper.testUtils.TestUtils.*

import java.net.URI
import java.nio.ByteBuffer
import java.util.UUID

class MetadataServiceTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {
  case class Test(name: String, value: String)

  val testCsvWithHeaders = "name,value\ntestName1,testValue1\ntestName2,testValue2"
  val testCsvWithoutHeaders = "testName1 testValue1\ntestName2 testValue2"

  val invalidTestCsvWithHeaders = "invalid,header\ninvalidName,invalidValue"
  val invalidTestCsvWithoutHeaders = "invalidValue"

  def mockS3(responseText: String): DAS3Client[IO] = {
    val s3 = mock[DAS3Client[IO]]
    when(s3.download(ArgumentMatchers.eq("bucket"), ArgumentMatchers.eq("prefix/metadata.json")))
      .thenReturn(IO(Flux.just(ByteBuffer.wrap(responseText.getBytes))))
    s3
  }

  private def checkTableItems(result: List[Obj], ids: List[UUID], expectedTableItem: DynamoFilesTableItem) = {
    val items = result.filter(i => ids.contains(UUID.fromString(i("id").str)))
    items.size should equal(1)
    items.map { item =>
      ids.contains(UUID.fromString(item("id").str)) should be(true)
      item("name").str should equal(expectedTableItem.name)
      item("title").str should equal(expectedTableItem.title)
      item("parentPath").str should equal(expectedTableItem.parentPath)
      item("batchId").str should equal(expectedTableItem.batchId)
      item.value.get("description").map(_.str).getOrElse("") should equal(expectedTableItem.description)
      item.value.get("fileSize").flatMap(_.numOpt).map(_.toLong) should equal(expectedTableItem.fileSize)
      item("type").str should equal(expectedTableItem.`type`.toString)
      item.value.get("checksumSha256").map(_.str) should equal(expectedTableItem.checksumSha256)
      item.value.get("fileExtension").flatMap(_.strOpt) should equal(expectedTableItem.fileExtension)
      item.value.get("customMetadataAttribute1").flatMap(_.strOpt) should equal(expectedTableItem.customMetadataAttribute1)
      item.value.get("originalFiles").map(_.arr.toList).getOrElse(Nil).map(_.str) should equal(expectedTableItem.originalFiles)
      item.value.get("originalMetadataFiles").map(_.arr.toList).getOrElse(Nil).map(_.str) should equal(expectedTableItem.originalMetadataFiles)
    }
  }

  val departmentSeriesTable: TableFor2[UUID, Option[UUID]] = Table(
    ("departmentId", "seriesId"),
    (UUID.randomUUID(), Option(UUID.randomUUID())),
    (UUID.randomUUID(), None)
  )
  val fileNameStates: TableFor2[String, Option[String]] = Table(
    ("fileName", "expectedExtension"),
    ("", None),
    ("nameWithoutExtension", None),
    ("name.txt", Some("txt")),
    ("name.with.dot.txt", Some("txt"))
  )

  forAll(fileNameStates) { (name, expectedExt) =>
    forAll(departmentSeriesTable) { (departmentId, seriesIdOpt) =>
      "parseMetadataJson" should s"return a list of tables with the correct prefix for department $departmentId and series " +
        s"${seriesIdOpt.getOrElse("None")} and a 'fileExtension' of ${expectedExt.getOrElse("None")} if file name is $name" in {
          def tableItem(id: UUID, tableType: String, parentPath: String) =
            Obj.from {
              Map(
                "batchId" -> "batchId",
                "id" -> id.toString,
                "parentPath" -> parentPath,
                "name" -> tableType,
                "type" -> "ArchiveFolder",
                "title" -> s"$tableType Title",
                "description" -> s"$tableType Description",
                "ttl" -> 1712707200,
                "series" -> seriesIdOpt.map(_.toString).getOrElse("Unknown")
              )
            }

          val batchId = "batchId"
          val folderId = UUID.randomUUID()
          val assetId = UUID.randomUUID()
          val fileIdOne = UUID.randomUUID()
          val fileIdTwo = UUID.randomUUID()
          val departmentTableItem = tableItem(departmentId, "department", "")
          val seriesTableItem = seriesIdOpt.map(id => tableItem(id, "series", departmentId.toString))
          val departmentAndSeries = DepartmentAndSeriesTableItems(departmentTableItem, seriesTableItem)

          val expectedTimeInSecs = 1712707200
          val originalFileId = UUID.randomUUID()
          val originalMetadataFileId = UUID.randomUUID()
          val metadata =
            s"""[{"id":"$folderId","parentId":null,"title":"TestTitle","type":"ArchiveFolder","name":"TestName","fileSize":null, "series": null},
           |{"id":"$assetId","parentId":"$folderId","title":"TestAssetTitle","type":"Asset","name":"TestAssetName","fileSize":null, "originalFiles" : ["$originalFileId"], "originalMetadataFiles": ["$originalMetadataFileId"], "customMetadataAttribute1": "customMetadataAttributeValue"},
           |{"id":"$fileIdOne","parentId":"$assetId","title":"Test","type":"File","name":"$name","fileSize":1, "checksumSha256": "$name-checksum"},
           |{"id":"$fileIdTwo","parentId":"$assetId","title":"","type":"File","name":"TEST-metadata.json","fileSize":2, "checksumSha256": "metadata-checksum"}]
           |""".stripMargin.replaceAll("\n", "")
          val s3 = mockS3(metadata)
          val input = Input(batchId, URI.create("s3://bucket/prefix/metadata.json"))
          val discoveryService = mock[DiscoveryService]
          def createCollectionAsset(obj: Obj) =
            Option(DiscoveryCollectionAsset(obj("name").str, DiscoveryScopeContent(obj("description").str), obj("title").str))
          when(discoveryService.getDepartmentAndSeriesItems(any[String], any[DepartmentAndSeriesCollectionAssets]))
            .thenReturn(departmentAndSeries)
          when(discoveryService.getDiscoveryCollectionAssets(any[Option[String]]))
            .thenReturn(IO(DepartmentAndSeriesCollectionAssets(createCollectionAsset(departmentTableItem), seriesTableItem.flatMap(createCollectionAsset))))
          val result =
            new MetadataService(s3, discoveryService).parseMetadataJson(input).unsafeRunSync()

          result.size should equal(5 + seriesIdOpt.size)

          val prefix = s"$departmentId${seriesIdOpt.map(id => s"/$id").getOrElse("")}"
          checkTableItems(
            result,
            List(departmentId),
            DynamoFilesTableItem(batchId, departmentId, "", "department", ArchiveFolder, "department Title", "department Description", Some("department"), 1, expectedTimeInSecs)
          )
          seriesIdOpt.map(seriesId =>
            checkTableItems(
              result,
              List(seriesId),
              DynamoFilesTableItem(batchId, seriesId, departmentId.toString, "series", ArchiveFolder, "series Title", "series Description", Some("series"), 1, expectedTimeInSecs)
            )
          )
          checkTableItems(result, List(folderId), DynamoFilesTableItem(batchId, folderId, prefix, "TestName", ArchiveFolder, "TestTitle", "", None, 1, expectedTimeInSecs))
          checkTableItems(
            result,
            List(assetId),
            DynamoFilesTableItem(
              batchId,
              assetId,
              s"$prefix/$folderId",
              "TestAssetName",
              Asset,
              "TestAssetTitle",
              "",
              None,
              1,
              expectedTimeInSecs,
              customMetadataAttribute1 = Option("customMetadataAttributeValue"),
              originalFiles = List(originalFileId.toString),
              originalMetadataFiles = List(originalMetadataFileId.toString)
            )
          )
          checkTableItems(
            result,
            List(fileIdOne),
            DynamoFilesTableItem(
              batchId,
              assetId,
              s"$prefix/$folderId/$assetId",
              name,
              File,
              "Test",
              "",
              Some(name),
              1,
              expectedTimeInSecs,
              Option(1),
              Option(s"$name-checksum"),
              expectedExt
            )
          )
          checkTableItems(
            result,
            List(fileIdTwo),
            DynamoFilesTableItem(
              batchId,
              assetId,
              s"$prefix/$folderId/$assetId",
              "TEST-metadata.json",
              File,
              "",
              "",
              Some("TEST-metadata.json"),
              1,
              expectedTimeInSecs,
              Option(2),
              Option(s"metadata-checksum"),
              Option("json")
            )
          )
        }
    }
  }
}
