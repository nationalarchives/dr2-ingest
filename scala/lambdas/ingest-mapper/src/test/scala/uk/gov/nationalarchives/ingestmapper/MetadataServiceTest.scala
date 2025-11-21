package uk.gov.nationalarchives.ingestmapper

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.implicits.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import ujson.*
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.ingestmapper.DiscoveryService.{DepartmentAndSeriesCollectionAssets, DiscoveryCollectionAsset, DiscoveryScopeContent}
import uk.gov.nationalarchives.ingestmapper.Lambda.Input
import uk.gov.nationalarchives.ingestmapper.MetadataService.*
import uk.gov.nationalarchives.ingestmapper.MetadataService.Type.*
import uk.gov.nationalarchives.ingestmapper.testUtils.LambdaTestTestUtils.*
import uk.gov.nationalarchives.ingestmapper.testUtils.TestUtils.*

import java.net.URI
import java.util.UUID

class MetadataServiceTest extends AnyFlatSpec with TableDrivenPropertyChecks {
  case class Test(name: String, value: String)

  val testCsvWithHeaders = "name,value\ntestName1,testValue1\ntestName2,testValue2"
  val testCsvWithoutHeaders = "testName1 testValue1\ntestName2 testValue2"

  val invalidTestCsvWithHeaders = "invalid,header\ninvalidName,invalidValue"
  val invalidTestCsvWithoutHeaders = "invalidValue"

  def mockS3(responseText: String): DAS3Client[IO] = {
    val ref = Ref.unsafe[IO, List[S3Object]](List(S3Object("bucket", "prefix/metadata.json", responseText)))
    mocks3Client(ref)
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
      item("childCount").num.toInt should equal(expectedTableItem.childCount)
      item.value.get("description").map(_.str).getOrElse("") should equal(expectedTableItem.description)
      item.value.get("fileSize").flatMap(_.numOpt).map(_.toLong) should equal(expectedTableItem.fileSize)
      item("type").str should equal(expectedTableItem.`type`.toString)
      item.value.get("checksumSha256").map(_.str) should equal(expectedTableItem.checksumSha256)
      item.value.get("fileExtension").flatMap(_.strOpt) should equal(expectedTableItem.fileExtension)
      item.value.get("customMetadataAttribute1").flatMap(_.strOpt) should equal(expectedTableItem.customMetadataAttribute1)
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
                "batchId" -> "groupId_0",
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
          val groupId = "groupId"
          val batchId = s"${groupId}_0"
          val folderIdOne = UUID.randomUUID()
          val assetIdOne = UUID.randomUUID()
          val fileIdOne = UUID.randomUUID()
          val metadataFileOne = UUID.randomUUID()
          val folderIdTwo = UUID.randomUUID()
          val assetIdTwo = UUID.randomUUID()
          val fileIdTwo = UUID.randomUUID()
          val metadataFileTwo = UUID.randomUUID()
          val departmentTableItem = tableItem(departmentId, "department", "")
          val seriesTableItem = seriesIdOpt.map(id => tableItem(id, "series", departmentId.toString))
          val departmentAndSeries = DepartmentAndSeriesTableItems(departmentTableItem, seriesTableItem)

          val expectedTimeInSecs = 1712707200
          val metadata =
            s"""[
           |{"id":"$folderIdTwo","parentId":null,"title":"TestTitle2","type":"ArchiveFolder","name":"TestName2","fileSize":null, "series": null},
           |{"id":"$assetIdTwo","parentId":"$folderIdTwo","title":"TestAssetTitle2","type":"Asset","name":"TestAssetName2","fileSize":null, "originalMetadataFiles": ["$metadataFileTwo"], "customMetadataAttribute1": "customMetadataAttributeValue"},
           |{"id":"$fileIdTwo","parentId":"$assetIdTwo","title":"Test2","type":"File","name":"$name","fileSize":1, "checksumSha256": "$name-checksum"},
           |{"id":"$metadataFileTwo","parentId":"$assetIdTwo","title":"","type":"File","name":"TEST2-metadata.json","fileSize":2, "checksumSha256": "metadata-checksum"},
           |{"id":"$folderIdOne","parentId":null,"title":"TestTitle","type":"ArchiveFolder","name":"TestName","fileSize":null, "series": null},
           |{"id":"$assetIdOne","parentId":"$folderIdOne","title":"TestAssetTitle","type":"Asset","name":"TestAssetName","fileSize":null, "originalMetadataFiles": ["$metadataFileOne"], "customMetadataAttribute1": "customMetadataAttributeValue"},
           |{"id":"$fileIdOne","parentId":"$assetIdOne","title":"Test","type":"File","name":"$name","fileSize":1, "checksumSha256": "$name-checksum"},
           |{"id":"$metadataFileOne","parentId":"$assetIdOne","title":"","type":"File","name":"TEST-metadata.json","fileSize":2, "checksumSha256": "metadata-checksum"}
           |]
           |""".stripMargin.replaceAll("\n", "")
          val s3 = mockS3(metadata)
          val input = Input(groupId, batchId, URI.create("s3://bucket/prefix/metadata.json"), "executionName")

          def createCollectionAsset(obj: Obj) =
            Option(DiscoveryCollectionAsset(obj("name").str, DiscoveryScopeContent(obj("description").strOpt), obj("title").strOpt))

          val discoveryService: DiscoveryService[IO] = new DiscoveryService[IO]:
            override def getDepartmentAndSeriesItems(batchId: String, departmentAndSeriesAssets: DepartmentAndSeriesCollectionAssets): DepartmentAndSeriesTableItems =
              departmentAndSeries

            override def getDiscoveryCollectionAssets(potentialSeries: Option[String]): IO[DepartmentAndSeriesCollectionAssets] =
              IO(DepartmentAndSeriesCollectionAssets(createCollectionAsset(departmentTableItem), seriesTableItem.flatMap(createCollectionAsset)))

          val result =
            new MetadataService(s3, discoveryService).parseMetadataJson(input).unsafeRunSync()

          result.size should equal(9 + seriesIdOpt.size)

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
              DynamoFilesTableItem(batchId, seriesId, departmentId.toString, "series", ArchiveFolder, "series Title", "series Description", Some("series"), 2, expectedTimeInSecs)
            )
          )
          checkTableItems(result, List(folderIdOne), DynamoFilesTableItem(batchId, folderIdOne, prefix, "TestName", ArchiveFolder, "TestTitle", "", None, 1, expectedTimeInSecs))
          checkTableItems(
            result,
            List(assetIdOne),
            DynamoFilesTableItem(
              batchId,
              assetIdOne,
              s"$prefix/$folderIdOne",
              "TestAssetName",
              Asset,
              "TestAssetTitle",
              "",
              None,
              2,
              expectedTimeInSecs,
              customMetadataAttribute1 = Option("customMetadataAttributeValue"),
              originalMetadataFiles = List(metadataFileOne.toString)
            )
          )
          checkTableItems(
            result,
            List(fileIdOne),
            DynamoFilesTableItem(
              batchId,
              assetIdOne,
              s"$prefix/$folderIdOne/$assetIdOne",
              name,
              File,
              "Test",
              "",
              Some(name),
              0,
              expectedTimeInSecs,
              Option(1),
              Option(s"$name-checksum"),
              expectedExt
            )
          )
          checkTableItems(
            result,
            List(metadataFileOne),
            DynamoFilesTableItem(
              batchId,
              assetIdOne,
              s"$prefix/$folderIdOne/$assetIdOne",
              "TEST-metadata.json",
              File,
              "",
              "",
              Some("TEST-metadata.json"),
              0,
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
