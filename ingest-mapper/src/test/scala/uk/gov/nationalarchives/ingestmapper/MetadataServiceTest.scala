package uk.gov.nationalarchives.ingestmapper

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.*
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import org.scalatestplus.mockito.MockitoSugar
import reactor.core.publisher.Flux
import ujson.*
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.ingestmapper.Lambda.Input
import uk.gov.nationalarchives.ingestmapper.MetadataService.*
import uk.gov.nationalarchives.ingestmapper.MetadataService.Type.*
import uk.gov.nationalarchives.ingestmapper.testUtils.TestUtils.*

import java.nio.ByteBuffer
import java.util.UUID

class MetadataServiceTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {
  case class Test(name: String, value: String)

  val testCsvWithHeaders = "name,value\ntestName1,testValue1\ntestName2,testValue2"
  val testCsvWithoutHeaders = "testName1 testValue1\ntestName2 testValue2"

  val invalidTestCsvWithHeaders = "invalid,header\ninvalidName,invalidValue"
  val invalidTestCsvWithoutHeaders = "invalidValue"

  def mockS3(responseText: String, name: String, returnError: Boolean = false): DAS3Client[IO] = {
    val s3 = mock[DAS3Client[IO]]
    val stub = when(s3.download(ArgumentMatchers.eq("bucket"), ArgumentMatchers.eq(s"prefix/$name")))
    if (returnError)
      stub.thenThrow(new Exception("Key not found"))
    else
      stub.thenReturn(IO(Flux.just(ByteBuffer.wrap(responseText.getBytes))))
    s3
  }

  private def checkTableRows(result: List[Obj], ids: List[UUID], expectedTable: DynamoTable) = {
    val rows = result.filter(r => ids.contains(UUID.fromString(r("id").str)))
    rows.size should equal(1)
    rows.map { row =>
      ids.contains(UUID.fromString(row("id").str)) should be(true)
      row("name").str should equal(expectedTable.name)
      row("title").str should equal(expectedTable.title)
      row("parentPath").str should equal(expectedTable.parentPath)
      row("batchId").str should equal(expectedTable.batchId)
      row.value.get("description").map(_.str).getOrElse("") should equal(expectedTable.description)
      row.value.get("fileSize").flatMap(_.numOpt).map(_.toLong) should equal(expectedTable.fileSize)
      row("type").str should equal(expectedTable.`type`.toString)
      row.value.get("checksumSha256").map(_.str) should equal(expectedTable.checksumSha256)
      row.value.get("fileExtension").flatMap(_.strOpt) should equal(expectedTable.fileExtension)
      row("ttl").num.toLong should equal(expectedTable.ttl)
      row.value.get("customMetadataAttribute1").flatMap(_.strOpt) should equal(expectedTable.customMetadataAttribute1)
      row.value.get("originalFiles").map(_.arr.toList).getOrElse(Nil).map(_.str) should equal(expectedTable.originalFiles)
      row.value.get("originalMetadataFiles").map(_.arr.toList).getOrElse(Nil).map(_.str) should equal(expectedTable.originalMetadataFiles)
    }
  }

  s"parseBagManifest" should "return the correct row values" in {
    val input = Input("testBatch", "bucket", "prefix/", Option("T"), Option("T TEST"))
    val id = UUID.randomUUID()
    val validBagitManifestRow = s"checksum $id"
    val s3 = mockS3(validBagitManifestRow, "manifest-sha256.txt")
    val result: List[BagitManifestRow] = new MetadataService(s3).parseBagManifest(input).unsafeRunSync()

    result.size should equal(1)
    result.head.checksum should equal("checksum")
    result.head.filePath should equal(id.toString)
  }

  s"parseBagManifest" should "return an error if there is only one column" in {
    val input = Input("testBatch", "bucket", "prefix/", Option("T"), Option("T TEST"))
    val invalidBagitManifestRow = "onlyOneColumn"
    val s3 = mockS3(invalidBagitManifestRow, "manifest-sha256.txt")

    val ex = intercept[Exception] {
      new MetadataService(s3).parseBagManifest(input).unsafeRunSync()
    }

    ex.getMessage should equal("Expecting 2 columns in manifest-sha256.txt, found 1")
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
          def table(id: UUID, tableType: String, parentPath: String) =
            Obj.from {
              Map(
                "batchId" -> "batchId",
                "id" -> id.toString,
                "parentPath" -> parentPath,
                "name" -> tableType,
                "type" -> "ArchiveFolder",
                "title" -> s"$tableType Title",
                "description" -> s"$tableType Description",
                "ttl" -> 1712707200
              )
            }

          val batchId = "batchId"
          val folderId = UUID.randomUUID()
          val assetId = UUID.randomUUID()
          val fileIdOne = UUID.randomUUID()
          val fileIdTwo = UUID.randomUUID()
          val departmentTable = table(departmentId, "department", "")
          val seriesTable = seriesIdOpt.map(id => table(id, "series", departmentId.toString))
          val departmentAndSeries = DepartmentAndSeriesTableData(departmentTable, seriesTable)

          val expectedTimeInSecs = 1712707200
          val originalFileId = UUID.randomUUID()
          val originalMetadataFileId = UUID.randomUUID()
          val metadata =
            s"""[{"id":"$folderId","parentId":null,"title":"TestTitle","type":"ArchiveFolder","name":"TestName","fileSize":null},
           |{"id":"$assetId","parentId":"$folderId","title":"TestAssetTitle","type":"Asset","name":"TestAssetName","fileSize":null, "originalFiles" : ["$originalFileId"], "originalMetadataFiles": ["$originalMetadataFileId"]},
           |{"id":"$fileIdOne","parentId":"$assetId","title":"Test","type":"File","name":"$name","fileSize":1, "checksumSha256": "$name-checksum"},
           |{"id":"$fileIdTwo","parentId":"$assetId","title":"","type":"File","name":"TEST-metadata.json","fileSize":2, "checksumSha256": "metadata-checksum"}]
           |""".stripMargin.replaceAll("\n", "")
          val bagitManifests: List[BagitManifestRow] = List(BagitManifestRow("checksum-docx", fileIdOne.toString), BagitManifestRow("checksum-metadata", fileIdTwo.toString))
          val s3 = mockS3(metadata, "metadata.json")
          val bagInfoJson = Obj(("customMetadataAttribute1", Value(Str("customMetadataAttributeValue"))))
          val input = Input(batchId, "bucket", "prefix/", Option("department"), Option("series"))
          val result =
            new MetadataService(s3).parseMetadataJson(input, departmentAndSeries, bagitManifests, bagInfoJson, Num(1712707200)).unsafeRunSync()

          result.size should equal(5 + seriesIdOpt.size)

          val prefix = s"$departmentId${seriesIdOpt.map(id => s"/$id").getOrElse("")}"
          checkTableRows(
            result,
            List(departmentId),
            DynamoTable(batchId, departmentId, "", "department", ArchiveFolder, "department Title", "department Description", Some("department"), 1, expectedTimeInSecs)
          )
          seriesIdOpt.map(seriesId =>
            checkTableRows(
              result,
              List(seriesId),
              DynamoTable(batchId, seriesId, departmentId.toString, "series", ArchiveFolder, "series Title", "series Description", Some("series"), 1, expectedTimeInSecs)
            )
          )
          checkTableRows(result, List(folderId), DynamoTable(batchId, folderId, prefix, "TestName", ArchiveFolder, "TestTitle", "", None, 1, expectedTimeInSecs))
          checkTableRows(
            result,
            List(assetId),
            DynamoTable(
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
          checkTableRows(
            result,
            List(fileIdOne),
            DynamoTable(
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
          checkTableRows(
            result,
            List(fileIdTwo),
            DynamoTable(
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
