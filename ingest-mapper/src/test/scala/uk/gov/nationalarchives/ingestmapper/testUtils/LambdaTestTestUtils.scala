package uk.gov.nationalarchives.ingestmapper.testUtils

import cats.effect.IO
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import org.mockito.Mockito._
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.TableDrivenPropertyChecks
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClient
import sttp.client3.SttpBackend
import ujson.{Obj, Str}
import uk.gov.nationalarchives.ingestmapper.Lambda.{Config, Dependencies, Input}
import uk.gov.nationalarchives.ingestmapper.MetadataService.DepartmentAndSeriesTableData
import uk.gov.nationalarchives.ingestmapper.testUtils.TestUtils.{DynamoLRequestField, DynamoNRequestField, DynamoSRequestField, DynamoTable, DynamoTableItem}
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.ingestmapper.{DiscoveryService, MetadataService}
import upickle.default.write

import java.net.URI
import java.util.UUID

class LambdaTestTestUtils(dynamoServer: WireMockServer, s3Server: WireMockServer, s3Prefix: String = "TEST/") extends TableDrivenPropertyChecks {
  val inputBucket = "input"
  val uuids: List[String] = List(
    "c7e6b27f-5778-4da8-9b83-1b64bbccbd03",
    "61ac0166-ccdf-48c4-800f-29e5fba2efda"
  )
  val config: Config = Config("test", "http://localhost:9015")
  val input: Input = Input("TEST", inputBucket, s3Prefix, Option("A"), Option("A 1"))

  def stubValidNetworkRequests(dynamoTable: String = "test"): (UUID, UUID, UUID, UUID, List[String], List[String]) = {
    val folderIdentifier = UUID.randomUUID()
    val assetIdentifier = UUID.randomUUID()
    val docxIdentifier = UUID.randomUUID()
    val metadataFileIdentifier = UUID.randomUUID()
    val originalFiles = List(UUID.randomUUID(), UUID.randomUUID()).map(_.toString)
    val originalMetadataFiles = List(UUID.randomUUID(), UUID.randomUUID()).map(_.toString)

    val metadata =
      s"""[{"id":"$folderIdentifier","parentId":null,"title":"TestTitle","type":"ArchiveFolder","name":"TestName","fileSize":null, "customMetadataAttribute2": "customMetadataValue2"},
         |{
         | "id":"$assetIdentifier",
         | "parentId":"$folderIdentifier",
         | "title":"TestAssetTitle",
         | "type":"Asset",
         | "name":"TestAssetName",
         | "fileSize":null,
         | "originalFiles": ${write(originalFiles)},
         | "originalMetadataFiles": ${write(originalMetadataFiles)}
         |},
         |{"id":"$docxIdentifier","parentId":"$assetIdentifier","title":"Test","type":"File","name":"Test.docx","fileSize":1, "customMetadataAttribute1": "customMetadataValue1"},
         |{"id":"$metadataFileIdentifier","parentId":"$assetIdentifier","title":"","type":"File","name":"TEST-metadata.json","fileSize":2}]
         |""".stripMargin.replaceAll("\n", "")

    val bagInfoMetadata =
      """{"customMetadataAttribute2": "customMetadataValueFromBagInfo","attributeUniqueToBagInfo": "bagInfoAttributeValue"}"""

    val manifestData: String =
      s"""checksumdocx data/$docxIdentifier
         |checksummetadata data/$metadataFileIdentifier
         |""".stripMargin

    stubNetworkRequests(dynamoTable, metadata, manifestData, bagInfoMetadata)
    (folderIdentifier, assetIdentifier, docxIdentifier, metadataFileIdentifier, originalFiles, originalMetadataFiles)
  }

  def stubInvalidNetworkRequests(dynamoTable: String = "test"): Unit = {
    val metadata: String = "{}"

    val manifestData: String = ""

    stubNetworkRequests(dynamoTable, metadata, manifestData, "{}")
  }

  private def stubNetworkRequests(dynamoTableName: String = "test", metadata: String, manifestData: String, bagInfoMetadata: String): Unit = {
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.RequestItems", containing(dynamoTableName)))
        .willReturn(ok())
    )

    List(
      ("metadata.json", metadata),
      ("bag-info.json", bagInfoMetadata),
      ("manifest-sha256.txt", manifestData)
    ).map { case (name, responseCsv) =>
      s3Server.stubFor(
        head(urlEqualTo(s"/TEST/$name"))
          .willReturn(
            ok()
              .withHeader("Content-Length", responseCsv.getBytes.length.toString)
              .withHeader("ETag", "abcde")
          )
      )
      s3Server.stubFor(
        get(urlEqualTo(s"/TEST/$name"))
          .willReturn(ok.withBody(responseCsv.getBytes))
      )
    }
  }

  def checkDynamoItems(tableRequestItems: List[DynamoTableItem], expectedTable: DynamoTable): Assertion = {
    val items = tableRequestItems
      .filter(_.PutRequest.Item.items("id").asInstanceOf[DynamoSRequestField].S == expectedTable.id.toString)
      .map(_.PutRequest.Item)
    items.size should equal(1)
    val dynamoFieldItems = items.head.items
    def list(name: String): List[String] = dynamoFieldItems
      .get(name)
      .map(_.asInstanceOf[DynamoLRequestField].L)
      .getOrElse(Nil)
    def num(name: String) = dynamoFieldItems.get(name).map(_.asInstanceOf[DynamoNRequestField].N)
    def strOpt(name: String) = dynamoFieldItems.get(name).map(_.asInstanceOf[DynamoSRequestField].S)
    def str(name: String) = strOpt(name).getOrElse("")
    str("id") should equal(expectedTable.id.toString)
    str("name") should equal(expectedTable.name)
    str("title") should equal(expectedTable.title)
    expectedTable.id_Code.map(id_Code => str("id_Code") should equal(id_Code))
    str("parentPath") should equal(expectedTable.parentPath)
    str("batchId") should equal(expectedTable.batchId)
    str("description") should equal(expectedTable.description)
    num("childCount").get should equal(expectedTable.childCount)
    num("fileSize") should equal(expectedTable.fileSize)
    str("type") should equal(expectedTable.`type`.toString)
    strOpt("customMetadataAttribute1") should equal(expectedTable.customMetadataAttribute1)
    strOpt("customMetadataAttribute2") should equal(expectedTable.customMetadataAttribute2)
    strOpt("attributeUniqueToBagInfo") should equal(expectedTable.attributeUniqueToBagInfo)
    list("originalFiles") should equal(expectedTable.originalFiles)
    list("originalMetadataFiles") should equal(expectedTable.originalMetadataFiles)
  }

  def dependencies(discoveryServiceException: Boolean = false): Dependencies = {
    val creds: StaticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
    val asyncS3Client: S3AsyncClient = S3AsyncClient
      .crtBuilder()
      .endpointOverride(URI.create("http://localhost:9008"))
      .credentialsProvider(creds)
      .region(Region.EU_WEST_2)
      .build()
    val asyncDynamoClient: DynamoDbAsyncClient = DynamoDbAsyncClient
      .builder()
      .endpointOverride(URI.create("http://localhost:9009"))
      .region(Region.EU_WEST_2)
      .credentialsProvider(creds)
      .build()
    val uuidsIterator: Iterator[String] = uuids.iterator
    val metadataService: MetadataService = new MetadataService(DAS3Client[IO](asyncS3Client))
    val dynamo: DADynamoDBClient[IO] = new DADynamoDBClient[IO](asyncDynamoClient)
    val randomUuidGenerator: () => UUID = () => UUID.fromString(uuidsIterator.next())
    val mockBackend = mock[SttpBackend[IO, Fs2Streams[IO]]]()
    val discoveryService = MockDiscoveryService(config.discoveryApiUrl, mockBackend, randomUuidGenerator, discoveryServiceException)
    Dependencies(metadataService, dynamo, IO.pure(discoveryService))
  }

  case class MockDiscoveryService(discoveryApiUrl: String, backend: SttpBackend[IO, Fs2Streams[IO]], randomUuidGenerator: () => UUID, discoveryServiceException: Boolean)
      extends DiscoveryService(discoveryApiUrl, backend, randomUuidGenerator) {
    private def generateJsonMap(col: String) = Map(
      "batchId" -> Str(input.batchId),
      "id" -> Str(randomUuidGenerator().toString),
      "name" -> Str(s"$col"),
      "type" -> Str("ArchiveFolder"),
      "title" -> Str(s"Test Title $col"),
      "description" -> Str(s"TestDescription$col with 0")
    )

    private val departmentJsonMap = generateJsonMap("A")
    private val departmentTableData = departmentJsonMap ++ Map("id_Code" -> departmentJsonMap("name"))
    private val seriesJsonMap = generateJsonMap("A 1")
    private val seriesTableData = seriesJsonMap ++ Map("parentPath" -> departmentJsonMap("id"), "id_Code" -> seriesJsonMap("name"))

    override def getDepartmentAndSeriesRows(input: Input): IO[DepartmentAndSeriesTableData] =
      if (discoveryServiceException) IO.raiseError(new Exception("Exception when sending request: GET http://localhost:9015/API/records/v1/collection/A"))
      else IO.pure(DepartmentAndSeriesTableData(Obj.from(departmentTableData), Some(Obj.from(seriesTableData))))
  }
}
