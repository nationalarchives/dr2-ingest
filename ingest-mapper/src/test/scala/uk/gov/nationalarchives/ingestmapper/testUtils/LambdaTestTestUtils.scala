package uk.gov.nationalarchives.ingestmapper.testUtils

import cats.effect.IO
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import org.mockito.Mockito.*
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.TableDrivenPropertyChecks
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClient
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.SttpBackend
import uk.gov.nationalarchives.ingestmapper.DiscoveryService.{DepartmentAndSeriesCollectionAssets, DiscoveryCollectionAsset, DiscoveryScopeContent}
import uk.gov.nationalarchives.ingestmapper.Lambda.{Config, Dependencies, Input}
import uk.gov.nationalarchives.ingestmapper.testUtils.TestUtils.*
import uk.gov.nationalarchives.ingestmapper.{DiscoveryService, MetadataService}
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import upickle.default.write

import java.net.URI
import java.time.Instant
import java.util.UUID

class LambdaTestTestUtils(dynamoServer: WireMockServer, s3Server: WireMockServer, s3Prefix: String = "TEST/") extends TableDrivenPropertyChecks {
  val inputBucket = "input"
  val uuids: List[String] = List(
    "c7e6b27f-5778-4da8-9b83-1b64bbccbd03",
    "61ac0166-ccdf-48c4-800f-29e5fba2efda",
    "5364b309-aa11-4660-b518-f47b5b96a588",
    "0dcc4151-b1d0-44ac-a4a1-5415d7d50d65"
  )
  val config: Config = Config("test", "http://localhost:9015")
  val input: Input = Input("TEST", URI.create(s"s3://$inputBucket/${s3Prefix}metadata.json"))

  type NetworkResponse = (UUID, UUID, UUID, UUID, List[String], List[String])

  def stubValidNetworkRequests(dynamoTable: String = "test"): (NetworkResponse, NetworkResponse) = {
    val folderIdentifierOne = UUID.randomUUID()
    val folderIdentifierTwo = UUID.randomUUID()
    val assetIdentifierOne = UUID.randomUUID()
    val assetIdentifierTwo = UUID.randomUUID()
    val docxIdentifierOne = UUID.randomUUID()
    val docxIdentifierTwo = UUID.randomUUID()
    val metadataFileIdentifierOne = UUID.randomUUID()
    val metadataFileIdentifierTwo = UUID.randomUUID()
    val originalFilesOne = List(UUID.randomUUID(), UUID.randomUUID()).map(_.toString)
    val originalFilesTwo = List(UUID.randomUUID(), UUID.randomUUID()).map(_.toString)
    val originalMetadataFilesOne = List(UUID.randomUUID(), UUID.randomUUID()).map(_.toString)
    val originalMetadataFilesTwo = List(UUID.randomUUID(), UUID.randomUUID()).map(_.toString)

    val metadata =
      s"""[
         |{"id":"$folderIdentifierOne","parentId":null,"title":"TestTitle","type":"ArchiveFolder","name":"TestName","fileSize":null, "customMetadataAttribute2": "customMetadataValue2", "series": "A 1"},
         |{"id":"$folderIdentifierTwo","parentId":null,"title":"TestTitle","type":"ArchiveFolder","name":"TestName","fileSize":null, "customMetadataAttribute2": "customMetadataValue2", "series": "B 2"},
         |{
         | "id":"$assetIdentifierOne",
         | "parentId":"$folderIdentifierOne",
         | "title":"TestAssetTitle",
         | "type":"Asset",
         | "name":"TestAssetName",
         | "fileSize":null,
         | "originalFiles": ${write(originalFilesOne)},
         | "originalMetadataFiles": ${write(originalMetadataFilesOne)}
         |},
         |{
         | "id":"$assetIdentifierTwo",
         | "parentId":"$folderIdentifierTwo",
         | "title":"TestAssetTitle",
         | "type":"Asset",
         | "name":"TestAssetName",
         | "fileSize":null,
         | "originalFiles": ${write(originalFilesTwo)},
         | "originalMetadataFiles": ${write(originalMetadataFilesTwo)}
         |},
         |{"id":"$docxIdentifierOne","parentId":"$assetIdentifierOne","title":"Test","type":"File","name":"Test.docx","fileSize":1, "customMetadataAttribute1": "customMetadataValue1"},
         |{"id":"$metadataFileIdentifierOne","parentId":"$assetIdentifierOne","title":"","type":"File","name":"TEST-metadata.json","fileSize":2},
         |{"id":"$docxIdentifierTwo","parentId":"$assetIdentifierTwo","title":"Test","type":"File","name":"Test.docx","fileSize":1, "customMetadataAttribute1": "customMetadataValue1"},
         |{"id":"$metadataFileIdentifierTwo","parentId":"$assetIdentifierTwo","title":"","type":"File","name":"TEST-metadata.json","fileSize":2}]
         |""".stripMargin.replaceAll("\n", "")

    stubNetworkRequests(dynamoTable, metadata)
    (
      (folderIdentifierOne, assetIdentifierOne, docxIdentifierOne, metadataFileIdentifierOne, originalFilesOne, originalMetadataFilesOne),
      (folderIdentifierTwo, assetIdentifierTwo, docxIdentifierTwo, metadataFileIdentifierTwo, originalFilesTwo, originalMetadataFilesTwo)
    )
  }

  def stubInvalidNetworkRequests(dynamoTable: String = "test"): Unit = {
    val metadata: String = "{}"

    val manifestData: String = ""

    stubNetworkRequests(dynamoTable, metadata)
  }

  private def stubNetworkRequests(dynamoTableName: String = "test", metadata: String): Unit = {
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.RequestItems", containing(dynamoTableName)))
        .willReturn(ok())
    )

    List(
      ("metadata.json", metadata)
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

  def checkDynamoItems(tableRequestItems: List[DynamoTableItem], expectedTable: DynamoFilesTableItem): Unit = {
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
    num("ttl").get should equal(expectedTable.ttl)
    str("type") should equal(expectedTable.`type`.toString)
    strOpt("customMetadataAttribute1") should equal(expectedTable.customMetadataAttribute1)
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
    val randomUuidGenerator: () => UUID = () => UUID.fromString(uuidsIterator.next())
    val mockBackend = mock[SttpBackend[IO, Fs2Streams[IO]]]()
    val discoveryService = MockDiscoveryService(config.discoveryApiUrl, mockBackend, randomUuidGenerator, discoveryServiceException)
    val metadataService: MetadataService = new MetadataService(DAS3Client[IO](asyncS3Client), discoveryService)
    val dynamo: DADynamoDBClient[IO] = DADynamoDBClient[IO](asyncDynamoClient)
    val fixedTime = () => Instant.parse("2024-01-01T00:00:00.00Z")
    Dependencies(metadataService, dynamo, fixedTime)
  }

  case class MockDiscoveryService(discoveryApiUrl: String, backend: SttpBackend[IO, Fs2Streams[IO]], randomUuidGenerator: () => UUID, discoveryServiceException: Boolean)
      extends DiscoveryService(discoveryApiUrl, backend, randomUuidGenerator) {

    def generateDiscoveryCollectionAsset(col: String): DiscoveryCollectionAsset =
      DiscoveryCollectionAsset(col, DiscoveryScopeContent(s"TestDescription$col with 0"), s"Test Title $col")

    override def getDiscoveryCollectionAssets(series: Option[String]): IO[DepartmentAndSeriesCollectionAssets] =
      if discoveryServiceException then IO.raiseError(new Exception("Exception when sending request: GET http://localhost:9015/API/records/v1/collection/A"))
      else if series.isEmpty then IO.pure(DepartmentAndSeriesCollectionAssets(None, None))
      else
        val department = series.get.split(" ").head
        IO.pure(DepartmentAndSeriesCollectionAssets(Option(generateDiscoveryCollectionAsset(department)), Option(generateDiscoveryCollectionAsset(series.get))))
  }
}
