package uk.gov.nationalarchives.ingestassetopexcreator.testUtils

import cats.effect.IO
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import org.apache.commons.io.output.ByteArrayOutputStream
import org.scalatest.prop.TableDrivenPropertyChecks
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClient
import uk.gov.nationalarchives.ingestassetopexcreator.Lambda.{Dependencies, Input}
import uk.gov.nationalarchives.ingestassetopexcreator.XMLCreator
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID
import scala.xml.Elem

class ExternalServicesTestUtils(dynamoServer: WireMockServer, s3Server: WireMockServer) extends TableDrivenPropertyChecks {

  def stubPutRequest(): (String, String) = {
    val bucket = "test-destination-bucket"

    val xipPath = s"/opex/$executionName/$assetParentPath/$assetId.pax/$assetId.xip"
    val opexPath = s"/opex/$executionName/$assetParentPath/$assetId.pax.opex"
    List(xipPath, opexPath).foreach { itemPath =>
      val postResponse = <InitiateMultipartUploadResult>
        <Bucket>{bucket}</Bucket>
        <Key>{itemPath}</Key>
        <UploadId>id</UploadId>
      </InitiateMultipartUploadResult>.toString
      s3Server.stubFor(
        post(urlEqualTo(s"$itemPath?uploads"))
          .withHost(equalTo(s"$bucket.localhost"))
          .willReturn(okXml(postResponse))
      )
      s3Server.stubFor(
        post(urlPathEqualTo(itemPath))
          .withQueryParam("uploadId", equalTo("id"))
          .withHost(equalTo(s"$bucket.localhost"))
          .willReturn(okXml(postResponse))
      )
      s3Server.stubFor(
        delete(urlPathEqualTo(itemPath))
          .withQueryParam("uploadId", equalTo("id"))
          .withHost(equalTo(s"$bucket.localhost"))
          .willReturn(ok())
      )
      s3Server.stubFor(
        put(urlPathEqualTo(itemPath))
          .withQueryParam("uploadId", equalTo("id"))
          .withHost(equalTo(s"$bucket.localhost"))
          .willReturn(ok().withHeader("ETag", "ETag"))
      )
      s3Server.stubFor(
        head(urlEqualTo(s"/$itemPath"))
          .willReturn(ok())
      )
    }
    (xipPath, opexPath)
  }

  def stubJsonCopyRequest(): (String, String) = stubCopyRequest(childIdJson, "json")

  def stubDocxCopyRequest(): (String, String) = stubCopyRequest(childIdDocx, "docx")

  def stubCopyRequest(childId: UUID, suffix: String): (String, String) = {
    val sourceName = s"/$batchId/$childId"
    val destinationName = s"/opex/$executionName/$assetParentPath/$assetId.pax/Representation_Preservation/$childId/Generation_1/$childId.$suffix"
    val response =
      <CopyObjectResult>
        <LastModified>2023-08-29T17:50:30.000Z</LastModified>
        <ETag>"9b2cf535f27731c974343645a3985328"</ETag>
      </CopyObjectResult>
    s3Server.stubFor(
      head(urlEqualTo(destinationName))
        .willReturn(ok().withHeader("Content-Length", "1"))
    )
    s3Server.stubFor(
      head(urlEqualTo(sourceName))
        .willReturn(ok().withHeader("Content-Length", "1"))
    )
    s3Server.stubFor(
      put(urlEqualTo(destinationName))
        .withHost(equalTo("test-destination-bucket.localhost"))
        .withHeader("x-amz-copy-source", equalTo(s"test-source-bucket$sourceName"))
        .willReturn(okXml(response.toString()))
    )
    (sourceName, destinationName)
  }

  def stubGetRequest(batchGetResponse: String): Unit =
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.RequestItems", containing("test-table")))
        .willReturn(ok().withBody(batchGetResponse))
    )

  def stubPostRequest(postResponse: String): Unit =
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.TableName", equalTo("test-table")))
        .willReturn(ok().withBody(postResponse))
    )

  val expectedOpex: Elem =
    <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.2">
      <opex:Transfer>
        <opex:SourceID>Test Name</opex:SourceID>
        <opex:Manifest>
          <opex:Files>
            <opex:File type="metadata" size="3052">68b1c80b-36b8-4f0f-94d6-92589002d87e.xip</opex:File>
            <opex:File type="content" size="1">Representation_Preservation/a25d33f3-7726-4fb3-8e6f-f66358451c4e/Generation_1/a25d33f3-7726-4fb3-8e6f-f66358451c4e.docx</opex:File>
            <opex:File type="content" size="2">Representation_Preservation/feedd76d-e368-45c8-96e3-c37671476793/Generation_1/feedd76d-e368-45c8-96e3-c37671476793.json</opex:File>
          </opex:Files>
          <opex:Folders>
            <opex:Folder>Representation_Preservation</opex:Folder>
            <opex:Folder>Representation_Preservation/a25d33f3-7726-4fb3-8e6f-f66358451c4e</opex:Folder>
            <opex:Folder>Representation_Preservation/a25d33f3-7726-4fb3-8e6f-f66358451c4e/Generation_1</opex:Folder>
            <opex:Folder>Representation_Preservation/feedd76d-e368-45c8-96e3-c37671476793</opex:Folder>
            <opex:Folder>Representation_Preservation/feedd76d-e368-45c8-96e3-c37671476793/Generation_1</opex:Folder>
          </opex:Folders>
        </opex:Manifest>
      </opex:Transfer>
      <opex:Properties>
        <opex:Title>Test Name</opex:Title>
        <opex:Description/>
        <opex:SecurityDescriptor>open</opex:SecurityDescriptor>
        <opex:Identifiers>
          <opex:Identifier type="UpstreamSystemReference">UpstreamSystemReference</opex:Identifier>
          <opex:Identifier type="Code">Code</opex:Identifier>
        </opex:Identifiers>
      </opex:Properties>
      <opex:DescriptiveMetadata>
        <Source xmlns="http://dr2.nationalarchives.gov.uk/source">
          <DigitalAssetSource>Test Digital Asset Source</DigitalAssetSource>
          <DigitalAssetSubtype>Test Digital Asset Subtype</DigitalAssetSubtype>
          <IngestDateTime>2023-09-01T00:00Z</IngestDateTime>
          <OriginalFiles>
            <File>b6102810-53e3-43a2-9f69-fafe71d4aa40</File>
          </OriginalFiles>
          <OriginalMetadataFiles>
            <File>c019df6a-fccd-4f81-86d8-085489fc71db</File>
          </OriginalMetadataFiles>
          <TransferDateTime>2023-08-01T00:00Z</TransferDateTime>
          <TransferringBody>Test Transferring Body</TransferringBody>
          <UpstreamSystem>Test Upstream System</UpstreamSystem>
          <UpstreamSystemRef>UpstreamSystemReference</UpstreamSystemRef>
        </Source>
      </opex:DescriptiveMetadata>
    </opex:OPEXMetadata>

  val assetId: UUID = UUID.fromString("68b1c80b-36b8-4f0f-94d6-92589002d87e")
  val assetParentPath: String = "a/parent/path"
  val childIdJson: UUID = UUID.fromString("feedd76d-e368-45c8-96e3-c37671476793")
  val childIdDocx: UUID = UUID.fromString("a25d33f3-7726-4fb3-8e6f-f66358451c4e")
  val batchId: String = "TEST-ID"
  val executionName = "test-execution"

  val input: Input = Input(assetId, batchId, executionName)

  def outputStream: ByteArrayOutputStream = new ByteArrayOutputStream()

  val emptyDynamoGetResponse: String = """{"Responses": {"test-table": []}}"""
  val emptyDynamoPostResponse: String = """{"Count": 0, "Items": []}"""
  val dynamoPostResponse: String =
    s"""{
       |  "Count": 2,
       |  "Items": [
       |    {
       |      "checksum_sha256": {
       |        "S": "checksumdocx"
       |      },
       |      "fileExtension": {
       |        "S": "docx"
       |      },
       |      "fileSize": {
       |        "N": "1"
       |      },
       |      "sortOrder": {
       |        "N": "1"
       |      },
       |      "childCount": {
       |        "N": "0"
       |      },
       |      "id": {
       |        "S": "$childIdDocx"
       |      },
       |      "parentPath": {
       |        "S": "parent/path"
       |      },
       |      "name": {
       |        "S": "$batchId.docx"
       |      },
       |      "type": {
       |        "S": "File"
       |      },
       |      "batchId": {
       |        "S": "$batchId"
       |      },
       |      "location": {
       |        "S": "s3://test-source-bucket/$batchId/$childIdDocx"
       |      },
       |      "transferringBody": {
       |        "S": "Test Transferring Body"
       |      },
       |      "transferCompleteDatetime": {
       |        "S": "2023-09-01T00:00Z"
       |      },
       |      "upstreamSystem": {
       |        "S": "Test Upstream System"
       |      },
       |      "digitalAssetSource": {
       |        "S": "Test Digital Asset Source"
       |      },
       |      "digitalAssetSubtype": {
       |        "S": "Test Digital Asset Subtype"
       |      },
       |      "representationType": {
       |        "S": "Preservation"
       |      },
       |      "representationSuffix": {
       |        "N": "1"
       |      },
       |      "originalFiles": {
       |        "L": [ { "S" : "b6102810-53e3-43a2-9f69-fafe71d4aa40" } ]
       |      },
       |      "originalMetadataFiles": {
       |        "L": [ { "S" : "c019df6a-fccd-4f81-86d8-085489fc71db" } ]
       |      },
       |      "id_Code": {
       |          "S": "Code"
       |      },
       |      "id_UpstreamSystemReference": {
       |        "S": "UpstreamSystemReference"
       |      }
       |    },
       |    {
       |      "checksum_sha256": {
       |        "S": "checksum"
       |      },
       |      "fileExtension": {
       |        "S": "json"
       |      },
       |      "fileSize": {
       |        "N": "2"
       |      },
       |      "sortOrder": {
       |        "N": "2"
       |      },
       |      "childCount": {
       |        "N": "0"
       |      },
       |      "id": {
       |        "S": "$childIdJson"
       |      },
       |      "parentPath": {
       |        "S": "parent/path"
       |      },
       |      "name": {
       |        "S": "$batchId.json"
       |      },
       |      "type": {
       |        "S": "File"
       |      },
       |      "batchId": {
       |        "S": "$batchId"
       |      },
       |      "location": {
       |        "S": "s3://test-source-bucket/$batchId/$childIdJson"
       |      },
       |      "transferringBody": {
       |        "S": "Test Transferring Body"
       |      },
       |      "transferCompleteDatetime": {
       |        "S": "2023-09-01T00:00Z"
       |      },
       |      "upstreamSystem": {
       |        "S": "Test Upstream System"
       |      },
       |      "digitalAssetSource": {
       |        "S": "Test Digital Asset Source"
       |      },
       |      "digitalAssetSubtype": {
       |        "S": "Test Digital Asset Subtype"
       |      },
       |      "representationType": {
       |        "S": "Preservation"
       |      },
       |      "representationSuffix": {
       |        "N": "1"
       |      },
       |      "originalFiles": {
       |        "L": [ { "S" : "b6102810-53e3-43a2-9f69-fafe71d4aa40" } ]
       |      },
       |      "originalMetadataFiles": {
       |        "L": [ { "S" : "c019df6a-fccd-4f81-86d8-085489fc71db" } ]
       |      },
       |      "id_Code": {
       |          "S": "Code"
       |      },
       |      "id_UpstreamSystemReference": {
       |        "S": "UpstreamSystemReference"
       |      }
       |    }
       |  ]
       |}
       |""".stripMargin

  def dynamoGetResponse(childCount: Int = 2): String =
    s"""{
       |  "Responses": {
       |    "test-table": [
       |      {
       |        "id": {
       |          "S": "$assetId"
       |        },
       |        "name": {
       |          "S": "Test Name"
       |        },
       |        "parentPath": {
       |          "S": "$assetParentPath"
       |        },
       |        "type": {
       |          "S": "Asset"
       |        },
       |        "batchId": {
       |          "S": "$batchId"
       |        },
       |        "childCount": {
       |          "N": "$childCount"
       |        },
       |        "transferringBody": {
       |          "S": "Test Transferring Body"
       |        },
       |        "transferCompleteDatetime": {
       |          "S": "2023-08-01T00:00Z"
       |        },
       |        "upstreamSystem": {
       |          "S": "Test Upstream System"
       |        },
       |        "digitalAssetSource": {
       |          "S": "Test Digital Asset Source"
       |        },
       |        "digitalAssetSubtype": {
       |          "S": "Test Digital Asset Subtype"
       |        },
       |        "originalFiles": {
       |          "L": [ { "S" : "b6102810-53e3-43a2-9f69-fafe71d4aa40" } ]
       |        },
       |        "originalMetadataFiles": {
       |          "L": [ { "S" : "c019df6a-fccd-4f81-86d8-085489fc71db" } ]
       |        },
       |        "id_Code": {
       |          "S": "Code"
       |        },
       |        "id_UpstreamSystemReference": {
       |          "S": "UpstreamSystemReference"
       |        }
       |      }
       |    ]
       |  }
       |}
       |""".stripMargin

  private val creds: StaticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
  private val asyncDynamoClient: DynamoDbAsyncClient = DynamoDbAsyncClient
    .builder()
    .endpointOverride(URI.create("http://localhost:9003"))
    .region(Region.EU_WEST_2)
    .credentialsProvider(creds)
    .build()

  private val asyncS3Client: S3AsyncClient = S3AsyncClient
    .crtBuilder()
    .endpointOverride(URI.create("http://localhost:9004"))
    .region(Region.EU_WEST_2)
    .credentialsProvider(creds)
    .targetThroughputInGbps(20.0)
    .minimumPartSizeInBytes(10 * 1024 * 1024)
    .build()

  val dependencies: Dependencies = Dependencies(DADynamoDBClient[IO](asyncDynamoClient), DAS3Client[IO](asyncS3Client), XMLCreator(OffsetDateTime.parse("2023-09-01T00:00Z")))
}
