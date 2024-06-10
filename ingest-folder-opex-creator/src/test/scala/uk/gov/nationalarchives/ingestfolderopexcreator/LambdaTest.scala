package uk.gov.nationalarchives.ingestfolderopexcreator

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.http.RequestMethod
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClient
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import uk.gov.nationalarchives.ingestfolderopexcreator.Lambda.{Config, Dependencies, Input}

import java.net.URI
import java.util.UUID
import scala.jdk.CollectionConverters.*

class LambdaTest extends AnyFlatSpec with BeforeAndAfterEach {
  val dynamoServer = new WireMockServer(9006)
  val s3Server = new WireMockServer(9007)
  val tableName = "test-table"

  override def beforeEach(): Unit = {
    dynamoServer.start()
    s3Server.start()
  }

  override def afterEach(): Unit = {
    dynamoServer.resetAll()
    s3Server.resetAll()
    dynamoServer.stop()
    s3Server.stop()
  }

  def stubPutRequest(itemPaths: String*): Unit = {
    s3Server.stubFor(
      head(urlEqualTo(s"/opex/$executionName/$folderId/$folderParentPath/$assetId.pax.opex"))
        .willReturn(ok().withHeader("Content-Length", "100"))
    )
    itemPaths.foreach { itemPath =>
      s3Server.stubFor(
        put(urlEqualTo(itemPath))
          .withHost(equalTo("test-destination-bucket.localhost"))
          .willReturn(ok())
      )
      s3Server.stubFor(
        head(urlEqualTo(s"/$itemPath"))
          .willReturn(ok().withHeader("Content-Length", "10"))
      )
    }
  }

  def stubBatchGetRequest(batchGetResponse: String): Unit =
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.RequestItems", containing(tableName)))
        .willReturn(ok().withBody(batchGetResponse))
    )

  def stubDynamoQueryRequest(queryResponse: String): Unit =
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.TableName", equalTo(tableName)))
        .willReturn(ok().withBody(queryResponse))
    )

  val folderId: UUID = UUID.fromString("68b1c80b-36b8-4f0f-94d6-92589002d87e")
  val assetId: UUID = UUID.fromString("5edc7a1b-e8c4-4961-a63b-75b2068b69ec")
  val folderParentPath: String = "a/parent/path"
  val childId: UUID = UUID.fromString("feedd76d-e368-45c8-96e3-c37671476793")
  val batchId: String = "TEST-ID"
  val executionName = "test-execution"
  private val config: Config = Config(tableName, "test-destination-bucket", "test-gsi")
  private val input: Input = Input(folderId, batchId, executionName)

  val emptyDynamoGetResponse: String = s"""{"Responses": {"$tableName": []}}"""
  val emptyDynamoQueryResponse: String = """{"Count": 0, "Items": []}"""
  val dynamoQueryResponse: String =
    s"""{
       |  "Count": 2,
       |  "Items": [
       |    {
       |      "id": {
       |        "S": "$childId"
       |      },
       |      "parentPath": {
       |        "S": "parent/path"
       |      },
       |      "name": {
       |        "S": "$batchId.json"
       |      },
       |      "type": {
       |        "S": "ContentFolder"
       |      },
       |      "batchId": {
       |        "S": "$batchId"
       |      },
       |      "childCount": {
       |        "N": "0"
       |      }
       |    },
       |    {
       |      "id": {
       |        "S": "$assetId"
       |      },
       |      "name": {
       |        "S": "Test Asset"
       |      },
       |      "parentPath": {
       |        "S": "$folderId/$folderParentPath"
       |      },
       |      "type": {
       |        "S": "Asset"
       |      },
       |      "batchId": {
       |        "S": "$batchId"
       |      },
       |      "childCount": {
       |        "N": "0"
       |      },
       |      "transferringBody": {
       |        "S": "transferringBody"
       |      },
       |      "transferCompleteDatetime": {
       |        "S": "2023-12-07T17:22:23.605036797Z"
       |      },
       |      "upstreamSystem": {
       |        "S": "upstreamSystem"
       |      },
       |      "digitalAssetSource": {
       |        "S": "digitalAssetSource"
       |      },
       |      "digitalAssetSubtype": {
       |        "S": "digitalAssetSubtype"
       |      },
       |      "originalFiles": {
       |        "L": [
       |        {
       |          "S": "${UUID.randomUUID()}"
       |        }
       |       ]
       |      },
       |      "originalMetadataFiles": {
       |        "L": [
       |          {
       |            "S": "${UUID.randomUUID()}"
       |          }
       |        ]
       |      }
       |    }
       |]
       |}
       |""".stripMargin
  val dynamoGetResponse: String =
    s"""{
       |  "Responses": {
       |    "$tableName": [
       |      {
       |        "id": {
       |          "S": "$folderId"
       |        },
       |        "name": {
       |          "S": "Test Name"
       |        },
       |        "parentPath": {
       |          "S": "$folderParentPath"
       |        },
       |        "type": {
       |          "S": "ArchiveFolder"
       |        },
       |        "batchId": {
       |          "S": "$batchId"
       |        },
       |        "childCount": {
       |          "N": "0"
       |        },
       |        "id_Code": {
       |          "S": "Code"
       |        }
       |      }
       |    ]
       |  }
       |}
       |""".stripMargin

  private val creds: StaticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
  private val asyncDynamoClient: DynamoDbAsyncClient = DynamoDbAsyncClient
    .builder()
    .endpointOverride(URI.create("http://localhost:9006"))
    .region(Region.EU_WEST_2)
    .credentialsProvider(creds)
    .build()

  private val asyncS3Client: S3AsyncClient = S3AsyncClient
    .crtBuilder()
    .endpointOverride(URI.create("http://localhost:9007"))
    .region(Region.EU_WEST_2)
    .credentialsProvider(creds)
    .targetThroughputInGbps(20.0)
    .minimumPartSizeInBytes(10 * 1024 * 1024)
    .build()
  private val dynamoClient: DADynamoDBClient[IO] = new DADynamoDBClient[IO](asyncDynamoClient)
  private val s3Client: DAS3Client[IO] = DAS3Client[IO](asyncS3Client)

  val dependencies: Dependencies = Dependencies(dynamoClient, s3Client, XMLCreator())

  "handler" should "return an error if the folder is not found in dynamo" in {
    stubBatchGetRequest(emptyDynamoGetResponse)
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"No folder found for $folderId and $batchId")
  }

  "handler" should "return an error if no children are found for the folder" in {
    stubBatchGetRequest(dynamoGetResponse)
    stubDynamoQueryRequest(emptyDynamoQueryResponse)
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"No children found for $folderId and $batchId")
  }

  "handler" should "return an error if the dynamo entry does not have a type of 'folder'" in {
    stubBatchGetRequest(dynamoGetResponse.replace("ArchiveFolder", "Asset"))
    stubDynamoQueryRequest(emptyDynamoQueryResponse)
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"Object $folderId is of type Asset and not 'ContentFolder' or 'ArchiveFolder'")
  }

  "handler" should "pass the correct id to dynamo getItem" in {
    stubBatchGetRequest(emptyDynamoGetResponse)
    intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    val serveEvents = dynamoServer.getAllServeEvents.asScala
    serveEvents.size should equal(1)
    serveEvents.head.getRequest.getBodyAsString should equal(s"""{"RequestItems":{"$tableName":{"Keys":[{"id":{"S":"$folderId"}}]}}}""")
  }

  "handler" should "pass the parent path with no prefixed slash to dynamo if the parent path is empty" in {
    stubBatchGetRequest(dynamoGetResponse.replace("a/parent/path", ""))
    stubDynamoQueryRequest(emptyDynamoQueryResponse)
    intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    val serveEvents = dynamoServer.getAllServeEvents.asScala
    val queryEvent = serveEvents.head
    val requestBody = queryEvent.getRequest.getBodyAsString
    val expectedRequestBody =
      s"""{"TableName":"$tableName","IndexName":"test-gsi","KeyConditionExpression":"#A = :batchId AND #B = :parentPath",""" +
        s""""ExpressionAttributeNames":{"#A":"batchId","#B":"parentPath"},"ExpressionAttributeValues":{":batchId":{"S":"TEST-ID"},":parentPath":{"S":"$folderId"}}}"""
    expectedRequestBody should equal(requestBody)
  }

  "handler" should "pass the correct parameters to dynamo for the query request" in {
    stubBatchGetRequest(dynamoGetResponse)
    stubDynamoQueryRequest(emptyDynamoQueryResponse)
    intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    val serveEvents = dynamoServer.getAllServeEvents.asScala
    val queryEvent = serveEvents.head
    val requestBody = queryEvent.getRequest.getBodyAsString
    val expectedRequestBody =
      s"""{"TableName":"$tableName","IndexName":"test-gsi","KeyConditionExpression":"#A = :batchId AND #B = :parentPath",""" +
        s""""ExpressionAttributeNames":{"#A":"batchId","#B":"parentPath"},"ExpressionAttributeValues":{":batchId":{"S":"TEST-ID"},":parentPath":{"S":"$folderParentPath/$folderId"}}}"""
    expectedRequestBody should equal(requestBody)
  }

  "handler" should "upload the opex file to the correct path" in {
    stubBatchGetRequest(dynamoGetResponse)
    stubDynamoQueryRequest(dynamoQueryResponse)
    val opexPath = s"/opex/$executionName/$folderParentPath/$folderId/$folderId.opex"
    stubPutRequest(opexPath)

    new Lambda().handler(input, config, dependencies).unsafeRunSync()

    val s3CopyRequests = s3Server.getAllServeEvents.asScala
    s3CopyRequests.count(_.getRequest.getUrl == opexPath) should equal(1)
  }

  "handler" should "upload the correct body to S3" in {
    val expectedResponseXML =
      <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.2">
      <opex:Properties>
        <opex:Title>Test Name</opex:Title>
        <opex:Description></opex:Description>
        <opex:SecurityDescriptor>open</opex:SecurityDescriptor>
        <opex:Identifiers>
          <opex:Identifier type="Code">Code</opex:Identifier>
        </opex:Identifiers>
      </opex:Properties>
      <opex:Transfer>
        <opex:SourceID>Test Name</opex:SourceID>
        <opex:Manifest>
          <opex:Folders>
            <opex:Folder>{assetId}.pax</opex:Folder>
            <opex:Folder>{childId}</opex:Folder>
          </opex:Folders>
          <opex:Files>
            <opex:File type="metadata" size="100">{assetId}.pax.opex</opex:File>
          </opex:Files>
        </opex:Manifest>
      </opex:Transfer>
    </opex:OPEXMetadata>
    stubBatchGetRequest(dynamoGetResponse)
    stubDynamoQueryRequest(dynamoQueryResponse)
    val opexPath = s"/opex/$executionName/$folderParentPath/$folderId/$folderId.opex"
    stubPutRequest(opexPath)

    new Lambda().handler(input, config, dependencies).unsafeRunSync()

    val s3Events = s3Server.getAllServeEvents.asScala
    val s3PutEvent = s3Events.filter(_.getRequest.getMethod == RequestMethod.PUT).head
    val body = s3PutEvent.getRequest.getBodyAsString.split("\r\n")(1)

    body should equal(expectedResponseXML.toString)
  }

  "handler" should "return an error if the Dynamo API is unavailable" in {
    dynamoServer.stop()
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal("Unable to execute HTTP request: Connection refused: localhost/127.0.0.1:9006")
  }

  "handler" should "return an error if the S3 API is unavailable" in {
    s3Server.stop()
    stubBatchGetRequest(dynamoGetResponse)
    stubDynamoQueryRequest(dynamoQueryResponse)
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal("Failed to send the request: socket connection refused.")
  }
}
