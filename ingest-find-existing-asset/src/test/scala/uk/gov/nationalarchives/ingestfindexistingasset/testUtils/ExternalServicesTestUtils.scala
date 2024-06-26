package uk.gov.nationalarchives.ingestfindexistingasset.testUtils

import cats.effect.IO
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient
import uk.gov.nationalarchives.ingestfindexistingasset.Lambda.{Config, Input, StateOutput}
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.Identifier
import uk.gov.nationalarchives.dp.client.EntityClient.*
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*

import java.net.URI
import java.util.UUID
import scala.jdk.CollectionConverters.CollectionHasAsScala

class ExternalServicesTestUtils extends AnyFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll {
  val dynamoServer = new WireMockServer(9016)
  val tableName = "test-table"

  val folderId: UUID = UUID.fromString("68b1c80b-36b8-4f0f-94d6-92589002d87e")
  val assetId: UUID = UUID.fromString("5edc7a1b-e8c4-4961-a63b-75b2068b69ec")
  val folderParentPath: String = "a/parent/path"
  val childId: UUID = UUID.fromString("feedd76d-e368-45c8-96e3-c37671476793")
  val batchId: String = "TEST-ID"
  val executionName = "test-execution"
  val config: Config = Config("http://localhost:9016", "", tableName)
  val input: Input = Input(assetId, batchId)

  val defaultEntityWithSourceIdReturnValue: IO[Seq[Entity]] =
    IO.pure(
      Seq(
        Entity(
          Some(InformationObject),
          UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50"),
          None,
          None,
          deleted = false,
          Some(InformationObject.entityPath),
          None,
          None
        )
      )
    )
  val emptyDynamoGetResponse: String = s"""{"Responses": {"$tableName": []}}"""
  val dynamoGetResponse: String =
    s"""{
       |  "Responses": {
       |    "$tableName": [
       |      {
       |        "id": {
       |          "S": "$assetId"
       |        },
       |        "name": {
       |          "S": "Test Name"
       |        },
       |        "parentPath": {
       |          "S": "$folderId/$folderParentPath"
       |        },
       |        "type": {
       |          "S": "Asset"
       |        },
       |        "batchId": {
       |          "S": "$batchId"
       |        },
       |        "id_Code": {
       |          "S": "Code"
       |        },
       |        "transferringBody": {
       |          "S": "transferringBody"
       |        },
       |        "transferCompleteDatetime": {
       |          "S": "2023-12-07T17:22:23.605036797Z"
       |        },
       |        "upstreamSystem": {
       |          "S": "upstreamSystem"
       |        },
       |        "digitalAssetSource": {
       |          "S": "digitalAssetSource"
       |        },
       |        "digitalAssetSubtype": {
       |          "S": "digitalAssetSubtype"
       |        },
       |        "childCount": {
       |          "N": "0"
       |        },
       |        "originalFiles": {
       |          "L": [
       |            {
       |              "S": "${UUID.randomUUID()}"
       |            }
       |          ]
       |        },
       |        "originalMetadataFiles": {
       |          "L": [
       |            {
       |              "S": "${UUID.randomUUID()}"
       |            }
       |          ]
       |        }
       |      }
       |    ]
       |  }
       |}
       |""".stripMargin

  private val expectedGetRequest =
    s"""{"RequestItems":{"test-table":{"Keys":[{"batchId":{"S":"$batchId"},"id":{"S":"$assetId"}}]}}}"""
  private val expectedUpdateRequest =
    s"""{"TableName":"test-table","Key":{"id":{"S":"$assetId"},"batchId":{"S":"$batchId"}},"AttributeUpdates":{"skipIngest":{"Value":{"BOOL":true},"Action":"PUT"}}}"""

  override def beforeEach(): Unit = {
    dynamoServer.start()
  }

  override def afterEach(): Unit = {
    dynamoServer.resetAll()
    dynamoServer.stop()
  }

  def stubBatchGetRequest(batchGetResponse: String): Unit =
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.RequestItems", containing(tableName)))
        .willReturn(ok().withBody(batchGetResponse))
    )

  def stubBatchPostRequest(potentialUpdateResponse: Option[String] = None): Unit =
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.TableName", containing(tableName)))
        .willReturn(
          potentialUpdateResponse match {
            case Some(updateResponse) => ok().withBody(updateResponse)
            case None                 => serverError()
          }
        )
    )

  private val creds: StaticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
  private val asyncDynamoClient: DynamoDbAsyncClient = DynamoDbAsyncClient
    .builder()
    .endpointOverride(URI.create("http://localhost:9016"))
    .region(Region.EU_WEST_2)
    .credentialsProvider(creds)
    .build()

  val dynamoClient: DADynamoDBClient[IO] = new DADynamoDBClient[IO](asyncDynamoClient)

  case class ArgumentVerifier(defaultEntityWithSourceIdReturnValue: IO[Seq[Entity]] = IO.pure(Nil)) {
    val mockEntityClient: EntityClient[IO, Fs2Streams[IO]] = mock[EntityClient[IO, Fs2Streams[IO]]]

    when(mockEntityClient.entitiesByIdentifier(any[Identifier]))
      .thenReturn(defaultEntityWithSourceIdReturnValue)

    def verifyInvocationsAndArgumentsPassed(
        numOfDynamoGetRequests: Int = 1,
        numOfDynamoUpdateRequests: Int = 0,
        expectedAssetExistsResponse: Boolean = false,
        potentialResponse: Option[StateOutput] = None
    ): Unit = {

      val serveEvents = dynamoServer.getAllServeEvents.asScala.toList

      serveEvents.size should equal(numOfDynamoGetRequests + numOfDynamoUpdateRequests)
      val d = serveEvents.map(_.getRequest.getBodyAsString)
      println(d)
      serveEvents.map(_.getRequest.getBodyAsString) should equal(
        List.fill(numOfDynamoUpdateRequests)(expectedUpdateRequest) ++ List.fill(numOfDynamoGetRequests)(expectedGetRequest)
      )
      potentialResponse.foreach(response => response.assetExists should equal(expectedAssetExistsResponse))
    }
  }
}
