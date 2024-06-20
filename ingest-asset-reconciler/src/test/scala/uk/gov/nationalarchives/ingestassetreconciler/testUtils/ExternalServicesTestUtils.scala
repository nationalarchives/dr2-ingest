package uk.gov.nationalarchives.ingestassetreconciler.testUtils

import cats.effect.IO
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.ingestassetreconciler.Lambda.{Dependencies, Input}
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.{Identifier => PreservicaIdentifier}
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType.*
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.*
import uk.gov.nationalarchives.DADynamoDBClient

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID
import scala.jdk.CollectionConverters.*

class ExternalServicesTestUtils(dynamoServer: WireMockServer) extends AnyFlatSpec with TableDrivenPropertyChecks with MockitoSugar {
  val assetId: UUID = UUID.fromString("68b1c80b-36b8-4f0f-94d6-92589002d87e")
  val assetName: UUID = UUID.fromString("acdb2e57-923b-4caa-8fd9-a2f79f650c43")
  val assetParentPath: String = "a/parent/path"
  val childIdJson: UUID = UUID.fromString("feedd76d-e368-45c8-96e3-c37671476793")
  val childIdDocx: UUID = UUID.fromString("a25d33f3-7726-4fb3-8e6f-f66358451c4e")
  val docxTitle: String = "TestTitle"
  val batchId: String = "TEST-ID"
  val lockTableBatchId: UUID = UUID.fromString("3d6f242a-f2f9-4e09-91b3-b2d3c8c6a84d")
  val executionId = "5619e6b0-e959-4e61-9f6e-17170f7c06e2-3a3443ae-92c4-4fc8-9cbd-10c2a58b6045"
  val parentMessageId: UUID = UUID.fromString("6d7ff426-047e-4dcb-a4d6-95077976b763")
  val messageId: UUID = UUID.fromString("787bf94b-efdc-4d4b-a93c-a0e537d089fd")
  val input: Input = Input(executionId, batchId, assetId)
  val newMessageId: UUID = UUID.fromString("dacaea0e-9a6e-4928-852d-bf3b92a84a8a")

  val defaultDocxBitStreamInfo: BitStreamInfo = BitStreamInfo(
    s"84cca074-a7bc-4740-9418-bcc9df9fef7e.docx",
    1234,
    "http://localhost/api/entity/content-objects/fc0a687d-f7fa-454e-941a-683bbf5594b1/generations/1/bitstreams/1/content",
    Fixity("SHA256", "f7523c5d03a2c850fa06b5bbfed4c216f6368826"),
    1,
    Original,
    Some(s"$docxTitle.docx"),
    None
  )
  val defaultJsonBitStreamInfo: BitStreamInfo = BitStreamInfo(
    s"9ef5eb16-3017-401f-8180-cf74c2c25ec1.json",
    1235,
    "http://localhost/api/entity/content-objects/4dee285b-64e4-49f8-942e-84ab460b5af6/generations/1/bitstreams/1/content",
    Fixity("SHA256", "a8cfe9e6b5c10a26046c849cd3776734626e74a2"),
    1,
    Original,
    Some(s"$batchId.json"),
    None
  )

  val emptyDynamoGetResponse: String = """{"Responses": {"test-table": []}}"""
  val emptyDynamoPostResponse: String = """{"Count": 0, "Items": []}"""
  val dynamoPostResponse: String =
    s"""{
       |  "Count": 2,
       |  "Items": [
       |    {
       |      "checksum_sha256": {
       |        "S": "f7523c5d03a2c850fa06b5bbfed4c216f6368826"
       |      },
       |      "title": {
       |        "S": "$docxTitle"
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
       |        "S": "$docxTitle.docx"
       |      },
       |      "type": {
       |        "S": "File"
       |      },
       |      "batchId": {
       |        "S": "$batchId"
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
       |        "S": "Access"
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
       |        "S": "a8cfe9e6b5c10a26046c849cd3776734626e74a2"
       |      },
       |      "title": {
       |        "S": ""
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
       |          "S": "$assetName"
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

  val dynamoLockTableGetResponse: String =
    s"""{
       |  "Responses": {
       |    "test-lock-table": [
       |      {
       |        "ioId": {
       |          "S": "$assetId"
       |        },
       |        "batchId": {
       |          "S": "$lockTableBatchId"
       |        },
       |        "message": {
       |          "S": "{'parentMessageId':'$parentMessageId','messageId':'$messageId','executionId':'$batchId'}"
       |        }
       |      }
       |    ]
       |  }
       |}
       |""".stripMargin

  val emptyLockTableGetResponse: String = """{"Responses": {"test-lock-table": []}}"""

  val defaultEntity =
    Entity(
      Some(InformationObject),
      UUID.fromString("354f47cf-3ca2-4a4e-8181-81b714334f00"),
      None,
      None,
      false,
      Some(InformationObject.entityPath),
      None,
      Some(UUID.fromString("a9e1cae8-ea06-4157-8dd4-82d0525b031c"))
    )

  val twoEntitiesWithSameDetails =
    Seq(defaultEntity, defaultEntity)


  private val expectedFilesTableGetRequest =
    s"""{"RequestItems":{"test-table":{"Keys":[{"id":{"S":"$assetId"}}]}}}"""
  private val expectedFilesTableUpdateRequest =
    s"""{"TableName":"test-table","IndexName":"",""" +
      """"KeyConditionExpression":"#A = :batchId AND #B = :parentPath",""" +
      """"ExpressionAttributeNames":{"#A":"batchId","#B":"parentPath"},""" +
      s""""ExpressionAttributeValues":{":batchId":{"S":"$batchId"},":parentPath":{"S":"${assetParentPath}/$assetId"}}}"""

  private val expectedLockTableGetRequest =
    s"""{"RequestItems":{"test-lock-table":{"Keys":[{"ioId":{"S":"$assetName"}}]}}}"""

  private val defaultIoWithIdentifier =
    IO.pure(
      Seq(defaultEntity)
    )

  private val defaultUrlToIoRep = IO.pure(
    Seq(
      "http://localhost/api/entity/information-objects/14e54a24-db26-4c00-852c-f28045e51828/representations/Preservation/10"
    )
  )

  private val defaultContentObjectsFromRep =
    IO.pure(
      Seq(
        Entity(
          Some(ContentObject),
          UUID.fromString("fc0a687d-f7fa-454e-941a-683bbf5594b1"),
          Some(s"$docxTitle.docx"),
          None,
          false,
          Some(ContentObject.entityPath),
          None,
          Some(UUID.fromString("354f47cf-3ca2-4a4e-8181-81b714334f00"))
        ),
        Entity(
          Some(ContentObject),
          UUID.fromString("4dee285b-64e4-49f8-942e-84ab460b5af6"),
          Some(s"$batchId.json"),
          None,
          false,
          Some(ContentObject.entityPath),
          None,
          Some(UUID.fromString("354f47cf-3ca2-4a4e-8181-81b714334f00"))
        )
      )
    )

  private val defaultBitStreamInfo =
    Seq(IO.pure(Seq(defaultDocxBitStreamInfo)), IO.pure(Seq(defaultJsonBitStreamInfo)))

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

  def stubLockTableGetRequest(batchLockTableGetResponse: String): Unit =
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.RequestItems", containing("test-lock-table")))
        .willReturn(ok().withBody(batchLockTableGetResponse))
    )

  private val creds: StaticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))

  private val asyncDynamoClient: DynamoDbAsyncClient = DynamoDbAsyncClient
    .builder()
    .endpointOverride(URI.create("http://localhost:9005"))
    .region(Region.EU_WEST_2)
    .credentialsProvider(creds)
    .build()

  private val dADynamoDBClient: DADynamoDBClient[IO] = new DADynamoDBClient[IO](asyncDynamoClient)

  private val mockEntityClient: EntityClient[IO, Fs2Streams[IO]] = mock[EntityClient[IO, Fs2Streams[IO]]]

  val dependencies: Dependencies = Dependencies(mockEntityClient, dADynamoDBClient, newMessageId, () => OffsetDateTime.parse("2024-06-01T00:00Z"))

  case class ArgumentVerifier(
      entitiesWithIdentifier: IO[Seq[Entity]] = defaultIoWithIdentifier,
      urlsToIoRepresentations: IO[Seq[String]] = defaultUrlToIoRep,
      contentObjectsFromReps: IO[Seq[Entity]] = defaultContentObjectsFromRep,
      bitstreamInfo: Seq[IO[Seq[BitStreamInfo]]] = defaultBitStreamInfo
  ) {
    Mockito.reset(mockEntityClient)
    when(
      mockEntityClient.entitiesByIdentifier(any[PreservicaIdentifier])
    ).thenReturn(entitiesWithIdentifier)

    when(
      mockEntityClient.getUrlsToIoRepresentations(any[UUID], any[Option[RepresentationType]])
    ).thenReturn(urlsToIoRepresentations)

    when(
      mockEntityClient.getContentObjectsFromRepresentation(any[UUID], ArgumentMatchers.eq(Preservation), any[Int])
    ).thenReturn(contentObjectsFromReps.map(_.lastOption.toList))

    when(
      mockEntityClient.getContentObjectsFromRepresentation(any[UUID], ArgumentMatchers.eq(Access), any[Int])
    ).thenReturn(contentObjectsFromReps.map(_.headOption.toList))

    when(
      mockEntityClient.getBitstreamInfo(ArgumentMatchers.eq(UUID.fromString("fc0a687d-f7fa-454e-941a-683bbf5594b1")))
    ).thenReturn(bitstreamInfo.head)

    when(
      mockEntityClient.getBitstreamInfo(ArgumentMatchers.eq(UUID.fromString("4dee285b-64e4-49f8-942e-84ab460b5af6")))
    ).thenReturn(bitstreamInfo.last)

    def verifyInvocationsAndArgumentsPassed(
        numOfEntitiesByIdentifierInvocations: Int = 1,
        numOfGetUrlsToIoRepresentationsRequests: Int = 2,
        numOfGetContentObjectsFromRepresentationRequests: Int = 2,
        numOfGetBitstreamInfoRequests: Int = 2,
        numOfFileTableGetRequests: Int = 1,
        numOfFileTableUpdateRequests: Int = 1,
        numOfLockTableGetRequests: Int = 0
    ): Unit = {
      val serveEvents = dynamoServer.getAllServeEvents.asScala.toList

      serveEvents.size should equal(numOfFileTableGetRequests + numOfFileTableUpdateRequests + numOfLockTableGetRequests)
      serveEvents.map(_.getRequest.getBodyAsString) should equal(
        List.fill(numOfLockTableGetRequests)(expectedLockTableGetRequest) ++
          List.fill(numOfFileTableUpdateRequests)(expectedFilesTableUpdateRequest) ++
          List.fill(numOfFileTableGetRequests)(expectedFilesTableGetRequest)
      )

      val entitiesByIdentifierIdentifierToGetCaptor = getIdentifierToGetCaptor

      verify(mockEntityClient, times(numOfEntitiesByIdentifierInvocations)).entitiesByIdentifier(
        entitiesByIdentifierIdentifierToGetCaptor.capture()
      )

      if (numOfEntitiesByIdentifierInvocations > 0)
        entitiesByIdentifierIdentifierToGetCaptor.getValue should be(PreservicaIdentifier("SourceID", "acdb2e57-923b-4caa-8fd9-a2f79f650c43"))

      val ioEntityRefForUrlsRequestCaptor = getIoEntityRefCaptor
      val optionalRepresentationTypeCaptorRequestCaptor = getOptionalRepresentationTypeCaptor

      verify(mockEntityClient, times(numOfGetUrlsToIoRepresentationsRequests)).getUrlsToIoRepresentations(
        ioEntityRefForUrlsRequestCaptor.capture(),
        optionalRepresentationTypeCaptorRequestCaptor.capture()
      )

      if (numOfGetUrlsToIoRepresentationsRequests > 0) {
        val uuid = UUID.fromString("354f47cf-3ca2-4a4e-8181-81b714334f00")
        ioEntityRefForUrlsRequestCaptor.getAllValues.asScala should be(List(uuid, uuid))
        optionalRepresentationTypeCaptorRequestCaptor.getAllValues.asScala.sortBy(_.map(_.toString())).toList should be(List(Some(Access), Some(Preservation)))
      }

      val ioEntityRefForContentObjectsRequestCaptor = getIoEntityRefCaptor
      val representationTypeCaptorRequestCaptor = getRepresentationTypeCaptor
      val versionCaptor = getVersion

      verify(mockEntityClient, times(numOfGetContentObjectsFromRepresentationRequests))
        .getContentObjectsFromRepresentation(
          ioEntityRefForContentObjectsRequestCaptor.capture(),
          representationTypeCaptorRequestCaptor.capture(),
          versionCaptor.capture()
        )

      if (numOfGetContentObjectsFromRepresentationRequests > 0) {
        val uuid = UUID.fromString("354f47cf-3ca2-4a4e-8181-81b714334f00")
        ioEntityRefForContentObjectsRequestCaptor.getAllValues.asScala should be(List(uuid, uuid))
        representationTypeCaptorRequestCaptor.getAllValues.asScala.sortBy(_.toString()).toList should be(List(Access, Preservation))
        versionCaptor.getAllValues.asScala.sorted.toList should be(List(1, 1))
      }

      val contentRefRequestCaptor = getContentRef

      verify(mockEntityClient, times(numOfGetBitstreamInfoRequests)).getBitstreamInfo(contentRefRequestCaptor.capture())

      if (numOfGetBitstreamInfoRequests > 0) {
        contentRefRequestCaptor.getAllValues.asScala.sorted.toList should be(
          List(UUID.fromString("fc0a687d-f7fa-454e-941a-683bbf5594b1"), UUID.fromString("4dee285b-64e4-49f8-942e-84ab460b5af6"))
        )
      }

      ()
    }

    def getIdentifierToGetCaptor: ArgumentCaptor[PreservicaIdentifier] = ArgumentCaptor.forClass(classOf[PreservicaIdentifier])

    def getIoEntityRefCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])

    def getOptionalRepresentationTypeCaptor: ArgumentCaptor[Option[RepresentationType]] =
      ArgumentCaptor.forClass(classOf[Option[RepresentationType]])

    def getRepresentationTypeCaptor: ArgumentCaptor[RepresentationType] =
      ArgumentCaptor.forClass(classOf[RepresentationType])

    def getVersion: ArgumentCaptor[Int] = ArgumentCaptor.forClass(classOf[Int])

    def getContentRef: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
  }
}
