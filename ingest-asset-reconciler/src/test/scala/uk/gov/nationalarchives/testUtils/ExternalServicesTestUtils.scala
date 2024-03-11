package uk.gov.nationalarchives.testUtils

import cats.effect.IO
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper}
import org.scalatest.prop.TableDrivenPropertyChecks
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DynamoFormatters.Identifier
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.{
  ContentObject,
  InformationObject,
  Preservation,
  RepresentationType
}
import uk.gov.nationalarchives.{DADynamoDBClient, Lambda}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.net.URI
import java.util.UUID

class ExternalServicesTestUtils(dynamoServer: WireMockServer) extends TableDrivenPropertyChecks {
  val assetId: UUID = UUID.fromString("68b1c80b-36b8-4f0f-94d6-92589002d87e")
  val assetParentPath: String = "a/parent/path"
  val childIdJson: UUID = UUID.fromString("feedd76d-e368-45c8-96e3-c37671476793")
  val childIdDocx: UUID = UUID.fromString("a25d33f3-7726-4fb3-8e6f-f66358451c4e")
  val docxTitle: String = "TestTitle"
  val batchId: String = "TEST-ID"
  val executionId = "5619e6b0-e959-4e61-9f6e-17170f7c06e2-3a3443ae-92c4-4fc8-9cbd-10c2a58b6045"
  val inputJson: String =
    s"""{"assetId": "$assetId", "batchId": "$batchId", "executionId": "$executionId"}"""
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
  val dynamoGetResponse: String =
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
  private val defaultIoWithIdentifier =
    IO(
      Seq(
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
      )
    )

  private val defaultUrlToIoRep = IO(
    Seq(
      "http://localhost/api/entity/information-objects/14e54a24-db26-4c00-852c-f28045e51828/representations/Preservation/10"
    )
  )

  private val defaultContentObjectsFromRep =
    IO(
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

  private val defaultBitStreamInfo = {
    Seq(
      IO(
        Seq(
          BitStreamInfo(
            s"84cca074-a7bc-4740-9418-bcc9df9fef7e.docx",
            1234,
            "http://localhost/api/entity/content-objects/fc0a687d-f7fa-454e-941a-683bbf5594b1/generations/1/bitstreams/1/content",
            Fixity("SHA256", "f7523c5d03a2c850fa06b5bbfed4c216f6368826"),
            Some(s"$docxTitle.docx")
          )
        )
      ),
      IO(
        Seq(
          BitStreamInfo(
            s"9ef5eb16-3017-401f-8180-cf74c2c25ec1.json",
            1235,
            "http://localhost/api/entity/content-objects/4dee285b-64e4-49f8-942e-84ab460b5af6/generations/1/bitstreams/1/content",
            Fixity("SHA256", "a8cfe9e6b5c10a26046c849cd3776734626e74a2"),
            Some(s"$batchId.json")
          )
        )
      )
    )
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

  def standardInput: ByteArrayInputStream = new ByteArrayInputStream(inputJson.getBytes)

  def outputStream: ByteArrayOutputStream = new ByteArrayOutputStream()

  case class TestLambda(
      entitiesWithIdentifier: IO[Seq[Entity]] = defaultIoWithIdentifier,
      urlsToIoRepresentations: IO[Seq[String]] = defaultUrlToIoRep,
      contentObjectsFromReps: IO[Seq[Entity]] = defaultContentObjectsFromRep,
      bitstreamInfo: Seq[IO[Seq[BitStreamInfo]]] = defaultBitStreamInfo
  ) extends Lambda {
    override lazy val entitiesClientIO: IO[EntityClient[IO, Fs2Streams[IO]]] = {
      when(
        mockEntityClient.entitiesByIdentifier(any[Identifier])
      ).thenReturn(entitiesWithIdentifier)

      when(
        mockEntityClient.getUrlsToIoRepresentations(any[UUID], any[Option[RepresentationType]])
      ).thenReturn(urlsToIoRepresentations)

      when(
        mockEntityClient.getContentObjectsFromRepresentation(any[UUID], any[RepresentationType], any[Int])
      ).thenReturn(contentObjectsFromReps)

      when(
        mockEntityClient.getBitstreamInfo(any[UUID])
      ).thenReturn(bitstreamInfo.head, bitstreamInfo(1))

      IO(mockEntityClient)
    }

    val creds: StaticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))

    private val asyncDynamoClient: DynamoDbAsyncClient = DynamoDbAsyncClient
      .builder()
      .endpointOverride(URI.create("http://localhost:9005"))
      .region(Region.EU_WEST_2)
      .credentialsProvider(creds)
      .build()
    override val dADynamoDBClient: DADynamoDBClient[IO] = new DADynamoDBClient[IO](asyncDynamoClient)

    private val mockEntityClient: EntityClient[IO, Fs2Streams[IO]] = mock[EntityClient[IO, Fs2Streams[IO]]]

    def verifyInvocationsAndArgumentsPassed(
        numOfEntitiesByIdentifierInvocations: Int = 1,
        numOfGetUrlsToIoRepresentationsRequests: Int = 1,
        numOfGetContentObjectsFromRepresentationRequests: Int = 1,
        numOfGetBitstreamInfoRequests: Int = 2
    ): Unit = {
      val entitiesByIdentifierIdentifierToGetCaptor = getIdentifierToGetCaptor

      verify(mockEntityClient, times(numOfEntitiesByIdentifierInvocations)).entitiesByIdentifier(
        entitiesByIdentifierIdentifierToGetCaptor.capture()
      )

      if (numOfEntitiesByIdentifierInvocations > 0)
        entitiesByIdentifierIdentifierToGetCaptor.getValue should be(Identifier("SourceID", "Test Name"))

      val ioEntityRefForUrlsRequestCaptor = getIoEntityRefCaptor
      val optionalRepresentationTypeCaptorRequestCaptor = getOptionalRepresentationTypeCaptor

      verify(mockEntityClient, times(numOfGetUrlsToIoRepresentationsRequests)).getUrlsToIoRepresentations(
        ioEntityRefForUrlsRequestCaptor.capture(),
        optionalRepresentationTypeCaptorRequestCaptor.capture()
      )

      if (numOfGetUrlsToIoRepresentationsRequests > 0) {
        ioEntityRefForUrlsRequestCaptor.getValue should be(UUID.fromString("354f47cf-3ca2-4a4e-8181-81b714334f00"))
        optionalRepresentationTypeCaptorRequestCaptor.getValue should be(Some(Preservation))
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
        ioEntityRefForContentObjectsRequestCaptor.getValue should be(
          UUID.fromString("354f47cf-3ca2-4a4e-8181-81b714334f00")
        )
        representationTypeCaptorRequestCaptor.getValue should be(Preservation)
        versionCaptor.getValue should be(1)
      }

      val contentRefRequestCaptor = getContentRef

      verify(mockEntityClient, times(numOfGetBitstreamInfoRequests)).getBitstreamInfo(contentRefRequestCaptor.capture())

      if (numOfGetBitstreamInfoRequests > 0)
        contentRefRequestCaptor.getValue should be(UUID.fromString("4dee285b-64e4-49f8-942e-84ab460b5af6"))

      ()
    }

    def getIdentifierToGetCaptor: ArgumentCaptor[Identifier] = ArgumentCaptor.forClass(classOf[Identifier])

    def getIoEntityRefCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])

    def getOptionalRepresentationTypeCaptor: ArgumentCaptor[Option[RepresentationType]] =
      ArgumentCaptor.forClass(classOf[Option[RepresentationType]])

    def getRepresentationTypeCaptor: ArgumentCaptor[RepresentationType] =
      ArgumentCaptor.forClass(classOf[RepresentationType])

    def getVersion: ArgumentCaptor[Int] = ArgumentCaptor.forClass(classOf[Int])

    def getContentRef: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
  }
}
