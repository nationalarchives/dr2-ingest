package uk.gov.nationalarchives.testUtils

import cats.effect.IO
import com.github.tomakehurst.wiremock.WireMockServer
import io.circe.Encoder
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchersSugar.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scanamo.DynamoFormat
import software.amazon.awssdk.services.sns.model.PublishBatchResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.Lambda.{Dependencies, GetDr2PreservicaVersionResponse, LatestPreservicaVersionMessage, PartitionKey}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}

class ExternalServicesTestUtils extends AnyFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll {
  val graphQlServerPort = 9001

  val wiremockGraphqlServer = new WireMockServer(graphQlServerPort)

  override def beforeAll(): Unit = {
    wiremockGraphqlServer.start()
  }

  override def afterAll(): Unit = {
    wiremockGraphqlServer.stop()
  }

  override def afterEach(): Unit = {
    wiremockGraphqlServer.resetAll()
  }

  case class ArgumentVerifier(
      getPreservicaNamespaceVersionReturnValue: IO[Float] = IO.pure(7.0f),
      getCurrentPreservicaVersionReturnValue: IO[List[GetDr2PreservicaVersionResponse]] = IO.pure(List(GetDr2PreservicaVersionResponse(6.9f))),
      snsPublishReturnValue: IO[List[PublishBatchResponse]] = IO.pure(List(PublishBatchResponse.builder().build()))
  ) {
    val endpointSinceCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val getItemsCaptor: ArgumentCaptor[List[PartitionKey]] = ArgumentCaptor.forClass(classOf[List[PartitionKey]])
    val tableNameCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    val snsArnCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val publishEntitiesCaptor: ArgumentCaptor[List[LatestPreservicaVersionMessage]] =
      ArgumentCaptor.forClass(classOf[List[LatestPreservicaVersionMessage]])

    val mockEntityClient: EntityClient[IO, Fs2Streams[IO]] = mock[EntityClient[IO, Fs2Streams[IO]]]
    val mockDynamoDBClient: DADynamoDBClient[IO] = mock[DADynamoDBClient[IO]]
    val mockSnsClient: DASNSClient[IO] = mock[DASNSClient[IO]]
    when(mockEntityClient.getPreservicaNamespaceVersion(any[String]))
      .thenReturn(getPreservicaNamespaceVersionReturnValue)

    when(
      mockDynamoDBClient
        .getItems[GetDr2PreservicaVersionResponse, PartitionKey](any[List[PartitionKey]], any[String])(
          any[DynamoFormat[GetDr2PreservicaVersionResponse]],
          any[DynamoFormat[PartitionKey]]
        )
    ).thenReturn(getCurrentPreservicaVersionReturnValue)

    when(
      mockSnsClient.publish(any[String])(any[List[LatestPreservicaVersionMessage]])(
        any[Encoder[LatestPreservicaVersionMessage]]
      )
    )
      .thenReturn(snsPublishReturnValue)

    val dependencies: Dependencies = Dependencies(mockEntityClient, mockSnsClient, mockDynamoDBClient)

    def verifyInvocationsAndArgumentsPassed(
        numOfCurrentPreservicaVersionInvocations: Int = 1,
        numOfLatestPreservicaVersionInvocations: Int = 1,
        numOfPublishInvocations: Int = 1
    ): Unit = {
      verify(mockDynamoDBClient, times(numOfCurrentPreservicaVersionInvocations))
        .getItems[GetDr2PreservicaVersionResponse, PartitionKey](
          getItemsCaptor.capture(),
          tableNameCaptor.capture()
        )(any[DynamoFormat[GetDr2PreservicaVersionResponse]], any[DynamoFormat[PartitionKey]])
      (0 until numOfCurrentPreservicaVersionInvocations).foreach { _ =>
        getItemsCaptor.getValue should be(List(PartitionKey("DR2PreservicaVersion")))
        tableNameCaptor.getValue should be("table-name")
      }

      verify(mockEntityClient, times(numOfLatestPreservicaVersionInvocations)).getPreservicaNamespaceVersion(
        endpointSinceCaptor.capture()
      )
      (0 until numOfLatestPreservicaVersionInvocations).foreach { _ =>
        endpointSinceCaptor.getValue should be("by-identifier?type=tnaTest&value=getLatestPreservicaVersion")
      }

      verify(mockSnsClient, times(numOfPublishInvocations)).publish(
        snsArnCaptor.capture()
      )(publishEntitiesCaptor.capture())(any[Encoder[LatestPreservicaVersionMessage]])
      (0 until numOfPublishInvocations).foreach { _ =>
        snsArnCaptor.getValue should be("arn:aws:sns:eu-west-2:123456789012:MockResourceId")
        publishEntitiesCaptor.getValue should be(
          List(
            LatestPreservicaVersionMessage(
              "Preservica has upgraded to version 7.0; we are using 6.9",
              7.0f
            )
          )
        )
      }
      ()
    }
  }
}
