package testUtils

import cats.effect.IO
import com.github.tomakehurst.wiremock.WireMockServer
import io.circe.Encoder
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.sns.model.PublishBatchResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient.DynamoDbRequest
import uk.gov.nationalarchives.dp.client.DataProcessor.EventAction
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}
import uk.gov.nationalarchives.dr2.Lambda
import uk.gov.nationalarchives.dr2.Lambda.CompactEntity

import java.time.ZonedDateTime
import java.util.UUID

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

  case class MockLambda(
      entitiesUpdatedSinceReturnValue: List[IO[Seq[Entity]]] = List(
        IO(
          Seq(
            Entity(
              "CO",
              UUID.fromString("4148ffe3-fffc-4252-9676-595c22b4fcd2"),
              Some("test file1"),
              deleted = false,
              "path/1"
            ),
            Entity(
              "IO",
              UUID.fromString("7f094550-7af2-4dc3-a954-9cd7f5c25d7f"),
              Some("test file2"),
              deleted = false,
              "path/2"
            ),
            Entity(
              "SO",
              UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50"),
              Some("test file3"),
              deleted = false,
              "path/3"
            )
          )
        ),
        IO(
          Seq(
            Entity(
              "SO",
              UUID.fromString("b10d021d-c013-48b1-90f9-e4ccc6149602"),
              Some("test file4"),
              deleted = false,
              "path/4"
            ),
            Entity(
              "IO",
              UUID.fromString("e9f6182f-f1b4-4683-89be-9505f5c943ec"),
              Some("test file5"),
              deleted = false,
              "path/5"
            ),
            Entity(
              "CO",
              UUID.fromString("97f49c11-3be4-4ffa-980d-e698d4faa52a"),
              Some("test file6"),
              deleted = false,
              "path/6"
            )
          )
        )
      ),
      entityEventActionsReturnValue: IO[Seq[EventAction]] = IO(
        Seq(
          EventAction(
            UUID.fromString("f24313ce-dd5d-4b28-9ebc-b47893f55a8e"),
            "Ingest",
            ZonedDateTime.parse("2023-06-06T20:39:53.377170+01:00")
          )
        )
      ),
      getAttributeValuesReturnValue: IO[Map[String, AttributeValue]] = IO(
        Map("datetime" -> AttributeValue.builder().s("2023-06-06T20:39:53.377170+01:00").build())
      ),
      snsPublishReturnValue: IO[List[PublishBatchResponse]] = IO(List(PublishBatchResponse.builder().build())),
      updateAttributeValuesReturnValue: IO[Int] = IO(200)
  ) extends Lambda() {
    val apiUrlCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val updatedSinceCaptor: ArgumentCaptor[ZonedDateTime] = ArgumentCaptor.forClass(classOf[ZonedDateTime])
    val entitiesSecretNameCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val eventActionsSecretNameCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val entitiesStartFromCaptor: ArgumentCaptor[Int] = ArgumentCaptor.forClass(classOf[Int])
    val eventActionsStartFromCaptor: ArgumentCaptor[Int] = ArgumentCaptor.forClass(classOf[Int])

    val entityCaptor: ArgumentCaptor[Entity] = ArgumentCaptor.forClass(classOf[Entity])

    val getAttributeValuesCaptor: ArgumentCaptor[DynamoDbRequest] = ArgumentCaptor.forClass(classOf[DynamoDbRequest])
    val updateAttributeValuesCaptor: ArgumentCaptor[DynamoDbRequest] = ArgumentCaptor.forClass(classOf[DynamoDbRequest])

    val snsArnCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val publishEntitiesCaptor: ArgumentCaptor[List[CompactEntity]] =
      ArgumentCaptor.forClass(classOf[List[CompactEntity]])

    val mockEntityClient: EntityClient[IO, Fs2Streams[IO]] = mock[EntityClient[IO, Fs2Streams[IO]]]
    val mockDynamoDBClient: DADynamoDBClient[IO] = mock[DADynamoDBClient[IO]]
    val mockSnsClient: DASNSClient[IO] = mock[DASNSClient[IO]]

    override def entitiesClientIO: IO[EntityClient[IO, Fs2Streams[IO]]] = {
      when(mockEntityClient.entitiesUpdatedSince(any[ZonedDateTime], any[String], any[Int], any[Int]))
        .thenReturn(
          entitiesUpdatedSinceReturnValue.headOption.getOrElse(IO(Nil)),
          if (entitiesUpdatedSinceReturnValue.length > 1) entitiesUpdatedSinceReturnValue(1) else IO(Nil),
          IO(Nil)
        )
      when(mockEntityClient.entityEventActions(any[Entity], any[String], any[Int], any[Int]))
        .thenReturn(entityEventActionsReturnValue)
      IO(mockEntityClient)
    }

    override def dADynamoDBClient: DADynamoDBClient[IO] = {
      when(mockDynamoDBClient.getAttributeValues(any[DynamoDbRequest])).thenReturn(getAttributeValuesReturnValue)
      when(mockDynamoDBClient.updateAttributeValues(any[DynamoDbRequest])).thenReturn(updateAttributeValuesReturnValue)
      mockDynamoDBClient
    }

    override def dASnsDBClient: DASNSClient[IO] = {
      when(mockSnsClient.publish(any[String])(any[List[CompactEntity]])(any[Encoder[CompactEntity]]))
        .thenReturn(snsPublishReturnValue)
      mockSnsClient
    }

    def verifyInvocationsAndArgumentsPassed(
        numOfGetAttributeValuesInvocations: Int,
        numOfEntitiesUpdatedSinceInvocations: Int,
        numOfEntityEventActionsInvocations: Int,
        numOfPublishInvocations: Int,
        numOfUpdateAttributeValuesInvocations: Int
    ): Unit = {

      verify(mockDynamoDBClient, times(numOfGetAttributeValuesInvocations)).getAttributeValues(
        getAttributeValuesCaptor.capture()
      )
      getAttributeValuesCaptor.getAllValues.toArray.toList should be(
        List.fill(numOfGetAttributeValuesInvocations)(
          DynamoDbRequest(
            "table-name",
            Map("id" -> AttributeValue.builder().s("LastPolled").build()),
            Map("datetime" -> None)
          )
        )
      )

      verify(mockEntityClient, times(numOfEntitiesUpdatedSinceInvocations)).entitiesUpdatedSince(
        updatedSinceCaptor.capture(),
        entitiesSecretNameCaptor.capture(),
        entitiesStartFromCaptor.capture(),
        any[Int]
      )
      updatedSinceCaptor.getAllValues.toArray.toList should be(
        List.fill(numOfEntitiesUpdatedSinceInvocations)(ZonedDateTime.parse("2023-06-06T20:39:53.377170+01:00"))
      )
      entitiesSecretNameCaptor.getAllValues.toArray.toList should be(
        List.fill(numOfEntitiesUpdatedSinceInvocations)("mockSecretName")
      )
      entitiesStartFromCaptor.getAllValues.toArray.toList should be(
        List(0, 1000, 2000).take(numOfEntitiesUpdatedSinceInvocations)
      )

      verify(mockEntityClient, times(numOfEntityEventActionsInvocations)).entityEventActions(
        entityCaptor.capture(),
        eventActionsSecretNameCaptor.capture(),
        eventActionsStartFromCaptor.capture(),
        any[Int]
      )
      entityCaptor.getAllValues.toArray.toList should be(
        List(
          Entity(
            "SO",
            UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50"),
            Some("test file3"),
            deleted = false,
            "path/3"
          ),
          Entity(
            "CO",
            UUID.fromString("97f49c11-3be4-4ffa-980d-e698d4faa52a"),
            Some("test file6"),
            deleted = false,
            "path/6"
          )
        ).take(numOfEntityEventActionsInvocations)
      )
      eventActionsSecretNameCaptor.getAllValues.toArray.toList should be(
        List.fill(numOfEntityEventActionsInvocations)("mockSecretName")
      )
      eventActionsStartFromCaptor.getAllValues.toArray.toList should be(
        List.fill(numOfEntityEventActionsInvocations)(0)
      )

      verify(mockSnsClient, times(numOfPublishInvocations)).publish(
        snsArnCaptor.capture()
      )(publishEntitiesCaptor.capture())(any[Encoder[CompactEntity]])
      snsArnCaptor.getAllValues.toArray.toList should be(
        List.fill(numOfPublishInvocations)("arn:aws:sns:eu-west-2:123456789012:MockResourceId")
      )
      publishEntitiesCaptor.getAllValues.toArray.toList should be(
        List(
          List(
            CompactEntity("co:4148ffe3-fffc-4252-9676-595c22b4fcd2", deleted = false),
            CompactEntity("io:7f094550-7af2-4dc3-a954-9cd7f5c25d7f", deleted = false),
            CompactEntity("so:d7879799-a7de-4aa6-8c7b-afced66a6c50", deleted = false)
          ),
          List(
            CompactEntity("so:b10d021d-c013-48b1-90f9-e4ccc6149602", deleted = false),
            CompactEntity("io:e9f6182f-f1b4-4683-89be-9505f5c943ec", deleted = false),
            CompactEntity("co:97f49c11-3be4-4ffa-980d-e698d4faa52a", deleted = false)
          )
        ).take(numOfPublishInvocations)
      )

      verify(mockDynamoDBClient, times(numOfUpdateAttributeValuesInvocations)).updateAttributeValues(
        updateAttributeValuesCaptor.capture()
      )
      updateAttributeValuesCaptor.getAllValues.toArray.toList should be(
        List.fill(numOfUpdateAttributeValuesInvocations)(
          DynamoDbRequest(
            "table-name",
            Map("id" -> AttributeValue.builder().s("LastPolled").build()),
            Map("datetime" -> Some(AttributeValue.builder().s("2023-06-06T20:39:53.377170+01:00").build()))
          )
        )
      )
      ()
    }
  }
}
