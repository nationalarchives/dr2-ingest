package uk.gov.nationalarchives.entityeventgenerator.testUtils

import cats.effect.IO
import com.github.tomakehurst.wiremock.WireMockServer
import io.circe.Encoder
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.mockito.{ArgumentCaptor, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatestplus.mockito.MockitoSugar.mock
import org.scanamo.DynamoFormat
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.sns.model.PublishBatchResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbRequest
import uk.gov.nationalarchives.entityeventgenerator.Lambda.{CompactEntity, Dependencies, GetItemsResponse, PartitionKey}
import uk.gov.nationalarchives.dp.client.DataProcessor.EventAction
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.*
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}

import java.time.ZonedDateTime
import java.util.UUID

class ExternalServicesTestUtilsOld extends AnyFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll {
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
      entitiesUpdatedSinceReturnValue: List[IO[Seq[Entity]]] = List(
        IO.pure(
          Seq(
            Entity(
              Option(ContentObject),
              UUID.fromString("4148ffe3-fffc-4252-9676-595c22b4fcd2"),
              Some("test file1"),
              Some("test file1 description"),
              deleted = false,
              Option("path/1")
            ),
            Entity(
              Option(InformationObject),
              UUID.fromString("7f094550-7af2-4dc3-a954-9cd7f5c25d7f"),
              Some("test file2"),
              Some("test file2 description"),
              deleted = false,
              Option("path/2")
            ),
            Entity(
              Option(StructuralObject),
              UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50"),
              Some("test file3"),
              Some("test file3 description"),
              deleted = false,
              Option("path/3")
            )
          )
        ),
        IO.pure(
          Seq(
            Entity(
              Option(StructuralObject),
              UUID.fromString("b10d021d-c013-48b1-90f9-e4ccc6149602"),
              Some("test file4"),
              Some("test file4 description"),
              deleted = false,
              Option("path/4")
            ),
            Entity(
              Option(InformationObject),
              UUID.fromString("e9f6182f-f1b4-4683-89be-9505f5c943ec"),
              Some("test file5"),
              Some("test file5 description"),
              deleted = false,
              Option("path/5")
            ),
            Entity(
              Option(ContentObject),
              UUID.fromString("97f49c11-3be4-4ffa-980d-e698d4faa52a"),
              Some("test file6"),
              Some("test file6 description"),
              deleted = false,
              Option("path/6")
            )
          )
        )
      ),
      entityEventActionsReturnValue: IO[Seq[EventAction]] = IO.pure(
        Seq(
          EventAction(
            UUID.fromString("f24313ce-dd5d-4b28-9ebc-b47893f55a8e"),
            "Ingest",
            ZonedDateTime.parse("2023-06-06T20:39:53.377170+01:00")
          )
        )
      ),
      getAttributeValuesReturnValue: IO[List[GetItemsResponse]] = IO.pure(
        List(GetItemsResponse("2023-06-06T20:39:53.377170+01:00"))
      ),
      snsPublishReturnValue: IO[List[PublishBatchResponse]] = IO(List(PublishBatchResponse.builder().build())),
      updateAttributeValuesReturnValue: IO[Int] = IO.pure(200)
  ) {
    val apiUrlCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val updatedSinceCaptor: ArgumentCaptor[ZonedDateTime] = ArgumentCaptor.forClass(classOf[ZonedDateTime])
    val entitiesStartFromCaptor: ArgumentCaptor[Int] = ArgumentCaptor.forClass(classOf[Int])
    val eventActionsStartFromCaptor: ArgumentCaptor[Int] = ArgumentCaptor.forClass(classOf[Int])

    val entityCaptor: ArgumentCaptor[Entity] = ArgumentCaptor.forClass(classOf[Entity])

    val getItemsCaptor: ArgumentCaptor[List[PartitionKey]] = ArgumentCaptor.forClass(classOf[List[PartitionKey]])
    val tableNameCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val updateAttributeValuesCaptor: ArgumentCaptor[DADynamoDbRequest] =
      ArgumentCaptor.forClass(classOf[DADynamoDbRequest])

    val snsArnCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val publishEntitiesCaptor: ArgumentCaptor[List[CompactEntity]] =
      ArgumentCaptor.forClass(classOf[List[CompactEntity]])

    val mockEntityClient: EntityClient[IO, Fs2Streams[IO]] = mock[EntityClient[IO, Fs2Streams[IO]]]
    val mockDynamoDBClient: DADynamoDBClient[IO] = mock[DADynamoDBClient[IO]]
    val mockSnsClient: DASNSClient[IO] = mock[DASNSClient[IO]]

    Mockito.reset(mockEntityClient, mockSnsClient, mockDynamoDBClient)

    val dependencies: Dependencies = Dependencies(mockEntityClient, mockSnsClient, mockDynamoDBClient)

    when(mockEntityClient.entitiesUpdatedSince(any[ZonedDateTime], any[Int], any[Int]))
      .thenReturn(
        entitiesUpdatedSinceReturnValue.headOption.getOrElse(IO.pure(Nil)),
        if entitiesUpdatedSinceReturnValue.length > 1 then entitiesUpdatedSinceReturnValue(1) else IO.pure(Nil),
        IO.pure(Nil)
      )
    when(mockEntityClient.entityEventActions(any[Entity], any[Int], any[Int]))
      .thenReturn(entityEventActionsReturnValue)

    when(
      mockDynamoDBClient.getItems[GetItemsResponse, PartitionKey](any[List[PartitionKey]], any[String])(using
        any[DynamoFormat[GetItemsResponse]],
        any[DynamoFormat[PartitionKey]]
      )
    ).thenReturn(getAttributeValuesReturnValue)
    when(mockDynamoDBClient.updateAttributeValues(any[DADynamoDbRequest]))
      .thenReturn(updateAttributeValuesReturnValue)

    when(mockSnsClient.publish(any[String])(any[List[CompactEntity]])(using any[Encoder[CompactEntity]]))
      .thenReturn(snsPublishReturnValue)

    def verifyInvocationsAndArgumentsPassed(
        numOfGetAttributeValuesInvocations: Int,
        numOfEntitiesUpdatedSinceInvocations: Int,
        numOfEntityEventActionsInvocations: Int,
        numOfPublishInvocations: Int,
        numOfUpdateAttributeValuesInvocations: Int
    ): Unit = {

      verify(mockDynamoDBClient, times(numOfGetAttributeValuesInvocations)).getItems[GetItemsResponse, PartitionKey](
        getItemsCaptor.capture(),
        tableNameCaptor.capture()
      )(using any[DynamoFormat[GetItemsResponse]], any[DynamoFormat[PartitionKey]])
      getItemsCaptor.getAllValues.toArray.toList should be(
        List.fill(numOfGetAttributeValuesInvocations)(
          List(PartitionKey("LastPolled"))
        )
      )

      tableNameCaptor.getAllValues.toArray.toList should be(
        List.fill(numOfGetAttributeValuesInvocations)("table-name")
      )

      verify(mockEntityClient, times(numOfEntitiesUpdatedSinceInvocations)).entitiesUpdatedSince(
        updatedSinceCaptor.capture(),
        entitiesStartFromCaptor.capture(),
        any[Int]
      )
      updatedSinceCaptor.getAllValues.toArray.toList should be(
        List.fill(numOfEntitiesUpdatedSinceInvocations)(ZonedDateTime.parse("2023-06-06T20:39:53.377170+01:00"))
      )
      entitiesStartFromCaptor.getAllValues.toArray.toList should be(
        List(0, 1000, 2000).take(numOfEntitiesUpdatedSinceInvocations)
      )

      verify(mockEntityClient, times(numOfEntityEventActionsInvocations)).entityEventActions(
        entityCaptor.capture(),
        eventActionsStartFromCaptor.capture(),
        any[Int]
      )
      entityCaptor.getAllValues.toArray.toList should be(
        List(
          Entity(
            Option(StructuralObject),
            UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50"),
            Some("test file3"),
            Some("test file3 description"),
            deleted = false,
            Option("path/3")
          ),
          Entity(
            Option(ContentObject),
            UUID.fromString("97f49c11-3be4-4ffa-980d-e698d4faa52a"),
            Some("test file6"),
            Some("test file6 description"),
            deleted = false,
            Option("path/6")
          )
        ).take(numOfEntityEventActionsInvocations)
      )
      eventActionsStartFromCaptor.getAllValues.toArray.toList should be(
        List.fill(numOfEntityEventActionsInvocations)(0)
      )

      verify(mockSnsClient, times(numOfPublishInvocations)).publish(
        snsArnCaptor.capture()
      )(publishEntitiesCaptor.capture())(using any[Encoder[CompactEntity]])
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
          DADynamoDbRequest(
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
