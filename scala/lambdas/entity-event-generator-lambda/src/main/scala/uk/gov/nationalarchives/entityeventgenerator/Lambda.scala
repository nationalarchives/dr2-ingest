package uk.gov.nationalarchives.entityeventgenerator

import cats.effect.IO
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import io.circe.Encoder
import org.scanamo.generic.auto.*
import pureconfig.ConfigReader
import software.amazon.awssdk.services.dynamodb.model.*
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbRequest
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.entityeventgenerator.Lambda.*
import uk.gov.nationalarchives.utils.EventCodecs.given
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}

import java.time.{Instant, OffsetDateTime}

class Lambda extends LambdaRunner[ScheduledEvent, Int, Config, Dependencies] {
  private val dateItemPrimaryKeyAndValue =
    Map("id" -> AttributeValue.builder().s("LastPolled").build())
  private val datetimeField = "eventDatetime"

  given Encoder[CompactEntity] =
    Encoder.forProduct2("id", "deleted")(entity => (entity.id, entity.deleted))

  private enum IgnoredEventTypes:
    case Download, Characterise, VirusCheck

  private def publishUpdatedEntitiesAndUpdateDateTime(
      config: Config,
      entityClient: EntityClient[IO, Fs2Streams[IO]],
      dADynamoDBClient: DADynamoDBClient[IO],
      dASnsDBClient: DASNSClient[IO],
      eventTriggeredDatetime: OffsetDateTime
  ): IO[Int] =
    for {
      numOfRecentlyUpdatedEntities <- getEntitiesUpdatedAndUpdateDB(
        config,
        entityClient,
        dADynamoDBClient,
        dASnsDBClient,
        eventTriggeredDatetime
      )
      _ <-
        if numOfRecentlyUpdatedEntities > 0 then
          publishUpdatedEntitiesAndUpdateDateTime(
            config,
            entityClient,
            dADynamoDBClient,
            dASnsDBClient,
            eventTriggeredDatetime
          )
        else IO.pure(numOfRecentlyUpdatedEntities)
    } yield numOfRecentlyUpdatedEntities

  private def getEntitiesUpdatedAndUpdateDB(
      config: Config,
      entitiesClient: EntityClient[IO, Fs2Streams[IO]],
      dADynamoDBClient: DADynamoDBClient[IO],
      dASnsDBClient: DASNSClient[IO],
      eventTriggeredDatetime: OffsetDateTime
  ): IO[Int] = {
    val ignoredEventTypes = IgnoredEventTypes.values.map(_.toString)
    for {
      updatedSinceResponses <- dADynamoDBClient.getItems[GetItemsResponse, PartitionKey](
        List(PartitionKey("LastPolled")),
        config.lastEventActionTableName
      )
      updatedSinceResponse = updatedSinceResponses.head
      updatedSinceAsDate = OffsetDateTime.parse(updatedSinceResponse.eventDatetime).toZonedDateTime
      currentStart = updatedSinceResponse.startAt
      recentlyUpdatedEntities <- entitiesClient.entitiesUpdatedSince(updatedSinceAsDate, currentStart)
      _ <- logger.info(s"There were ${recentlyUpdatedEntities.length} entities updated since $updatedSinceAsDate")

      entityLastEventActionDate <-
        if !recentlyUpdatedEntities.forall(_.deleted) then
          val lastUpdatedEntity: Entity = recentlyUpdatedEntities.filterNot(_.deleted).last
          entitiesClient.entityEventActions(lastUpdatedEntity).map { entityEventActions =>
            Some(entityEventActions.filterNot(ev => ignoredEventTypes.contains(ev.eventType)).head.dateOfEvent.toOffsetDateTime)
          }
        else if recentlyUpdatedEntities.nonEmpty && recentlyUpdatedEntities
            .forall(_.deleted)
        then { // If all entities are deleted, return the updatedSinceAsDate in order to get the next page of results
          IO.pure(Option(updatedSinceAsDate.toOffsetDateTime))
        } else IO.none

      _ <- IO.whenA(entityLastEventActionDate.exists(_.isBefore(eventTriggeredDatetime))) {
        for {
          _ <- dASnsDBClient.publish[CompactEntity](config.snsArn)(convertToCompactEntities(recentlyUpdatedEntities.toList))
          updateDateAttributeValue = AttributeValue.builder().s(entityLastEventActionDate.get.toString).build()
          // This is to cover the case where there are more than 1000 entities with the same last event action date or for when there are only deleted entities in the response.
          nextStart = if entityLastEventActionDate.get.isEqual(OffsetDateTime.parse(updatedSinceResponse.eventDatetime)) then currentStart + 1000 else 0
          startAttributeValue = AttributeValue.builder.n(nextStart.toString).build()
          updateDateRequest = DADynamoDbRequest(
            config.lastEventActionTableName,
            dateItemPrimaryKeyAndValue,
            Map(datetimeField -> updateDateAttributeValue, "startAt" -> startAttributeValue)
          )
          dynamoStatusCode <- dADynamoDBClient.updateAttributeValues(updateDateRequest)
          _ <- logger.info(s"Dynamo updateAttributeValues returned status code $dynamoStatusCode")
        } yield ()
      }
    } yield recentlyUpdatedEntities.length
  }

  override def dependencies(config: Config): IO[Dependencies] = for {
    client <- Fs2Client.entityClient(config.secretName)
  } yield Dependencies(client, DASNSClient[IO](), DADynamoDBClient[IO]())

  private def convertToCompactEntities(entitiesToTransform: List[Entity]): List[CompactEntity] =
    entitiesToTransform.map { entity =>
      val id = entity.entityType
        .map(t => s"${t.entityTypeShort.toLowerCase}:${entity.ref}")
        .getOrElse(entity.ref.toString)
      CompactEntity(id, entity.deleted)
    }

  override def handler: (ScheduledEvent, Config, Dependencies) => IO[Int] = { (event, config, dependencies) =>
    val eventTriggeredDatetime: OffsetDateTime =
      OffsetDateTime.ofInstant(Instant.ofEpochMilli(event.getTime.getMillis), event.getTime.getZone.toTimeZone.toZoneId)
    for {
      numOfEntitiesUpdated <- publishUpdatedEntitiesAndUpdateDateTime(
        config,
        dependencies.entityClient,
        dependencies.daDynamoDBClient,
        dependencies.daSNSClient,
        eventTriggeredDatetime
      )
    } yield numOfEntitiesUpdated
  }

}

object Lambda {
  case class Config(secretName: String, snsArn: String, lastEventActionTableName: String) derives ConfigReader
  case class CompactEntity(id: String, deleted: Boolean)
  private case class PartitionKey(id: String)
  case class GetItemsResponse(eventDatetime: String, startAt: Int)

  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], daSNSClient: DASNSClient[IO], daDynamoDBClient: DADynamoDBClient[IO])
}
