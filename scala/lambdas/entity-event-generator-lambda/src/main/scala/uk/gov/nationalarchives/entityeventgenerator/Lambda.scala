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
      _ <- logger.info(s"Getting entities - event triggered time: $eventTriggeredDatetime")
      updatedSinceResponses <- dADynamoDBClient.getItems[GetItemsResponse, PartitionKey](
        List(PartitionKey("LastPolled")),
        config.lastEventActionTableName
      )
      updatedSinceResponse = updatedSinceResponses.head
      updatedSinceAsDate = OffsetDateTime.parse(updatedSinceResponse.eventDatetime).toZonedDateTime
      currentStart = updatedSinceResponse.startAt
      entitiesUpdated <- entitiesClient.entitiesUpdatedSince(updatedSinceAsDate, currentStart)
      recentlyUpdatedEntities = entitiesUpdated.entities
      _ <- logger.info(s"There were ${recentlyUpdatedEntities.length} entities updated since $updatedSinceAsDate with startAt $currentStart")
      entityLastEventActionDate <-
        if recentlyUpdatedEntities.nonEmpty && (entitiesUpdated.hasNext || recentlyUpdatedEntities.forall(_.deleted)) then
          // If there is another page, return the updatedSinceAsDate in order to get the next page of results.
          // If all entities are deleted, we can't get the event actions so return the same updatedSinceAsDate.
          IO.pure(Option(updatedSinceAsDate.toOffsetDateTime))
        else if recentlyUpdatedEntities.nonEmpty then
          val lastUpdatedEntity: Entity = recentlyUpdatedEntities.filterNot(_.deleted).last
          entitiesClient.entityEventActions(lastUpdatedEntity).map { entityEventActions =>
            Some(entityEventActions.filterNot(ev => ignoredEventTypes.contains(ev.eventType)).head.dateOfEvent.toOffsetDateTime)
          }
        else IO.none

      count <-
        if entityLastEventActionDate.exists(_.isBefore(eventTriggeredDatetime)) then
          val updateDateAttributeValue = AttributeValue.builder().s(entityLastEventActionDate.get.toString).build()
          val nextStart =
            if recentlyUpdatedEntities.nonEmpty then currentStart + recentlyUpdatedEntities.length
            else 0
          val startAttributeValue = AttributeValue.builder.n(nextStart.toString).build()
          val updateDateRequest = DADynamoDbRequest(
            config.lastEventActionTableName,
            dateItemPrimaryKeyAndValue,
            Map(datetimeField -> updateDateAttributeValue, "startAt" -> startAttributeValue)
          )
          for
            _ <- dASnsDBClient.publish[CompactEntity](config.snsArn)(convertToCompactEntities(recentlyUpdatedEntities.toList))
            dynamoStatusCode <- dADynamoDBClient.updateAttributeValues(updateDateRequest)
            _ <- logger.info(s"Dynamo updateAttributeValues returned status code $dynamoStatusCode")
          yield recentlyUpdatedEntities.length
        else {
          // Sometimes the last action date is after the triggered datetime.
          // In that case, we want to terminate the lambda and wait for the next invocation.
          IO.pure(0)
        }
    } yield count
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
