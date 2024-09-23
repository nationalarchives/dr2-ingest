package uk.gov.nationalarchives.entityeventgenerator

import cats.effect.IO
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import io.circe.Encoder
import org.scanamo.generic.auto.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import software.amazon.awssdk.services.dynamodb.model.*
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbRequest
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}
import uk.gov.nationalarchives.utils.EventDecoders.given
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.entityeventgenerator.Lambda.*
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client

import java.time.{Instant, OffsetDateTime}

class Lambda extends LambdaRunner[ScheduledEvent, Int, Config, Dependencies] {
  private val maxEntitiesPerPage: Int = 1000
  private val dateItemPrimaryKeyAndValue =
    Map("id" -> AttributeValue.builder().s("LastPolled").build())
  private val datetimeField = "datetime"

  given Encoder[CompactEntity] =
    Encoder.forProduct2("id", "deleted")(entity => (entity.id, entity.deleted))

  private def publishUpdatedEntitiesAndUpdateDateTime(
      config: Config,
      entityClient: EntityClient[IO, Fs2Streams[IO]],
      dADynamoDBClient: DADynamoDBClient[IO],
      dASnsDBClient: DASNSClient[IO],
      startFrom: Int,
      eventTriggeredDatetime: OffsetDateTime
  ): IO[Int] =
    for {
      numOfRecentlyUpdatedEntities <- getEntitiesUpdatedAndUpdateDB(
        config,
        entityClient,
        dADynamoDBClient,
        dASnsDBClient,
        startFrom,
        eventTriggeredDatetime
      )
      _ <-
        if numOfRecentlyUpdatedEntities > 0 then
          publishUpdatedEntitiesAndUpdateDateTime(
            config,
            entityClient,
            dADynamoDBClient,
            dASnsDBClient,
            startFrom + maxEntitiesPerPage,
            eventTriggeredDatetime
          )
        else IO.pure(numOfRecentlyUpdatedEntities)
    } yield numOfRecentlyUpdatedEntities

  def entityToString(entity: Entity) = s"${entity.entityType.map(_.entityTypeShort).getOrElse("")}:${entity.ref} "

  private def getEntitiesUpdatedAndUpdateDB(
      config: Config,
      entitiesClient: EntityClient[IO, Fs2Streams[IO]],
      dADynamoDBClient: DADynamoDBClient[IO],
      dASnsDBClient: DASNSClient[IO],
      startFrom: Int,
      eventTriggeredDatetime: OffsetDateTime
  ): IO[Int] =
    for {
      updatedSinceResponses <- dADynamoDBClient.getItems[GetItemsResponse, PartitionKey](
        List(PartitionKey("LastPolled")),
        config.lastEventActionTableName
      )
      updatedSinceResponse = updatedSinceResponses.head
      updatedSinceAsDate = OffsetDateTime.parse(updatedSinceResponse.datetime).toZonedDateTime
      recentlyUpdatedEntities <- entitiesClient.entitiesUpdatedSince(updatedSinceAsDate, startFrom)
      _ <- logger.info(s"There were ${recentlyUpdatedEntities.length} entities updated since $updatedSinceAsDate")
      _ <- logger.info(s"The updated entity refs are ${recentlyUpdatedEntities.map(entityToString).mkString("\n")}")

      entityLastEventActionDate <-
        if (recentlyUpdatedEntities.nonEmpty) {
          val lastUpdatedEntity: Entity = recentlyUpdatedEntities.last
          entitiesClient.entityEventActions(lastUpdatedEntity).flatMap { entityEventActions =>
            for {
              _ <- logger
                .info(s"Found ${entityEventActions.map(action => s"${action.eventType}, ${action.dateOfEvent}").mkString("\n")} for entity ${entityToString(lastUpdatedEntity)}")
            } yield Some(entityEventActions.head.dateOfEvent.toOffsetDateTime)

          }
        } else IO.pure(None)
      _ <- IO.whenA(entityLastEventActionDate.exists(_.isBefore(eventTriggeredDatetime))) {
        for {
          _ <- dASnsDBClient.publish[CompactEntity](config.snsArn)(convertToCompactEntities(recentlyUpdatedEntities.toList))
          updateDateAttributeValue = AttributeValue.builder().s(entityLastEventActionDate.get.toString).build()
          updateDateRequest = DADynamoDbRequest(
            config.lastEventActionTableName,
            dateItemPrimaryKeyAndValue,
            Map(datetimeField -> Some(updateDateAttributeValue))
          )
          dynamoStatusCode <- dADynamoDBClient.updateAttributeValues(updateDateRequest)
          _ <- logger.info(s"Dynamo updateAttributeValues returned status code $dynamoStatusCode")
        } yield ()
      }
    } yield recentlyUpdatedEntities.length

  override def dependencies(config: Config): IO[Dependencies] = for {
    client <- Fs2Client.entityClient(config.apiUrl, config.secretName)
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
      _ <- IO.println(s"Event triggered date time $eventTriggeredDatetime")
      numOfEntitiesUpdated <- publishUpdatedEntitiesAndUpdateDateTime(
        config,
        dependencies.entityClient,
        dependencies.daDynamoDBClient,
        dependencies.daSNSClient,
        0,
        eventTriggeredDatetime
      )
    } yield numOfEntitiesUpdated
  }

}

object Lambda {
  case class Config(apiUrl: String, secretName: String, snsArn: String, lastEventActionTableName: String) derives ConfigReader
  case class CompactEntity(id: String, deleted: Boolean)
  case class PartitionKey(id: String)
  case class GetItemsResponse(datetime: String)

  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], daSNSClient: DASNSClient[IO], daDynamoDBClient: DADynamoDBClient[IO])
}
