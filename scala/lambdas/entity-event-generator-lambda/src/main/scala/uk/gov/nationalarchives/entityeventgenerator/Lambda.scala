package uk.gov.nationalarchives.entityeventgenerator

import cats.effect.IO
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import io.circe.Encoder
import io.circe.syntax.*
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

import java.nio.file.{Path, StandardOpenOption}
import java.time.{Instant, OffsetDateTime}
import scala.jdk.CollectionConverters.*

class Lambda extends LambdaRunner[ScheduledEvent, Int, Config, Dependencies] {
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
      _ <- IO.println(s"Num of entities $numOfRecentlyUpdatedEntities")
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
    val ignoredEventTypes = List("Download", "Characterise", "VirusCheck")
    for {
      updatedSinceResponses <- dADynamoDBClient.getItems[GetItemsResponse, PartitionKey](
        List(PartitionKey("LastPolled")),
        config.lastEventActionTableName
      )
      updatedSinceResponse = updatedSinceResponses.head
      updatedSinceAsDate = OffsetDateTime.parse(updatedSinceResponse.datetime).toZonedDateTime
      currentStart = updatedSinceResponse.start
      recentlyUpdatedEntities <- entitiesClient.entitiesUpdatedSince(updatedSinceAsDate, currentStart)
      _ <- logger.info(s"There were ${recentlyUpdatedEntities.length} entities updated since $updatedSinceAsDate")

      entityLastEventActionDate <-
        if (recentlyUpdatedEntities.nonEmpty) {
          val lastUpdatedEntity: Entity = recentlyUpdatedEntities.last
          entitiesClient.entityEventActions(lastUpdatedEntity).map { entityEventActions =>
            Some(entityEventActions.filterNot(ev => ignoredEventTypes.contains(ev.eventType)).head.dateOfEvent.toOffsetDateTime)
          }
        } else IO.pure(None)

      _ <- IO.whenA(entityLastEventActionDate.exists(_.isBefore(eventTriggeredDatetime))) {
        val compactEntities: List[CompactEntity] = writeToFile(recentlyUpdatedEntities, "out")
        for {
          _ <- IO.println(s"Published ${compactEntities.size} entries")
          _ <- IO.println(s"Start: $currentStart Updated since: $updatedSinceAsDate New updated since: $entityLastEventActionDate")
          updateDateAttributeValue = AttributeValue.builder().s(entityLastEventActionDate.get.toString).build()
          start = if entityLastEventActionDate.get.isEqual(OffsetDateTime.parse(updatedSinceResponse.datetime)) then currentStart + 1000 else 0
          startAttributeValue = AttributeValue.builder.n(start.toString).build()
          updateDateRequest = DADynamoDbRequest(
            config.lastEventActionTableName,
            dateItemPrimaryKeyAndValue,
            Map(datetimeField -> Some(updateDateAttributeValue), "start" -> Some(startAttributeValue))
          )
          dynamoStatusCode <- dADynamoDBClient.updateAttributeValues(updateDateRequest)
          _ <- logger.info(s"Dynamo updateAttributeValues returned status code $dynamoStatusCode")
        } yield ()
      }
    } yield recentlyUpdatedEntities.length
  }

  private def writeToFile(recentlyUpdatedEntities: Seq[Entity], fileName: String) = {
    val compactEntities = convertToCompactEntities(recentlyUpdatedEntities.toList)
    java.nio.file.Files.write(Path.of("""C:\Users\spalmer\Documents\""" + fileName + ".log"), compactEntities
      .map(row => row.asJson.noSpaces).asJava, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    compactEntities
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
  case class PartitionKey(id: String)
  case class GetItemsResponse(datetime: String, start: Int)

  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], daSNSClient: DASNSClient[IO], daDynamoDBClient: DADynamoDBClient[IO])
}
