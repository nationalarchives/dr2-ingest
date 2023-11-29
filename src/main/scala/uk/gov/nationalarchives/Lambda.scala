package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import io.circe.Encoder
import org.scanamo.generic.auto.genericDerivedFormat
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import software.amazon.awssdk.services.dynamodb.model._
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbRequest
import uk.gov.nationalarchives.Lambda.{CompactEntity, Config, GetItemsResponse, PartitionKey}
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client

import java.time.{Instant, OffsetDateTime}

class Lambda extends RequestHandler[ScheduledEvent, Unit] {
  private val maxEntitiesPerPage: Int = 1000
  private val dateItemPrimaryKeyAndValue =
    Map("id" -> AttributeValue.builder().s("LastPolled").build())
  private val datetimeField = "datetime"
  private val configIo: IO[Config] = ConfigSource.default.loadF[IO, Config]()
  lazy val entitiesClientIO: IO[EntityClient[IO, Fs2Streams[IO]]] = configIo.flatMap { config =>
    Fs2Client.entityClient(config.apiUrl, config.secretName)
  }
  val dADynamoDBClient: DADynamoDBClient[IO] = DADynamoDBClient[IO]()

  val dASnsDBClient: DASNSClient[IO] = DASNSClient[IO]()

  implicit val enc: Encoder[CompactEntity] =
    Encoder.forProduct2("id", "deleted")(entity => (entity.id, entity.deleted))

  override def handleRequest(event: ScheduledEvent, context: Context): Unit = {
    val eventTriggeredDatetime: OffsetDateTime =
      OffsetDateTime.ofInstant(Instant.ofEpochMilli(event.getTime.getMillis), event.getTime.getZone.toTimeZone.toZoneId)

    val entities = for {
      config <- configIo
      entitiesClient <- entitiesClientIO
      numOfEntitiesUpdated <- publishUpdatedEntitiesAndUpdateDateTime(config, entitiesClient, 0, eventTriggeredDatetime)
    } yield numOfEntitiesUpdated

    entities.unsafeRunSync()
  }

  private def publishUpdatedEntitiesAndUpdateDateTime(
      config: Config,
      entityClient: EntityClient[IO, Fs2Streams[IO]],
      startFrom: Int,
      eventTriggeredDatetime: OffsetDateTime
  ): IO[Int] =
    for {
      numOfRecentlyUpdatedEntities <- getEntitiesUpdatedAndUpdateDB(
        config,
        entityClient,
        startFrom,
        eventTriggeredDatetime
      )
      _ <-
        if (numOfRecentlyUpdatedEntities > 0)
          publishUpdatedEntitiesAndUpdateDateTime(
            config,
            entityClient,
            startFrom + maxEntitiesPerPage,
            eventTriggeredDatetime
          )
        else IO(numOfRecentlyUpdatedEntities)
    } yield numOfRecentlyUpdatedEntities

  private def getEntitiesUpdatedAndUpdateDB(
      config: Config,
      entitiesClient: EntityClient[IO, Fs2Streams[IO]],
      startFrom: Int,
      eventTriggeredDatetime: OffsetDateTime
  ): IO[Int] =
    for {
      logger <- Slf4jLogger.create[IO]
      updatedSinceResponses <- dADynamoDBClient.getItems[GetItemsResponse, PartitionKey](
        List(PartitionKey("LastPolled")),
        config.lastEventActionTableName
      )
      updatedSinceResponse = updatedSinceResponses.head
      updatedSinceAsDate = OffsetDateTime.parse(updatedSinceResponse.datetime).toZonedDateTime
      recentlyUpdatedEntities <- entitiesClient.entitiesUpdatedSince(updatedSinceAsDate, startFrom)
      _ <- logger.info(s"There were ${recentlyUpdatedEntities.length} entities updated since $updatedSinceAsDate")

      entityLastEventActionDate <-
        if (recentlyUpdatedEntities.nonEmpty) {
          val lastUpdatedEntity: Entity = recentlyUpdatedEntities.last
          entitiesClient.entityEventActions(lastUpdatedEntity).map { entityEventActions =>
            Some(entityEventActions.head.dateOfEvent.toOffsetDateTime)
          }
        } else IO(None)

      lastEventActionBeforeEventTriggered = entityLastEventActionDate.map(_.isBefore(eventTriggeredDatetime))

      _ <-
        if (lastEventActionBeforeEventTriggered.getOrElse(false)) {
          val compactEntities: List[CompactEntity] = convertToCompactEntities(recentlyUpdatedEntities.toList)

          for {
            _ <- dASnsDBClient.publish[CompactEntity](config.snsArn)(compactEntities)
            updateDateAttributeValue = AttributeValue.builder().s(entityLastEventActionDate.get.toString).build()
            updateDateRequest = DADynamoDbRequest(
              config.lastEventActionTableName,
              dateItemPrimaryKeyAndValue,
              Map(datetimeField -> Some(updateDateAttributeValue))
            )
            statusCode <- dADynamoDBClient.updateAttributeValues(updateDateRequest)
          } yield statusCode
        } else IO(0)
    } yield recentlyUpdatedEntities.length

  private def convertToCompactEntities(entitiesToTransform: List[Entity]): List[CompactEntity] =
    entitiesToTransform.map { entity =>
      val id = entity.entityType
        .map(t => s"${t.entityTypeShort.toLowerCase}:${entity.ref}")
        .getOrElse(entity.ref.toString)
      CompactEntity(id, entity.deleted)
    }
}

object Lambda {
  case class Config(apiUrl: String, secretName: String, snsArn: String, lastEventActionTableName: String)
  case class CompactEntity(id: String, deleted: Boolean)
  case class PartitionKey(id: String)
  case class GetItemsResponse(datetime: String)
}
