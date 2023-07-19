package uk.gov.nationalarchives.dr2

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import io.circe.Encoder
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import software.amazon.awssdk.services.dynamodb.model._
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient.DynamoDbRequest
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}
import uk.gov.nationalarchives.dr2.Lambda.{CompactEntity, Config}

import java.time.OffsetDateTime
import java.util.{Date, TimeZone}

class Lambda extends RequestHandler[ScheduledEvent, Unit] {
  private val maxEntitiesPerPage: Int = 1000
  private val dateItemPrimaryKeyAndValue =
    Map("id" -> AttributeValue.builder().s("LastPolled").build())
  private val datetimeField = "datetime"

  private val configIo: IO[Config] = ConfigSource.default.loadF[IO, Config]()
  lazy val entitiesClientIO: IO[EntityClient[IO, Fs2Streams[IO]]] = configIo.flatMap { config =>
    Fs2Client.entityClient(config.apiUrl)
  }

  val dADynamoDBClient: DADynamoDBClient[IO] = DADynamoDBClient[IO]()

  val dASnsDBClient: DASNSClient[IO] = DASNSClient[IO]()

  implicit val enc: Encoder[CompactEntity] =
    Encoder.forProduct2("id", "deleted")(entity => (entity.id, entity.deleted))

  override def handleRequest(event: ScheduledEvent, context: Context): Unit = {
    val offset = if (TimeZone.getTimeZone("Europe/London").inDaylightTime(new Date())) "+0100" else "+0000"
    val datetimeOfEventString: String = event.getTime.toString().replace("Z", offset)
    val eventTriggeredDatetime: OffsetDateTime = OffsetDateTime.parse(datetimeOfEventString)

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
      updatedSinceAttributes <- dADynamoDBClient.getAttributeValues(
        DynamoDbRequest(config.lastEventActionTableName, dateItemPrimaryKeyAndValue, Map(datetimeField -> None))
      )
      updatedSinceAttributeValue = updatedSinceAttributes(datetimeField)
      updatedSinceAsDate = OffsetDateTime.parse(updatedSinceAttributeValue.s()).toZonedDateTime
      recentlyUpdatedEntities <- entitiesClient.entitiesUpdatedSince(updatedSinceAsDate, config.secretName, startFrom)
      // TODO convert println method to a logging one, once the assembly logging plugin is working
      _ <- IO.println(s"There were ${recentlyUpdatedEntities.length} entities updated since $updatedSinceAsDate")

      entityLastEventActionDate <-
        if (recentlyUpdatedEntities.nonEmpty) {
          val lastUpdatedEntity: Entity = recentlyUpdatedEntities.last
          entitiesClient.entityEventActions(lastUpdatedEntity, config.secretName).map { entityEventActions =>
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
            updateDateRequest = DynamoDbRequest(
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
      val id = s"${entity.entityType.toLowerCase()}:${entity.ref}"
      CompactEntity(id, entity.deleted)
    }
}

object Lambda {
  case class Config(apiUrl: String, secretName: String, snsArn: String, lastEventActionTableName: String)
  case class CompactEntity(id: String, deleted: Boolean)
}
