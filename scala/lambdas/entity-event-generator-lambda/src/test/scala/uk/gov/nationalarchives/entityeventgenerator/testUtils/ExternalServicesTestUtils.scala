package uk.gov.nationalarchives.entityeventgenerator.testUtils

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import io.circe.Encoder
import org.joda.time.DateTime
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.sns.model.PublishBatchResponse
import sttp.capabilities
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}
import uk.gov.nationalarchives.dp.client.DataProcessor.EventAction
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.{Client, DataProcessor, Entities, EntityClient}
import uk.gov.nationalarchives.entityeventgenerator.Lambda
import uk.gov.nationalarchives.entityeventgenerator.Lambda.{CompactEntity, Config, Dependencies, GetItemsResponse}
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.SecurityTag.*
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

object ExternalServicesTestUtils {

  val config: Config = Config("", "", "arn:aws:sns:eu-west-2:123456789012:MockResourceId", "table-name")

  def event(time: String): ScheduledEvent = {
    val event = new ScheduledEvent()
    event.setTime(DateTime.parse(time))
    event
  }

  def notImplemented[T]: IO[T] = IO.raiseError(new Exception("Not implemented"))

  def preservicaClient(ref: Ref[IO, List[Entity]], eventActions: List[EventAction], errors: Option[Errors]): EntityClient[IO, Fs2Streams[IO]] =
    new EntityClient[IO, Fs2Streams[IO]]:

      override val dateFormatter: DateTimeFormatter = DateTimeFormatter.BASIC_ISO_DATE

      override def metadataForEntity(entity: Entity): IO[EntityClient.EntityMetadata] = notImplemented

      override def getBitstreamInfo(contentRef: UUID): IO[Seq[Client.BitStreamInfo]] = notImplemented

      override def getEntity(entityRef: UUID, entityType: EntityClient.EntityType): IO[Entity] = notImplemented

      override def getEntityIdentifiers(entity: Entity): IO[Seq[Entities.IdentifierResponse]] = notImplemented

      override def getUrlsToIoRepresentations(ioEntityRef: UUID, representationType: Option[EntityClient.RepresentationType]): IO[Seq[String]] = notImplemented

      override def getContentObjectsFromRepresentation(ioEntityRef: UUID, representationType: EntityClient.RepresentationType, repTypeIndex: Int): IO[Seq[Entity]] = notImplemented

      override def addEntity(addEntityRequest: EntityClient.AddEntityRequest): IO[UUID] = notImplemented

      override def updateEntity(updateEntityRequest: EntityClient.UpdateEntityRequest): IO[String] = notImplemented

      override def updateEntityIdentifiers(entity: Entity, identifiers: Seq[Entities.IdentifierResponse]): IO[Seq[Entities.IdentifierResponse]] = notImplemented

      override def streamBitstreamContent[T](stream: capabilities.Streams[Fs2Streams[IO]])(url: String, streamFn: stream.BinaryStream => IO[T]): IO[T] = notImplemented

      override def entitiesUpdatedSince(dateTime: ZonedDateTime, startEntry: Int, maxEntries: Int): IO[Seq[Entity]] = ref.getAndUpdate { existing =>
        Nil
      }

      override def entityEventActions(entity: Entity, startEntry: Int, maxEntries: Int): IO[Seq[DataProcessor.EventAction]] =
        IO.raiseWhen(errors.exists(_.getEventActionsError))(new Exception("Error getting event actions")) >> IO.pure(eventActions)

      override def entitiesByIdentifier(identifier: EntityClient.Identifier): IO[Seq[Entity]] = notImplemented

      override def addIdentifierForEntity(entityRef: UUID, entityType: EntityClient.EntityType, identifier: EntityClient.Identifier): IO[String] = notImplemented

      override def getPreservicaNamespaceVersion(endpoint: String): IO[Float] = notImplemented

  def dynamoClient(ref: Ref[IO, List[String]], errors: Option[Errors]): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = notImplemented

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      notImplemented

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] =
      IO.raiseWhen(errors.exists(_.getItemsError))(new Exception("Error getting items from Dynamo")) >>
        ref.get.map(existing => existing.map(GetItemsResponse.apply).map(_.asInstanceOf[T]))

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] =
      IO.raiseWhen(errors.exists(_.updateAttributeValuesError))(new Exception("Error updating Dynamo attribute values")) >>
        ref
          .update { _ =>
            dynamoDbRequest.attributeNamesAndValuesToUpdate.getOrElse("datetime", None).map(_.s()).toList
          }
          .map(_ => 1)

  def snsClient(ref: Ref[IO, List[CompactEntity]], errors: Option[Errors]): DASNSClient[IO] = new DASNSClient[IO]:
    override def publish[T <: Product](topicArn: String)(messages: List[T])(using enc: Encoder[T]): IO[List[PublishBatchResponse]] =
      IO.raiseWhen(errors.exists(_.publishError))(new Exception("Error publishing to SNS")) >>
        ref
          .update { existing =>
            existing ++ messages.collect { case ce: CompactEntity => ce }
          }
          .map(_ => Nil)

  case class Errors(getItemsError: Boolean = false, updateAttributeValuesError: Boolean = false, getEventActionsError: Boolean = false, publishError: Boolean = false)

  def runLambda(
      event: ScheduledEvent,
      entities: List[Entity],
      eventActions: List[EventAction],
      dateTimes: List[String],
      errors: Option[Errors] = None
  ): (List[String], List[CompactEntity], Either[Throwable, Int]) = (for {
    preservicaRef <- Ref.of[IO, List[Entity]](entities)
    dynamoRef <- Ref.of[IO, List[String]](dateTimes)
    snsRef <- Ref.of[IO, List[CompactEntity]](Nil)
    dependencies = Dependencies(preservicaClient(preservicaRef, eventActions, errors), snsClient(snsRef, errors), dynamoClient(dynamoRef, errors))
    res <- new Lambda().handler(event, config, dependencies).attempt
    dynamoResult <- dynamoRef.get
    snsResult <- snsRef.get
  } yield (dynamoResult, snsResult, res)).unsafeRunSync()

  def generateEventAction(time: String): EventAction = EventAction(
    UUID.randomUUID,
    "event",
    ZonedDateTime.parse(time)
  )

  def generateEntity: Entity = Entity(
    Option(InformationObject),
    UUID.randomUUID,
    Some(s"mock title"),
    Some(s"mock description"),
    deleted = false,
    Option(InformationObject.entityPath),
    Some(Open),
    None
  )
}
