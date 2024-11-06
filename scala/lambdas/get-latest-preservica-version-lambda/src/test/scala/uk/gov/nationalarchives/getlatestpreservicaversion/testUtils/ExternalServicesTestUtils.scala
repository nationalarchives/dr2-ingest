package uk.gov.nationalarchives.getlatestpreservicaversion.testUtils

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent
import io.circe.Encoder
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.sns.model.PublishBatchResponse
import sttp.capabilities
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}
import uk.gov.nationalarchives.dp.client.{Client, DataProcessor, Entities, EntityClient}
import uk.gov.nationalarchives.getlatestpreservicaversion.Lambda
import uk.gov.nationalarchives.getlatestpreservicaversion.Lambda.{Config, Dependencies, GetDr2PreservicaVersionResponse, LatestPreservicaVersionMessage}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

object ExternalServicesTestUtils:

  def notImplemented[T]: IO[T] = IO.raiseError(new Exception("Not implemented"))

  def dynamoClient(responses: List[GetDr2PreservicaVersionResponse], errors: Option[Errors]): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = notImplemented

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      notImplemented

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] =
      errors.raise(_.dynamoError, "Error getting version from Dynamo") >> IO.pure(responses.map(_.asInstanceOf[T]))

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = notImplemented

  def preservicaClient(version: Float, errors: Option[Errors]): EntityClient[IO, Fs2Streams[IO]] = new EntityClient[IO, Fs2Streams[IO]] {

    override val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME

    override def metadataForEntity(entity: Entities.Entity): IO[EntityClient.EntityMetadata] = notImplemented

    override def getBitstreamInfo(contentRef: UUID): IO[Seq[Client.BitStreamInfo]] = notImplemented

    override def getEntity(entityRef: UUID, entityType: EntityClient.EntityType): IO[Entities.Entity] = notImplemented

    override def getEntityIdentifiers(entity: Entities.Entity): IO[Seq[Entities.IdentifierResponse]] = notImplemented

    override def getUrlsToIoRepresentations(ioEntityRef: UUID, representationType: Option[EntityClient.RepresentationType]): IO[Seq[String]] = notImplemented

    override def getContentObjectsFromRepresentation(ioEntityRef: UUID, representationType: EntityClient.RepresentationType, repTypeIndex: Int): IO[Seq[Entities.Entity]] =
      notImplemented

    override def addEntity(addEntityRequest: EntityClient.AddEntityRequest): IO[UUID] = notImplemented

    override def updateEntity(updateEntityRequest: EntityClient.UpdateEntityRequest): IO[String] = notImplemented

    override def updateEntityIdentifiers(entity: Entities.Entity, identifiers: Seq[Entities.IdentifierResponse]): IO[Seq[Entities.IdentifierResponse]] = notImplemented

    override def streamBitstreamContent[T](stream: capabilities.Streams[Fs2Streams[IO]])(url: String, streamFn: stream.BinaryStream => IO[T]): IO[T] = notImplemented

    override def entitiesUpdatedSince(dateTime: ZonedDateTime, startEntry: Int, maxEntries: Int): IO[Seq[Entities.Entity]] = notImplemented

    override def entityEventActions(entity: Entities.Entity, startEntry: Int, maxEntries: Int): IO[Seq[DataProcessor.EventAction]] = notImplemented

    override def entitiesByIdentifier(identifier: EntityClient.Identifier): IO[Seq[Entities.Entity]] = notImplemented

    override def addIdentifierForEntity(entityRef: UUID, entityType: EntityClient.EntityType, identifier: EntityClient.Identifier): IO[String] = notImplemented

    override def getPreservicaNamespaceVersion(endpoint: String): IO[Float] = errors.raise(_.preservicaError, "Error getting Preservica version") >> IO.pure(version)
  }

  def snsClient(ref: Ref[IO, List[LatestPreservicaVersionMessage]], errors: Option[Errors]): DASNSClient[IO] = new DASNSClient[IO]:
    override def publish[T <: Product](topicArn: String)(messages: List[T])(using enc: Encoder[T]): IO[List[PublishBatchResponse]] =
      errors.raise(_.snsError, "Error sending to SNS") >>
        ref
          .update { existing =>
            messages.map(_.asInstanceOf[LatestPreservicaVersionMessage]) ++ existing
          }
          .map(_ => Nil)

  val config: Config = Config("", "", "", "")

  case class Errors(dynamoError: Boolean = false, preservicaError: Boolean = false, snsError: Boolean = false)

  extension (errors: Option[Errors]) def raise(fn: Errors => Boolean, errorMessage: String): IO[Unit] = IO.raiseWhen(errors.exists(fn))(new Exception(errorMessage))

  def runLambda(
      dynamoResponses: List[GetDr2PreservicaVersionResponse],
      preservicaVersion: Float,
      errors: Option[Errors] = None
  ): (Either[Throwable, Unit], List[LatestPreservicaVersionMessage]) = (for {
    messagesRef <- Ref.of[IO, List[LatestPreservicaVersionMessage]](Nil)
    dependencies = Dependencies(preservicaClient(preservicaVersion, errors), snsClient(messagesRef, errors), dynamoClient(dynamoResponses, errors))
    res <- new Lambda().handler(new ScheduledEvent(), config, dependencies).attempt
    messages <- messagesRef.get
  } yield (res, messages)).unsafeRunSync()
