package uk.gov.nationalarchives.custodialcopyqueuecreator

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.{MessageAttribute, SQSMessage}
import io.circe.syntax.*
import io.circe.{Decoder, Encoder}
import software.amazon.awssdk.services.sqs.model.{ChangeMessageVisibilityResponse, DeleteMessageResponse, GetQueueAttributesResponse, QueueAttributeName, SendMessageResponse}
import sttp.capabilities
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DASQSClient
import uk.gov.nationalarchives.DASQSClient.FifoQueueConfiguration
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient.{EntitiesUpdated, Identifier, StandardEntityMetadata}
import uk.gov.nationalarchives.dp.client.{Client, DataProcessor, Entities, EntityClient}
import uk.gov.nationalarchives.custodialcopyqueuecreator.Lambda.*

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*

object Utils:
  val inputQueue = "input-queue"
  val outputQueue = "output-queue"
  val config: Config = Config("", outputQueue)
  val dedupeUuid: UUID = UUID.randomUUID

  def runLambda(sqsMessages: List[SQSMessage], entities: List[Entity]): Map[String, List[SQSMessage]] = {
    val sqsEvent = new SQSEvent()
    sqsEvent.setRecords(sqsMessages.asJava)
    for {
      sqsMessagesRef <- Ref.of[IO, Map[String, List[SQSMessage]]](Map(inputQueue -> sqsMessages, outputQueue -> Nil))
      entitiesRef <- Ref.of[IO, List[Entity]](entities)
      _ <- new Lambda().handler(sqsEvent, config, Dependencies(entityClient(entitiesRef), sqsClient(sqsMessagesRef), () => dedupeUuid))
      messages <- sqsMessagesRef.get
    } yield messages
  }.unsafeRunSync()

  def sqsClient(sqsMessagesRef: Ref[IO, Map[String, List[SQSMessage]]]): DASQSClient[IO] = new DASQSClient[IO]:
    override def sendMessage[T <: Product](
        queueUrl: String
    )(message: T, potentialFifoQueueConfiguration: Option[FifoQueueConfiguration], delaySeconds: Int)(using enc: Encoder[T]): IO[SendMessageResponse] = sqsMessagesRef
      .update { messagesMap =>
        val newMessage = new SQSMessage()
        newMessage.setBody(message.asJson.noSpaces)
        potentialFifoQueueConfiguration.foreach { queueConfig =>
          newMessage.setMessageId(queueConfig.messageGroupId)

          val messageAttribute = new MessageAttribute()
          messageAttribute.setStringValue(queueConfig.messageDeduplicationId)
          newMessage.setMessageAttributes(Map("deduplicationId" -> messageAttribute).asJava)
        }
        messagesMap.map {
          case (queue, messages) if queue == queueUrl => queue -> (newMessage :: messages)
          case (queue, messages)                      => queue -> messages
        }
      }
      .map(_ => SendMessageResponse.builder.build)

    override def deleteMessage(queueUrl: String, receiptHandle: String): IO[DeleteMessageResponse] = IO.pure(DeleteMessageResponse.builder.build)

    override def receiveMessages[T](queueUrl: String, maxNumberOfMessages: Int)(using dec: Decoder[T]): IO[List[DASQSClient.MessageResponse[T]]] = IO.pure(Nil)

    override def getQueueAttributes(queueUrl: String, attributeNames: List[QueueAttributeName]): IO[GetQueueAttributesResponse] = IO.raiseError(new Exception("Not implemented"))

    override def changeVisibilityTimeout(queueUrl: String)(receiptHandle: String, timeout: Duration): IO[ChangeMessageVisibilityResponse] = IO.stub

  def entityClient(entitiesRef: Ref[IO, List[Entity]]): EntityClient[IO, Fs2Streams[IO]] = new EntityClient[IO, Fs2Streams[IO]]:
    val entityMetadata: StandardEntityMetadata = StandardEntityMetadata(<A></A>, Seq(<A></A>), Seq(<A></A>), Seq(<A></A>), Seq(<A></A>))
    override val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_DATE

    override def metadataForEntity(entity: Entities.Entity): IO[EntityClient.EntityMetadata] = IO.pure(entityMetadata)

    override def getBitstreamInfo(contentRef: UUID): IO[Seq[Client.BitStreamInfo]] = IO.pure(Nil)

    override def getEntity(entityRef: UUID, entityType: EntityClient.EntityType): IO[Entities.Entity] = entitiesRef.get.flatMap { entities =>
      IO.fromOption(entities.find(entity => entity.entityType.contains(entityType) && entity.ref == entityRef))(new Exception(s"Entity $entityRef not found"))
    }

    override def getEntityIdentifiers(entity: Entities.Entity): IO[Seq[Entities.IdentifierResponse]] = IO.pure(Nil)

    override def getUrlsToIoRepresentations(ioEntityRef: UUID, representationType: Option[EntityClient.RepresentationType]): IO[Seq[String]] = IO.pure(Nil)

    override def getContentObjectsFromRepresentation(ioEntityRef: UUID, representationType: EntityClient.RepresentationType, repTypeIndex: Int): IO[Seq[Entities.Entity]] =
      IO.pure(Nil)

    override def streamAllEntityRefs(repTypeFilter: Option[EntityClient.RepresentationType]): fs2.Stream[IO, Entities.EntityRef] = fs2.Stream.empty[IO]

    override def addEntity(addEntityRequest: EntityClient.AddEntityRequest): IO[UUID] = IO.pure(UUID.randomUUID)

    override def updateEntity(updateEntityRequest: EntityClient.UpdateEntityRequest): IO[String] = IO.pure("")

    override def updateEntityIdentifiers(entity: Entities.Entity, identifiers: Seq[Entities.IdentifierResponse]): IO[Seq[Entities.IdentifierResponse]] = IO.pure(Nil)

    override def streamBitstreamContent[T](stream: capabilities.Streams[Fs2Streams[IO]])(url: String, streamFn: stream.BinaryStream => IO[T]): IO[T] = IO.pure("".asInstanceOf[T])

    override def entitiesUpdatedSince(sinceDateTime: ZonedDateTime, startEntry: Int, maxEntries: Int, potentialEndDate: Option[ZonedDateTime]): IO[EntitiesUpdated] = IO.stub

    override def entityEventActions(entity: Entities.Entity, startEntry: Int, maxEntries: Int): IO[Seq[DataProcessor.EventAction]] = IO.pure(Nil)

    override def entitiesPerIdentifier(identifiers: Seq[EntityClient.Identifier]): IO[Map[Identifier, Seq[Entities.Entity]]] = IO.pure(Map.empty)

    override def addIdentifierForEntity(entityRef: UUID, entityType: EntityClient.EntityType, identifier: EntityClient.Identifier): IO[String] = IO.pure("")

    override def getPreservicaNamespaceVersion(endpoint: String): IO[Float] = IO(1.0f)
