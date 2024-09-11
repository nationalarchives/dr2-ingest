package uk.gov.nationalarchives.ingestupsertarchivefolders

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import sttp.capabilities
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.dp.client.Entities.Entity
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.InformationObject
import uk.gov.nationalarchives.dp.client.EntityClient.StandardEntityMetadata
import uk.gov.nationalarchives.dp.client.{Client, DataProcessor, Entities, EntityClient}
import uk.gov.nationalarchives.{DADynamoDBClient, DAEventBridgeClient}
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.ingestupsertarchivefolders.Lambda.*

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.xml.Node
object Runner extends App {

  val archiveFolders = List("01e8565a-824b-483b-91e4-5950abb439d2","f59b3b75-3252-45b5-8609-eca444364bee","8ca36b1a-7f07-401e-879f-98cd5bc8208f","b823a3d9-d634-4d3f-a1d2-818303850e4e","415ce44c-6923-4c4c-945d-69fac024b746","77900312-fcb7-461b-a6e7-56d79d346d9d","a7970d8b-2cbe-49b4-ab6b-bfa43f9b3c6e","79f4a5be-8a48-4721-a8f6-ba53de00de49","d07db5d6-a510-46a5-a2c2-07b88ab83aa4")

  val entityClient: EntityClient[IO, Fs2Streams[IO]] = new EntityClient[IO, Fs2Streams[IO]] {

    override val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME

    override def metadataForEntity(entity: Entities.Entity): IO[EntityClient.EntityMetadata] = IO(StandardEntityMetadata(<A></A>, Nil, Nil, Nil, Nil))

    override def getBitstreamInfo(contentRef: UUID): IO[Seq[Client.BitStreamInfo]] = IO(Nil)

    override def getEntity(entityRef: UUID, entityType: EntityClient.EntityType): IO[Entities.Entity] = IO(Entity(Option(InformationObject), UUID.randomUUID, None, None, false, None))

    override def getEntityIdentifiers(entity: Entities.Entity): IO[Seq[Entities.IdentifierResponse]] = IO(Nil)

    override def getUrlsToIoRepresentations(ioEntityRef: UUID, representationType: Option[EntityClient.RepresentationType]): IO[Seq[String]] = IO(Nil)

    override def getContentObjectsFromRepresentation(ioEntityRef: UUID, representationType: EntityClient.RepresentationType, repTypeIndex: Int): IO[Seq[Entities.Entity]] = IO(Nil)

    override def addEntity(addEntityRequest: EntityClient.AddEntityRequest): IO[UUID] = IO(UUID.randomUUID)

    override def updateEntity(updateEntityRequest: EntityClient.UpdateEntityRequest): IO[String] = IO("")

    override def updateEntityIdentifiers(entity: Entities.Entity, identifiers: Seq[Entities.IdentifierResponse]): IO[Seq[Entities.IdentifierResponse]] = IO(Nil)

    override def streamBitstreamContent[T](stream: capabilities.Streams[Fs2Streams[IO]])(url: String, streamFn: stream.BinaryStream => IO[T]): IO[T] = IO(().asInstanceOf[T])

    override def entitiesUpdatedSince(dateTime: ZonedDateTime, startEntry: Int, maxEntries: Int): IO[Seq[Entities.Entity]] = IO(Nil)

    override def entityEventActions(entity: Entities.Entity, startEntry: Int, maxEntries: Int): IO[Seq[DataProcessor.EventAction]] = IO(Nil)

    override def entitiesByIdentifier(identifier: EntityClient.Identifier): IO[Seq[Entities.Entity]] = IO(Nil)

    override def addIdentifierForEntity(entityRef: UUID, entityType: EntityClient.EntityType, identifier: EntityClient.Identifier): IO[String] = IO("")

    override def getPreservicaNamespaceVersion(endpoint: String): IO[Float] = IO(1f)
  }
  val input = StepFnInput("TDR_7114d947-360e-42db-b2f8-d6628c276e67_3", archiveFolders, Nil, Nil)
  private val apiUrl = "https://tna.preservica.com"
  private val secretName = "asdasd"
  val dependencies = Dependencies(entityClient, DADynamoDBClient[IO](), DAEventBridgeClient[IO]())
  val config = Config(apiUrl, secretName, "intg-dr2-ingest-files")
  val res = new Lambda().handler(input, config, dependencies).unsafeRunSync()
  println(res)
}
