package uk.gov.nationalarchives.ingestupsertarchivefolders.testUtils

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor3, TableFor5}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, EitherValues}
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.{BatchWriteItemResponse, ResourceNotFoundException}
import sttp.capabilities
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.dp.client.Entities.{Entity, IdentifierResponse}
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.SecurityTag.*
import uk.gov.nationalarchives.dp.client.EntityClient.{AddEntityRequest, EntityType, Identifier, UpdateEntityRequest, Identifier as PreservicaIdentifier}
import uk.gov.nationalarchives.dp.client.{Client, DataProcessor, Entities, EntityClient}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{ArchiveFolderDynamoItem, Identifier as DynamoIdentifier}
import uk.gov.nationalarchives.ingestupsertarchivefolders.Lambda
import uk.gov.nationalarchives.ingestupsertarchivefolders.Lambda.*
import uk.gov.nationalarchives.DADynamoDBClient

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

class ExternalServicesTestUtils extends AnyFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll with TableDrivenPropertyChecks with EitherValues {

  private val config: Config = Config("", "table-name")

  val input: StepFnInput = StepFnInput("TDD-2023-ABC", Nil)

  def notImplemented[T]: IO[T] = IO.raiseError(new Exception("Not implemented"))

  def dynamoClient(ref: Ref[IO, List[ArchiveFolderDynamoItem]], dynamoError: Boolean): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = notImplemented

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      notImplemented

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] =
      IO.raiseWhen(dynamoError)(ResourceNotFoundException.builder.message(s"$tableName not found").build) >> ref.get.map(_.map(_.asInstanceOf[T]))

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = notImplemented

  case class PreservicaErrors(entitiesPerIdentifier: Boolean = false, addEntity: Boolean = false, updateEntity: Boolean = false)

  def preservicaClient(ref: Ref[IO, List[EntityWithIdentifiers]], errors: Option[PreservicaErrors]): EntityClient[IO, Fs2Streams[IO]] =
    new EntityClient[IO, Fs2Streams[IO]] {

      override val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME

      override def metadataForEntity(entity: Entity): IO[EntityClient.EntityMetadata] = notImplemented

      override def getBitstreamInfo(contentRef: UUID): IO[Seq[Client.BitStreamInfo]] = notImplemented

      override def getEntity(entityRef: UUID, entityType: EntityType): IO[Entity] = notImplemented

      override def getEntityIdentifiers(entity: Entity): IO[Seq[IdentifierResponse]] = ref.get.map { existing =>
        identifierResponses(entity, existing)
      }

      override def streamAllEntityRefs(repTypeFilter: Option[EntityClient.RepresentationType]): fs2.Stream[IO, Entities.EntityRef] = fs2.Stream.empty[IO]

      override def getUrlsToIoRepresentations(ioEntityRef: UUID, representationType: Option[EntityClient.RepresentationType]): IO[Seq[String]] = notImplemented

      override def getContentObjectsFromRepresentation(ioEntityRef: UUID, representationType: EntityClient.RepresentationType, repTypeIndex: Int): IO[Seq[Entity]] = notImplemented

      override def addEntity(aer: AddEntityRequest): IO[UUID] =
        IO.raiseWhen(errors.exists(_.addEntity))(new Exception("API has encountered an issue adding entity")) >> ref.modify { existing =>
          val newEntity = Entity(aer.entityType.some, aer.ref.getOrElse(UUID.randomUUID), aer.title.some, aer.description, false, None, aer.securityTag.some, aer.parentRef)
          (EntityWithIdentifiers(newEntity, Nil) :: existing, newEntity.ref)
        }

      override def updateEntity(uer: UpdateEntityRequest): IO[String] =
        IO.raiseWhen(errors.exists(_.updateEntity))(new Exception("API has encountered an issue updating an entity")) >> ref
          .update { existing =>
            existing.map { each =>
              if each.entity.ref == uer.ref then
                val newDescription = uer.descriptionToChange <+> each.entity.description
                val newEntity = Entity(uer.entityType.some, uer.ref, uer.title.some, newDescription, false, None, uer.securityTag.some, uer.parentRef)
                each.copy(entity = newEntity)
              else each
            }
          }
          .map(_ => "")

      override def updateEntityIdentifiers(entity: Entity, identifiers: Seq[IdentifierResponse]): IO[Seq[IdentifierResponse]] = ref.modify { existing =>
        val updatedEntities = existing.map { each =>
          if each.entity.ref == entity.ref then
            val newIdentifiers = each.identifiers.map { eachIdentifier =>
              identifiers
                .find(_.identifierName == eachIdentifier.identifierName)
                .map(id => eachIdentifier.copy(value = id.value))
                .getOrElse(eachIdentifier)
            }
            each.copy(identifiers = newIdentifiers)
          else each
        }
        val response = identifierResponses(entity, updatedEntities)
        (updatedEntities, response)
      }

      override def streamBitstreamContent[T](stream: capabilities.Streams[Fs2Streams[IO]])(url: String, streamFn: stream.BinaryStream => IO[T]): IO[T] = notImplemented

      override def entitiesUpdatedSince(sinceDateTime: ZonedDateTime, startEntry: Int, maxEntries: Int, potentialEndDate: Option[ZonedDateTime]): IO[Seq[Entity]] = notImplemented

      override def entityEventActions(entity: Entity, startEntry: Int, maxEntries: Int): IO[Seq[DataProcessor.EventAction]] = notImplemented

      override def entitiesPerIdentifier(identifiers: Seq[PreservicaIdentifier]): IO[Map[PreservicaIdentifier, Seq[Entity]]] =
        IO.raiseWhen(errors.exists(_.entitiesPerIdentifier))(new Exception("API has encountered and issue")) >>
          ref.get.map { existing =>
            existing
              .flatMap(_.identifiers)
              .map { value =>
                value -> existing.filter(_.identifiers.contains(value)).map(_.entity)
              }
              .toMap
              .view
              .filterKeys(identifiers.contains)
              .toMap
          }

      override def addIdentifierForEntity(entityRef: UUID, entityType: EntityType, identifier: PreservicaIdentifier): IO[String] = ref
        .update { existing =>
          existing.map { eachExisting =>
            if eachExisting.entity.ref == entityRef then eachExisting.copy(identifiers = identifier :: eachExisting.identifiers)
            else eachExisting
          }
        }
        .map(_ => "")

      override def getPreservicaNamespaceVersion(endpoint: String): IO[Float] = notImplemented
    }

  private def identifierResponses(entity: Entity, existing: List[EntityWithIdentifiers]) = {
    existing
      .filter(_.entity.ref == entity.ref)
      .flatMap(_.identifiers)
      .map(id => IdentifierResponse(entity.ref.toString, id.identifierName, id.value))
  }

  def runLambda(
      items: List[ArchiveFolderDynamoItem],
      entities: List[EntityWithIdentifiers],
      dynamoError: Boolean = false,
      preservicaErrors: Option[PreservicaErrors] = None
  ): (List[EntityWithIdentifiers], Throwable) = (for {
    itemsRef <- Ref.of[IO, List[ArchiveFolderDynamoItem]](items)
    entitiesRef <- Ref.of[IO, List[EntityWithIdentifiers]](entities)
    res <- Lambda().handler(input, config, Dependencies(preservicaClient(entitiesRef, preservicaErrors), dynamoClient(itemsRef, dynamoError))).attempt
    entities <- entitiesRef.get
  } yield (entities, if res.isRight then new Exception("Error not found") else res.left.value)).unsafeRunSync()

  def validateParentHierarchy(entities: List[Entity]): Boolean = {
    val ids = entities.map(_.ref).toSet
    entities.forall {
      case Entity(_, _, _, _, _, _, _, None)         => true
      case Entity(_, _, _, _, _, _, _, Some(parent)) => ids.contains(parent)
    }
  }

  def generateItem(suffix: String = "", parentPath: Option[String] = None, identifiers: List[DynamoIdentifier] = Nil): ArchiveFolderDynamoItem = ArchiveFolderDynamoItem(
    "batchId",
    UUID.randomUUID,
    parentPath,
    s"mock name_$suffix",
    ArchiveFolder,
    Some(s"mock title_$suffix"),
    Some(s"mock description_$suffix"),
    identifiers,
    1
  )
  case class EntityWithIdentifiers(entity: Entity, identifiers: List[Identifier])

  def generateEntity(suffix: String = "", identifierValue: String = "", parent: Option[UUID] = None): EntityWithIdentifiers = {
    val entity = Entity(
      Some(StructuralObject),
      UUID.randomUUID,
      Some(s"mock title_$suffix"),
      Some(s"mock description_$suffix"),
      deleted = false,
      Some(StructuralObject.entityPath),
      Some(Unknown),
      parent
    )
    EntityWithIdentifiers(entity, List(Identifier("SourceID", identifierValue)))
  }

  val missingTitleInDbScenarios: TableFor5[String, Option[String], Option[String], Option[String], Option[String]] = Table(
    (
      "Test",
      "Title from DB",
      "Title from Preservica",
      "Description from DB",
      "Description from Preservica"
    ),
    (
      "title not found in DB, title was found in Preservica but no description updates necessary",
      None,
      Some(""),
      Some("mock description_1"),
      Some("mock description_1")
    ),
    (
      "title not found in DB, title was found in Preservica but description needs to be updated",
      None,
      Some(""),
      Some("mock description_1"),
      Some("mock description_old_1")
    ),
    (
      "title and description not found in DB, title and description found in Preservica",
      None,
      Some(""),
      None,
      Some("")
    ),
    (
      "title and description not found in DB, title found in Preservica but not description",
      None,
      Some(""),
      None,
      None
    )
  )

  val missingDescriptionInDbScenarios: TableFor5[String, Option[String], Option[String], Option[String], Option[String]] = Table(
    (
      "Test",
      "Title from DB",
      "Title from Preservica",
      "Description from DB",
      "Description from Preservica"
    ),
    (
      "description not found in DB, description was found in Preservica but no title updates necessary",
      Some("mock title_1"),
      Some("mock title_1"),
      None,
      Some("")
    ),
    (
      "description not found in DB, description was found in Preservica but title needs to be updated",
      Some("mock title_1"),
      Some("mock title_old_1"),
      None,
      Some("")
    ),
    (
      "description not found in DB, description not found in Preservica but no title updates necessary",
      Some("mock title_1"),
      Some("mock title_1"),
      None,
      None
    ),
    (
      "description not found in DB, description not found in Preservica but title needs to be updated",
      Some("mock title_1"),
      Some("mock title_old_1"),
      None,
      None
    )
  )

  def dynamoIdentifier(name: String, value: String): List[DynamoIdentifier] = List(DynamoIdentifier(name, value))
  def preservicaIdentifier(name: String, value: String): List[Identifier] = List(Identifier(name, value))

  val identifierScenarios: TableFor3[List[DynamoIdentifier], List[Identifier], List[Identifier]] = Table(
    ("identifierFromDynamo", "identifierFromPreservica", "expectedResult"),
    (dynamoIdentifier("1", "1"), preservicaIdentifier("1", "1"), preservicaIdentifier("1", "1")),
    (dynamoIdentifier("1", "2"), preservicaIdentifier("1", "1"), preservicaIdentifier("1", "2")),
    (dynamoIdentifier("1", "1"), preservicaIdentifier("2", "2"), preservicaIdentifier("2", "2") ++ preservicaIdentifier("1", "1")),
    (dynamoIdentifier("1", "1") ++ dynamoIdentifier("2", "2"), preservicaIdentifier("2", "3"), preservicaIdentifier("2", "2") ++ preservicaIdentifier("1", "1"))
  )
}
