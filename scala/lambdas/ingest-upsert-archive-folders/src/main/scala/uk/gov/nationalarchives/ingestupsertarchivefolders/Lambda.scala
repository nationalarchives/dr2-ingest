package uk.gov.nationalarchives.ingestupsertarchivefolders

import cats.effect.IO
import cats.implicits.*
import io.circe.generic.auto.*
import pureconfig.ConfigReader
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{Identifier => DynamoIdentifier, *}
import uk.gov.nationalarchives.ingestupsertarchivefolders.Lambda.*
import uk.gov.nationalarchives.dp.client.Entities.{Entity, IdentifierResponse}
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.SecurityTag.*
import uk.gov.nationalarchives.dp.client.EntityClient.*
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.DADynamoDBClient

import java.util.UUID

class Lambda extends LambdaRunner[StepFnInput, Unit, Config, Dependencies] {
  private val sourceId = "SourceID"

  given Ordering[ArchiveFolderDynamoItem] = (x: ArchiveFolderDynamoItem, y: ArchiveFolderDynamoItem) => x.potentialParentPath.compare(y.potentialParentPath)

  extension [T](potentialValue: Option[T]) private def toStringOrEmpty: String = potentialValue.map(_.toString).getOrElse("")

  extension (archiveFolderDynamoItem: ArchiveFolderDynamoItem)
    private def directParentId: Option[UUID] = archiveFolderDynamoItem.potentialParentPath.flatMap(_.split("/").lastOption).map(UUID.fromString)

  override def handler: (
      StepFnInput,
      Config,
      Dependencies
  ) => IO[Unit] = (stepFnInput, config, dependencies) => {

    val folderIdPartitionKeysAndValues: List[FilesTablePrimaryKey] =
      stepFnInput.archiveHierarchyFolders.map(UUID.fromString).map(id => FilesTablePrimaryKey(FilesTablePartitionKey(id), FilesTableSortKey(stepFnInput.batchId)))

    def getFolderRows(using Ordering[ArchiveFolderDynamoItem]): IO[List[ArchiveFolderDynamoItem]] =
      dependencies.dADynamoDBClient
        .getItems[ArchiveFolderDynamoItem, FilesTablePrimaryKey](
          folderIdPartitionKeysAndValues,
          config.archiveFolderTableName
        )
        .map(_.sorted)

    def createNonExistentFolders(folderRows: List[ArchiveFolderDynamoItem], entitiesByIdentifier: Map[String, Entity]): IO[Unit] =
      folderRows
        .foldLeft(IO(Map.empty[UUID, UUID])) { (idToEntityRefMapIO, folderRow) =>
          entitiesByIdentifier
            .get(folderRow.name)
            .map { entity =>
              val expectedParent = getExpectedParentRef(folderRow, folderRows, entitiesByIdentifier)
              for {
                _ <- IO.raiseWhen(!entity.entityType.contains(StructuralObject))(
                  new Exception(s"The entity type for folder id ${folderRow.id} should be 'StructuralObject' but it is ${entity.entityType.toStringOrEmpty}")
                )
                _ <- IO.raiseWhen(entity.parent != expectedParent)(
                  new Exception(s"API returned a parent ref of '${entity.parent.toStringOrEmpty}' for entity ${entity.ref} instead of expected '${expectedParent.toStringOrEmpty}'")
                )
                _ <- IO.raiseWhen(entity.securityTag.isEmpty || entity.securityTag.flatMap(Option.apply).isEmpty)(
                  new Exception(s"Security tag '${entity.securityTag}' is unexpected for SO ref '${entity.ref}'")
                )
                updatedEntityMap <- idToEntityRefMapIO.map(_ + (folderRow.id -> entity.ref))
              } yield updatedEntityMap
            }
            .getOrElse {
              for {
                entityMap <- idToEntityRefMapIO
                entityId <- addFolder(folderRow, entityMap)
              } yield entityMap + (folderRow.id -> entityId)
            }
        }
        .void

    def addFolder(folderRow: ArchiveFolderDynamoItem, idToEntityRefMap: Map[UUID, UUID]) = {
      val potentialParentRef = for {
        directParent <- folderRow.directParentId
        entityRef <- idToEntityRefMap.get(directParent)
      } yield entityRef
      val addFolderRequest = AddEntityRequest(
        None,
        folderRow.potentialTitle.getOrElse(folderRow.name),
        folderRow.potentialDescription,
        StructuralObject,
        Unknown,
        potentialParentRef
      )
      val folderName = folderRow.name
      val identifiersToAdd = List(Identifier(sourceId, folderName)) ++
        folderRow.identifiers.map(id => Identifier(id.identifierName, id.value))
      for {
        entityId <- dependencies.entityClient.addEntity(addFolderRequest)
        _ <- identifiersToAdd.traverse { identifierToAdd =>
          dependencies.entityClient.addIdentifierForEntity(
            entityId,
            StructuralObject,
            identifierToAdd
          )
        }
      } yield entityId
    }

    def getEntitiesByIdentifier(
        folderRows: List[FolderDynamoItem]
    ): IO[Map[String, Entity]] = {
      val identifiers = folderRows
        .map(_.name)
        .map(name => Identifier(sourceId, name))
        .distinct
      for {
        entityMap <- dependencies.entityClient.entitiesPerIdentifier(identifiers)
        multipleEntries = entityMap.filter { case (value, entities) => entities.length > 1 }
        _ <- IO.raiseWhen(multipleEntries.nonEmpty)(new Exception(s"There is more than 1 entity with these SourceIDs: ${multipleEntries.keys.map(_.value).mkString(" ")}"))

      } yield entityMap.collect { case (identifier, entity :: Nil) =>
        identifier.value -> entity
      }
    }

    val logWithBatchRef = log(Map("batchRef" -> stepFnInput.batchId))(_)

    for {
      folderRows <- getFolderRows
      entitiesByIdentifier <- getEntitiesByIdentifier(folderRows)
      _ <- createNonExistentFolders(folderRows, entitiesByIdentifier)
      _ <- folderRows.filter(row => entitiesByIdentifier.contains(row.name)).traverse { folderRow =>
        val entity = entitiesByIdentifier(folderRow.name)
        for {
          _ <- createUpdateRequest(folderRow, folderRows, entity, entitiesByIdentifier).toList.traverse { updateEntityRequest =>
            logWithBatchRef(s"Updating entity ${updateEntityRequest.ref}") >> dependencies.entityClient.updateEntity(updateEntityRequest)
          }
          identifiers <- dependencies.entityClient.getEntityIdentifiers(entity)
          identifiersToAdd <- findIdentifiersToAdd(folderRow.identifiers, identifiers)
          identifiersToUpdate <- findIdentifiersToUpdate(folderRow.identifiers, identifiers)
          _ <- identifiersToAdd.traverse(ita => dependencies.entityClient.addIdentifierForEntity(entity.ref, StructuralObject, ita))
          _ <- identifiersToUpdate.traverse(itu => dependencies.entityClient.updateEntityIdentifiers(entity, identifiersToUpdate.map(_.newIdentifier)))
        } yield ()
      }
      _ <- logWithBatchRef(s"Created ${folderRows.count(row => !entitiesByIdentifier.contains(row.name))} entities which did not previously exist")
    } yield ()
  }

  private def getExpectedParentRef(folderRow: ArchiveFolderDynamoItem, folderRows: List[ArchiveFolderDynamoItem], sourceMap: Map[String, Entity]) = {
    folderRows
      .find(row => folderRow.directParentId.contains(row.id))
      .map(_.name)
      .flatMap(sourceMap.get)
      .map(_.ref)
  }

  private def createUpdateRequest(
      folderRow: ArchiveFolderDynamoItem,
      folderRows: List[ArchiveFolderDynamoItem],
      entity: Entity,
      sourceMap: Map[String, Entity]
  ): Option[UpdateEntityRequest] = {
    val potentialNewTitle = folderRow.potentialTitle.orElse(entity.title)
    val potentialNewDescription = folderRow.potentialDescription

    val titleHasChanged = potentialNewTitle != entity.title && potentialNewTitle.nonEmpty
    val descriptionHasChanged = potentialNewDescription != entity.description && potentialNewDescription.nonEmpty
    val parentRef = getExpectedParentRef(folderRow, folderRows, sourceMap)
    val updateEntityRequest =
      if (titleHasChanged || descriptionHasChanged) {
        val updatedTitleOrDescriptionRequest =
          UpdateEntityRequest(
            entity.ref,
            potentialNewTitle.get,
            if (descriptionHasChanged) potentialNewDescription else entity.description,
            StructuralObject,
            entity.securityTag.get,
            parentRef
          )
        Some(updatedTitleOrDescriptionRequest)
      } else None

    updateEntityRequest
  }

  private def findIdentifiersToUpdate(
      identifiersFromDynamo: List[DynamoIdentifier],
      identifiersFromPreservica: Seq[IdentifierResponse]
  ): IO[Seq[IdentifierToUpdate]] = IO {
    identifiersFromDynamo.flatMap { id =>
      identifiersFromPreservica
        .find(pid => pid.identifierName == id.identifierName && pid.value != id.value)
        .map(pid => IdentifierToUpdate(pid, IdentifierResponse(pid.id, id.identifierName, id.value)))
    }
  }

  private def findIdentifiersToAdd(
      identifiersFromDynamo: List[DynamoIdentifier],
      identifiersFromPreservica: Seq[IdentifierResponse]
  ): IO[Seq[Identifier]] = IO {
    identifiersFromDynamo
      .filterNot { id =>
        identifiersFromPreservica.exists(pid => pid.identifierName == id.identifierName)
      }
      .map(id => Identifier(id.identifierName, id.value))
  }

  override def dependencies(config: Config): IO[Dependencies] = {
    Fs2Client.entityClient(config.secretName).map { client =>
      Dependencies(client, DADynamoDBClient[IO]())
    }
  }
}

object Lambda {
  case class Config(secretName: String, archiveFolderTableName: String) derives ConfigReader

  case class StepFnInput(
      batchId: String,
      archiveHierarchyFolders: List[String]
  )

  private[nationalarchives] case class EntityWithUpdateEntityRequest(
      entity: Entity,
      updateEntityRequest: UpdateEntityRequest
  )

  private[nationalarchives] case class IdentifierToUpdate(
      oldIdentifier: IdentifierResponse,
      newIdentifier: IdentifierResponse
  )

  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], dADynamoDBClient: DADynamoDBClient[IO])

}
