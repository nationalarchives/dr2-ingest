package uk.gov.nationalarchives.ingestupsertarchivefolders

import cats.effect.IO
import cats.implicits.*
import io.circe.generic.auto.*
import pureconfig.generic.derivation.default.*
import pureconfig.ConfigReader
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{Identifier => DynamoIdentifier, *}
import uk.gov.nationalarchives.utils.ExternalUtils.DetailType.DR2Message
import uk.gov.nationalarchives.ingestupsertarchivefolders.Lambda.*
import uk.gov.nationalarchives.dp.client.Entities.{Entity, IdentifierResponse}
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.SecurityTag.*
import uk.gov.nationalarchives.dp.client.EntityClient.*
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.{DADynamoDBClient, DAEventBridgeClient}

import java.util.UUID

class Lambda extends LambdaRunner[StepFnInput, Unit, Config, Dependencies] {
  private val sourceId = "SourceID"

  given Ordering[ArchiveFolderDynamoTable] = (x: ArchiveFolderDynamoTable, y: ArchiveFolderDynamoTable) => x.potentialParentPath.compare(y.potentialParentPath)

  extension [T](potentialValue: Option[T]) private def toStringOrEmpty: String = potentialValue.map(_.toString).getOrElse("")

  extension (archiveFolderDynamoTable: ArchiveFolderDynamoTable)
    private def directParentId: Option[UUID] = archiveFolderDynamoTable.potentialParentPath.flatMap(_.split("/").lastOption).map(UUID.fromString)

  override def handler: (
      StepFnInput,
      Config,
      Dependencies
  ) => IO[Unit] = (stepFnInput, config, dependencies) => {

    val folderIdPartitionKeysAndValues: List[FilesTablePrimaryKey] =
      stepFnInput.archiveHierarchyFolders.map(UUID.fromString).map(id => FilesTablePrimaryKey(FilesTablePartitionKey(id), FilesTableSortKey(stepFnInput.batchId)))

    def getFolderRows: IO[List[ArchiveFolderDynamoTable]] =
      dependencies.dADynamoDBClient
        .getItems[ArchiveFolderDynamoTable, FilesTablePrimaryKey](
          folderIdPartitionKeysAndValues,
          config.archiveFolderTableName
        )
        .map(_.sorted)

    def getIdToEntityMap(folderRows: List[ArchiveFolderDynamoTable], sourceMap: Map[String, Entity]) =
      folderRows.foldLeft(IO(Map.empty[UUID, UUID])) { (entityMapIO, folderRow) =>
        sourceMap
          .get(folderRow.name)
          .map { entity =>
            val expectedParent = getExpectedParent(folderRow, folderRows, sourceMap)
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
              mapping <- entityMapIO.map(_ + (folderRow.id -> entity.ref))
            } yield mapping
          }
          .getOrElse {
            for {
              entityMap <- entityMapIO
              entityId <- addFolder(folderRow, entityMap)
            } yield entityMap + (folderRow.id -> entityId)

          }
      }

    def addFolder(folderRow: ArchiveFolderDynamoTable, entityMap: Map[UUID, UUID]) = {
      val potentialParentRef = for {
        directParent <- folderRow.directParentId
        entityRef <- entityMap.get(directParent)
      } yield entityRef
      val addFolderRequest = AddEntityRequest(
        None,
        folderRow.potentialTitle.getOrElse(folderRow.name),
        folderRow.potentialDescription,
        StructuralObject,
        Open,
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
        folderRows: List[DynamoTable]
    ): IO[Map[String, Entity]] = {
      folderRows
        .map(_.name)
        .toSet
        .map(name => Identifier(sourceId, name))
        .toList
        .traverse(identifier =>
          dependencies.entityClient.entitiesByIdentifier(identifier).flatMap {
            case head :: next =>
              if next.nonEmpty then IO.raiseError(new Exception(s"There is more than 1 entity with the same SourceID ${identifier.value}"))
              else IO(identifier.value -> Option(head))
            case Nil => IO(identifier.value -> None)
          }
        )
        .map {
          _.collect { case (value, Some(entity)) =>
            (value, entity)
          }.toMap
        }
    }

    val logWithBatchRef = log(Map("batchRef" -> stepFnInput.batchId))(_)

    for {
      folderRows <- getFolderRows
      entitiesByIdentifier <- getEntitiesByIdentifier(folderRows)
      idToEntityRef <- getIdToEntityMap(folderRows, entitiesByIdentifier)
      _ <- folderRows.filter(row => entitiesByIdentifier.contains(row.name)).traverse { folderRow =>
        val entity = entitiesByIdentifier(folderRow.name)
        for {
          _ <- createUpdateRequest(folderRow, folderRows, entity, entitiesByIdentifier).toList.traverse { updateEntityRequest =>
            val message = generateTitleDescriptionSlackMessage(config.apiUrl, updateEntityRequest, entity)
            for {
              _ <- logWithBatchRef(s"Updating entity ${updateEntityRequest.ref}")
              _ <- dependencies.entityClient.updateEntity(updateEntityRequest)
              _ <- logWithBatchRef(s"Sending\n$message\nto slack")
              _ <- dependencies.eventBridgeClient.publishEventToEventBridge(getClass.getName, DR2Message, Detail(message))
            } yield ()
          }
          identifiers <- dependencies.entityClient.getEntityIdentifiers(entity)
          identifiersToAdd <- findIdentifiersToAdd(folderRow.identifiers, identifiers)
          identifiersToUpdate <- findIdentifiersToUpdate(folderRow.identifiers, identifiers)
          _ <- identifiersToAdd.traverse(ita => dependencies.entityClient.addIdentifierForEntity(entity.ref, StructuralObject, ita))
          _ <- identifiersToUpdate.traverse(itu => dependencies.entityClient.updateEntityIdentifiers(entity, identifiersToUpdate.map(_.newIdentifier)))
          updatedSlackMessage <- generateIdentifierSlackMessage(
            config.apiUrl,
            entity,
            identifiersToUpdate,
            identifiersToAdd
          )
          _ <- updatedSlackMessage.map(msg => dependencies.eventBridgeClient.publishEventToEventBridge(getClass.getName, DR2Message, Detail(msg))).sequence
        } yield ()

      }
      _ <- logWithBatchRef(s"Created ${folderRows.count(row => !entitiesByIdentifier.contains(row.name))} entities which did not previously exist")
    } yield ()
  }

  private def getExpectedParent(folderRow: ArchiveFolderDynamoTable, folderRows: List[ArchiveFolderDynamoTable], sourceMap: Map[String, Entity]) = {
    folderRows
      .find(row => folderRow.directParentId.contains(row.id))
      .map(_.name)
      .flatMap(sourceMap.get)
      .map(_.ref)
  }

  private def createUpdateRequest(
      folderRow: ArchiveFolderDynamoTable,
      folderRows: List[ArchiveFolderDynamoTable],
      entity: Entity,
      sourceMap: Map[String, Entity]
  ): Option[UpdateEntityRequest] = {
    val potentialNewTitle = folderRow.potentialTitle.orElse(entity.title)
    val potentialNewDescription = folderRow.potentialDescription

    val titleHasChanged = potentialNewTitle != entity.title && potentialNewTitle.nonEmpty
    val descriptionHasChanged = potentialNewDescription != entity.description && potentialNewDescription.nonEmpty
    val parentRef = getExpectedParent(folderRow, folderRows, sourceMap)
    val updateEntityRequest =
      if (titleHasChanged || descriptionHasChanged) {
        val updatedTitleOrDescriptionRequest =
          UpdateEntityRequest(
            entity.ref,
            potentialNewTitle.get,
            if (descriptionHasChanged) potentialNewDescription else None,
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

  private def generateIdentifierSlackMessage(
      preservicaUrl: String,
      entity: Entity,
      updatedIdentifier: Seq[IdentifierToUpdate],
      addedIdentifiers: Seq[Identifier]
  ): IO[Option[String]] = IO {
    if (updatedIdentifier.isEmpty && addedIdentifiers.isEmpty) {
      None
    } else
      Option {
        val firstLine = generateSlackMessageFirstLine(preservicaUrl, entity)
        val potentialUpdatedHeader = updatedIdentifier.headOption
          .map(_ => "The following identifiers have been updated\n")
          .getOrElse("")
        val updatedIdentifierMessage = updatedIdentifier
          .map { ui =>
            s"""
               |*Old value* ${ui.oldIdentifier.identifierName}: ${ui.oldIdentifier.value}
               |*New value* ${ui.newIdentifier.identifierName}: ${ui.newIdentifier.value}
               |
               |""".stripMargin
          }
          .mkString("")
        val potentialAddIdentifiersHeader = addedIdentifiers.headOption.map(_ => "The following identifiers have been added\n").getOrElse("")
        val addedIdentifiersMessage = addedIdentifiers
          .map { ai =>
            s"""
               |${ai.identifierName}: ${ai.value}
               |""".stripMargin
          }
          .mkString("")
        firstLine + potentialUpdatedHeader + updatedIdentifierMessage + potentialAddIdentifiersHeader + addedIdentifiersMessage
      }

  }

  private def generateTitleDescriptionSlackMessage(
      preservicaUrl: String,
      updateEntityRequest: UpdateEntityRequest,
      entity: Entity
  ): String = {
    val firstLineOfMsg: String = generateSlackMessageFirstLine(preservicaUrl, entity, messageWithNewLine = false)

    val titleUpdates = Option.when(entity.title.getOrElse("") != updateEntityRequest.title)(
      "Title has changed"
    )
    val descriptionUpdates = Option.when(updateEntityRequest.descriptionToChange.isDefined)(
      "Description has changed"
    )

    firstLineOfMsg ++ s"*${List(titleUpdates, descriptionUpdates).flatten.mkString(" and ")}*"
  }

  private def generateSlackMessageFirstLine(preservicaUrl: String, entity: Entity, messageWithNewLine: Boolean = true) = {
    val entityTypeShort = entity.entityType
      .map(_.entityTypeShort)
      .getOrElse("IO") // We need a default and Preservica don't validate the entity type in the url
    val entityUrl = s"$preservicaUrl/explorer/explorer.html#properties:$entityTypeShort&${entity.ref}"
    val firstLineOfMsg = s":preservica: Entity <$entityUrl|${entity.ref}> has been updated: "
    val firstLineOfMsgWithNewLine =
      s"""$firstLineOfMsg
         |""".stripMargin
    if (messageWithNewLine) firstLineOfMsgWithNewLine else firstLineOfMsg
  }

  override def dependencies(config: Config): IO[Dependencies] = {
    Fs2Client.entityClient(config.apiUrl, config.secretName).map { client =>
      Dependencies(client, DADynamoDBClient[IO](), DAEventBridgeClient[IO]())
    }
  }
}

object Lambda extends App {
  case class Config(apiUrl: String, secretName: String, archiveFolderTableName: String) derives ConfigReader

  case class StepFnInput(
      batchId: String,
      archiveHierarchyFolders: List[String],
      contentFolders: List[String],
      contentAssets: List[String]
  )

  private[nationalarchives] case class EntityWithUpdateEntityRequest(
      entity: Entity,
      updateEntityRequest: UpdateEntityRequest
  )

  private[nationalarchives] case class IdentifierToUpdate(
      oldIdentifier: IdentifierResponse,
      newIdentifier: IdentifierResponse
  )

  case class Detail(slackMessage: String)

  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], dADynamoDBClient: DADynamoDBClient[IO], eventBridgeClient: DAEventBridgeClient[IO])

}
