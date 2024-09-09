package uk.gov.nationalarchives.ingestupsertarchivefolders

import cats.effect.IO
import cats.implicits.*
import io.circe.generic.auto.*
import pureconfig.generic.derivation.default.*
import pureconfig.ConfigReader
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{ArchiveFolderDynamoTable, DynamoTable, FilesTablePartitionKey, FilesTablePrimaryKey, FilesTableSortKey}
import uk.gov.nationalarchives.ingestupsertarchivefolders.Lambda.*
import uk.gov.nationalarchives.dp.client.Entities.{Entity, IdentifierResponse}
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.SecurityTag.*
import uk.gov.nationalarchives.dp.client.EntityClient.*
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.utils.ExternalUtils.DetailType.DR2Message
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.{DADynamoDBClient, DAEventBridgeClient}

import java.util.UUID

class Lambda extends LambdaRunner[StepFnInput, Unit, Config, Dependencies] {
  private val structuralObject = StructuralObject
  private val sourceId = "SourceID"

  override def handler: (
      StepFnInput,
      Config,
      Dependencies
  ) => IO[Unit] = (stepFnInput, config, dependencies) => {

    def createFolders(
        folderInfoOfEntities: List[FullFolderInfo],
        previouslyCreatedEntityIdsWithFolderRowIdsAsKeys: Map[UUID, UUID] = Map()
    ): IO[Unit] = {
      if (folderInfoOfEntities.isEmpty) IO.unit
      else {
        val folderInfo = folderInfoOfEntities.head
        val potentialParentRef =
          if (folderInfo.expectedParentRef.isEmpty) {
            // 'expectedParentRef' is empty either because parent was not in Preservica at start of Lambda, or folder is top-level
            val parentId = folderInfo.folderRow.parentPath
              .flatMap(_.split('/').lastOption)
              .map(UUID.fromString)
            parentId.flatMap(previouslyCreatedEntityIdsWithFolderRowIdsAsKeys.get)
          } else folderInfo.expectedParentRef

        val addFolderRequest = AddEntityRequest(
          None,
          folderInfo.folderRow.title.getOrElse(folderInfo.folderRow.name),
          folderInfo.folderRow.description,
          structuralObject,
          Open,
          potentialParentRef
        )

        val folderName = folderInfo.folderRow.name
        val identifiersToAdd = List(Identifier(sourceId, folderName)) ++
          folderInfo.folderRow.identifiers.map(id => Identifier(id.identifierName, id.value))

        for {
          entityId <- dependencies.entityClient.addEntity(addFolderRequest)
          _ <- identifiersToAdd.traverse { identifierToAdd =>
            dependencies.entityClient.addIdentifierForEntity(
              entityId,
              structuralObject,
              identifierToAdd
            )
          }
          _ <- createFolders(
            folderInfoOfEntities.tail,
            previouslyCreatedEntityIdsWithFolderRowIdsAsKeys + (folderInfo.folderRow.id -> entityId)
          )
        } yield ()
      }
    }

    def getEntitiesByIdentifier(
        folderRows: List[DynamoTable]
    ): IO[Map[String, Option[Entity]]] = {
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
        .map(_.toMap)
    }

    val logWithBatchRef = log(Map("batchRef" -> stepFnInput.batchId))(_)

    val folderIdPartitionKeysAndValues: List[FilesTablePrimaryKey] =
      stepFnInput.archiveHierarchyFolders.map(UUID.fromString).map(id => FilesTablePrimaryKey(FilesTablePartitionKey(id), FilesTableSortKey(stepFnInput.batchId)))

    for {
      folderRows <- dependencies.dADynamoDBClient.getItems[ArchiveFolderDynamoTable, FilesTablePrimaryKey](
        folderIdPartitionKeysAndValues,
        config.archiveFolderTableName
      )

      _ <- checkParentPathsValid(folderRows)

      _ <- logWithBatchRef(s"Searching for Source Ids ${folderRows.map(_.name).mkString(",")}")
      sourceIdToEntity <- getEntitiesByIdentifier(folderRows)

      (folderInfoOfEntitiesThatDoNotExist, folderInfoOfEntitiesThatExist) = createFullFolderInfo(sourceIdToEntity, folderRows)
        .partition(_.entity.isEmpty)

      _ <- createFolders(folderInfoOfEntitiesThatDoNotExist)
      _ <- logWithBatchRef(s"Created ${folderInfoOfEntitiesThatDoNotExist.length} entities which did not previously exist")
      _ <- verifyEntitiesAreStructuralObjects(folderInfoOfEntitiesThatExist)

      folderInfoOfEntitiesThatExistWithSecurityTags <-
        verifyExpectedParentFolderMatchesFolderFromApiAndGetSecurityTag(folderInfoOfEntitiesThatExist)
      folderUpdateRequests = findOnlyFoldersThatNeedUpdatingAndCreateRequests(
        folderInfoOfEntitiesThatExistWithSecurityTags
      )

      _ <- folderInfoOfEntitiesThatExistWithSecurityTags.map { fi =>
        val identifiersFromDynamo = fi.folderRow.identifiers.map(id => Identifier(id.identifierName, id.value))

        val entity = fi.entity.get
        for {
          identifiersFromPreservica <- dependencies.entityClient.getEntityIdentifiers(entity)
          identifiersToUpdate <- findIdentifiersToUpdate(identifiersFromDynamo, identifiersFromPreservica)
          _ <- logWithBatchRef(s"Found ${identifiersToUpdate.length} identifiers to update")
          identifiersToAdd <- findIdentifiersToAdd(identifiersFromDynamo, identifiersFromPreservica)
          _ <- logWithBatchRef(s"Found ${identifiersToAdd.length} identifiers to add")
          _ <- identifiersToAdd.map { id =>
            dependencies.entityClient.addIdentifierForEntity(entity.ref, entity.entityType.getOrElse(StructuralObject), id)
          }.sequence
          updatedIdentifier = identifiersToUpdate.map(_.newIdentifier)
          _ <-
            if (updatedIdentifier.nonEmpty)
              dependencies.entityClient.updateEntityIdentifiers(entity, identifiersToUpdate.map(_.newIdentifier))
            else IO.unit
          updatedSlackMessage <- generateIdentifierSlackMessage(
            config.apiUrl,
            entity,
            identifiersToUpdate,
            identifiersToAdd
          )
          _ <- updatedSlackMessage.map(msg => dependencies.eventBridgeClient.publishEventToEventBridge(getClass.getName, DR2Message, Detail(msg))).sequence
        } yield ()
      }.sequence
      _ <- folderUpdateRequests.map { folderUpdateRequest =>
        val message = generateTitleDescriptionSlackMessage(config.apiUrl, folderUpdateRequest)
        for {
          _ <- logWithBatchRef(s"Updating entity ${folderUpdateRequest.updateEntityRequest.ref}")
          _ <- dependencies.entityClient.updateEntity(folderUpdateRequest.updateEntityRequest)
          _ <- logWithBatchRef(s"Sending\n$message\nto slack")
          _ <- dependencies.eventBridgeClient.publishEventToEventBridge(getClass.getName, DR2Message, Detail(message))
        } yield ()
      }.sequence
    } yield ()
  }

  private def createFullFolderInfo(sourceIdToEntity: Map[String, Option[Entity]], folderRows: List[ArchiveFolderDynamoTable]) = {
    folderRows.map(row => {
      val potentialParentId = for {
        parentPath <- row.parentPath
        directParent <- parentPath.split("/").lastOption.map(UUID.fromString)
        directParentRow <- folderRows.find(_.id == directParent)
        directParentEntity <- sourceIdToEntity.get(directParentRow.name)
        entity <- directParentEntity
      } yield entity.ref
      FullFolderInfo(row, sourceIdToEntity.get(row.name).flatten, potentialParentId)
    })
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
      folderUpdateRequest: EntityWithUpdateEntityRequest
  ): String = {
    val entity: Entity = folderUpdateRequest.entity
    val firstLineOfMsg: String = generateSlackMessageFirstLine(preservicaUrl, entity, messageWithNewLine = false)
    val updateEntityRequest = folderUpdateRequest.updateEntityRequest

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

  private def checkParentPathsValid(
      folderRows: List[DynamoTable]
  ): IO[Unit] = {
    val rowsWithInvalidPaths = folderRows.flatMap(row =>
      row.parentPath.flatMap { path =>
        path.split("/").toList.map(UUID.fromString) match
          case ::(head, next) =>
            val topLevelFolder = folderRows.find(_.id == head)
            if topLevelFolder.nonEmpty && topLevelFolder.flatMap(_.parentPath).isEmpty && next.forall(id => folderRows.exists(_.id == id)) then None
            else Option(row)
          case Nil => None
      }
    )
    IO.raiseWhen(rowsWithInvalidPaths.nonEmpty)(
      new Exception(
        s"The following rows have invalid paths ${rowsWithInvalidPaths.map(row => s"id: ${row.id}, parentPath: ${row.parentPath.getOrElse("")}").mkString("\n")}"
      )
    )
  }

  private def verifyEntitiesAreStructuralObjects(folderInfoOfEntitiesThatExist: List[FullFolderInfo]): IO[List[Unit]] =
    folderInfoOfEntitiesThatExist.map { folderInfo =>
      val potentialEntityType: Option[EntityClient.EntityType] = folderInfo.entity.flatMap(_.entityType)

      potentialEntityType
        .collect {
          case entityType if entityType != StructuralObject =>
            IO.raiseError(
              new Exception(
                s"The entity type for folder id ${folderInfo.folderRow.id} should be 'StructuralObject' but it is $entityType"
              )
            )
          case _ => IO.unit
        }
        .getOrElse(IO.raiseError(new Exception(s"There is no entity type for folder id ${folderInfo.folderRow.id}")))
    }.sequence

  private def verifyExpectedParentFolderMatchesFolderFromApiAndGetSecurityTag(
      folderInfoOfEntitiesThatExist: List[FullFolderInfo]
  ): IO[List[FullFolderInfo]] =
    folderInfoOfEntitiesThatExist.map { folderInfo =>
      val entity = folderInfo.entity.get
      val ref = entity.ref
      val parentRef = entity.parent

      if (parentRef != folderInfo.expectedParentRef)
        IO.raiseError {
          new Exception(
            s"API returned a parent ref of '${parentRef.getOrElse("None")}' for entity $ref instead of expected '${folderInfo.expectedParentRef.getOrElse("")}'"
          )
        }
      else
        entity.securityTag match {
          case open @ Some(Open)     => IO.pure(folderInfo.copy(securityTag = open))
          case closed @ Some(Closed) => IO.pure(folderInfo.copy(securityTag = closed))
          case unexpectedTag =>
            IO.raiseError(new Exception(s"Security tag '$unexpectedTag' is unexpected for SO ref '$ref'"))
        }
    }.sequence

  private def findIdentifiersToUpdate(
      identifiersFromDynamo: List[Identifier],
      identifiersFromPreservica: Seq[IdentifierResponse]
  ): IO[Seq[IdentifierToUpdate]] = IO {
    identifiersFromDynamo.flatMap { id =>
      identifiersFromPreservica
        .find(pid => pid.identifierName == id.identifierName && pid.value != id.value)
        .map(pid => IdentifierToUpdate(pid, IdentifierResponse(pid.id, id.identifierName, id.value)))
    }
  }

  private def findIdentifiersToAdd(
      identifiersFromDynamo: Seq[Identifier],
      identifiersFromPreservica: Seq[IdentifierResponse]
  ): IO[Seq[Identifier]] = IO {
    identifiersFromDynamo
      .filterNot { id =>
        identifiersFromPreservica.exists(pid => pid.identifierName == id.identifierName)
      }
      .map(id => Identifier(id.identifierName, id.value))
  }

  private def findOnlyFoldersThatNeedUpdatingAndCreateRequests(
      folderInfoOfEntitiesThatExist: List[FullFolderInfo]
  ): List[EntityWithUpdateEntityRequest] =
    folderInfoOfEntitiesThatExist.flatMap { folderInfo =>
      val folderRow = folderInfo.folderRow
      val entity = folderInfo.entity.get

      val potentialNewTitle = folderRow.title.orElse(entity.title)
      val potentialNewDescription = folderRow.description

      val titleHasChanged = potentialNewTitle != entity.title && potentialNewTitle.nonEmpty
      val descriptionHasChanged = potentialNewDescription != entity.description && potentialNewDescription.nonEmpty

      val updateEntityRequest =
        if (titleHasChanged || descriptionHasChanged) {
          val parentRef = folderInfo.expectedParentRef
          val updatedTitleOrDescriptionRequest =
            UpdateEntityRequest(
              entity.ref,
              potentialNewTitle.get,
              if (descriptionHasChanged) potentialNewDescription else None,
              structuralObject,
              folderInfo.securityTag.get,
              parentRef
            )
          Some(updatedTitleOrDescriptionRequest)
        } else None

      updateEntityRequest.map(EntityWithUpdateEntityRequest(entity, _))
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

  private case class FullFolderInfo(
      folderRow: DynamoTable,
      entity: Option[Entity],
      expectedParentRef: Option[UUID],
      securityTag: Option[SecurityTag] = None
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
