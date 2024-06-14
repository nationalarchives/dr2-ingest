package uk.gov.nationalarchives.ingestupsertarchivefolders

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{times, verify}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{ArchiveFolderDynamoTable, Identifier => DynamoIdentifier}
import uk.gov.nationalarchives.dp.client.EntityClient.{AddEntityRequest, EntityType, UpdateEntityRequest, Identifier as PreservicaIdentifier}
import uk.gov.nationalarchives.dp.client.EntityClient.SecurityTag.*
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*

import java.util.UUID
import scala.collection.immutable.ListMap
import org.scalatest.matchers.should.Matchers.*
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import uk.gov.nationalarchives.ingestupsertarchivefolders.Lambda.{Config, EntityWithUpdateEntityRequest, StepFnInput}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, IdentifierResponse}
import uk.gov.nationalarchives.ingestupsertarchivefolders.testUtils.ExternalServicesTestUtils

import scala.jdk.CollectionConverters.ListHasAsScala

class LambdaTest extends ExternalServicesTestUtils with MockitoSugar {
  private val config: Config = Config("http://localhost:9014", "", "")
  private val parentSo = structuralObjects(0).head.ref
  private val childSo = structuralObjects(1).head.ref
  private val grandChildSo = structuralObjects(2).head.ref

  private val folderIds = folderIdsAndRows.keys.toList

  val input: StepFnInput = StepFnInput("TDD-2023-ABC", folderIds.map(_.toString), Nil, List("a8163bde-7daa-43a7-9363-644f93fe2f2b"))

  private def convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows: ListMap[UUID, ArchiveFolderDynamoTable]) =
    IO.pure(folderIdsAndRows.values.toList)

  "handler" should "call the DDB client's 'getAttributeValues' and entities client's 'entitiesByIdentifier' 3x, " +
    "and 'addEntity' and 'addIdentifiersForEntity' once if 1 folder row's Entity was not returned from the 'entitiesByIdentifier' call" in {
      val responseWithNoEntity = IO.pure(Seq())
      val argumentVerifier = ArgumentVerifier(
        convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
        entitiesWithSourceIdReturnValue = defaultEntitiesWithSourceIdReturnValues.updated(2, responseWithNoEntity),
        addEntityReturnValues = List(IO.pure(childSo))
      )

      new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()

      argumentVerifier.verifyInvocationsAndArgumentsPassed(
        folderIdsAndRows,
        3,
        addEntityRequests = List(
          AddEntityRequest(
            None,
            "mock title_1_1_1",
            Some("mock description_1_1_1"),
            StructuralObject,
            Open,
            Some(childSo)
          )
        ),
        2
      )
    }

  "handler" should "call the DDB client's 'getAttributeValues' and entities client's 'entitiesByIdentifier' 3x, " +
    "and 'addEntity' and 'addIdentifiersForEntity' 3x if 3 folder row's Entities were not returned from the 'entitiesByIdentifier' call" + "" +
    "passing in None as the parentRef for the top-level folder" in {
      val responseWithNoEntity = IO.pure(Seq())
      val argumentVerifier = ArgumentVerifier(
        convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
        entitiesWithSourceIdReturnValue = List(responseWithNoEntity, responseWithNoEntity, responseWithNoEntity),
        addEntityReturnValues = List(
          IO.pure(parentSo),
          IO.pure(childSo),
          IO.pure(grandChildSo)
        )
      )

      new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()

      argumentVerifier.verifyInvocationsAndArgumentsPassed(
        folderIdsAndRows,
        3,
        addEntityRequests = List(
          AddEntityRequest(
            None,
            "mock title_1",
            Some("mock description_1"),
            StructuralObject,
            Open,
            None
          ),
          AddEntityRequest(
            None,
            "mock title_1_1",
            Some("mock description_1_1"),
            StructuralObject,
            Open,
            Some(parentSo)
          ),
          AddEntityRequest(
            None,
            "mock title_1_1_1",
            Some("mock description_1_1_1"),
            StructuralObject,
            Open,
            Some(childSo)
          )
        ),
        6
      )
    }

  "handler" should "call the DDB client's 'getAttributeValues' and entities client's 'entitiesByIdentifier' 3x, " +
    "and 'addEntity' and 'addIdentifiersForEntity' twice if a folder row's Entity was not returned from the 'entitiesByIdentifier' call, nor its parent" in {
      // Since the parent's ref is unknown, Entity's "expectedParentRef" property can not be used and instead needs to use different method
      val responseWithNoEntity = IO.pure(Seq())
      val argumentVerifier =
        ArgumentVerifier(
          convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
          entitiesWithSourceIdReturnValue = defaultEntitiesWithSourceIdReturnValues.take(1) ++ List(responseWithNoEntity, responseWithNoEntity),
          addEntityReturnValues = List(
            IO.pure(childSo),
            IO.pure(grandChildSo)
          )
        )

      new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()

      argumentVerifier.verifyInvocationsAndArgumentsPassed(
        folderIdsAndRows,
        3,
        addEntityRequests = List(
          AddEntityRequest(
            None,
            "mock title_1_1",
            Some("mock description_1_1"),
            StructuralObject,
            Open,
            Some(UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50"))
          ),
          AddEntityRequest(
            None,
            "mock title_1_1_1",
            Some("mock description_1_1_1"),
            StructuralObject,
            Open,
            Some(UUID.fromString("a2d39ea3-6216-4f93-b078-62c7896b174c"))
          )
        ),
        4
      )
    }

  "handler" should "call the DDB client's 'getAttributeValues' and entities client's 'entitiesByIdentifier' 3x, " +
    "and 'addEntity' once (and addIdentifiersForEntity 2x) using the name instead of the title if the folder's title was not found " +
    "and a folder row's Entity was not returned from the 'entitiesByIdentifier' call " in {
      val folderIdsAndRows1stIdModified = folderIdsAndRows.map { case (folderId, response) =>
        if (folderId == UUID.fromString("93f5a200-9ee7-423d-827c-aad823182ad2"))
          folderId -> response.copy(name = "mock name_1_1_1", title = None)
        else folderId -> response
      }
      val responseWithNoEntity = IO.pure(Seq())
      val argumentVerifier =
        ArgumentVerifier(
          convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows1stIdModified),
          entitiesWithSourceIdReturnValue = defaultEntitiesWithSourceIdReturnValues.updated(2, responseWithNoEntity),
          addEntityReturnValues = List(IO.pure(grandChildSo))
        )

      new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()

      argumentVerifier.verifyInvocationsAndArgumentsPassed(
        folderIdsAndRows1stIdModified,
        3,
        addEntityRequests = List(
          AddEntityRequest(
            None,
            "mock name_1_1_1",
            Some("mock description_1_1_1"),
            StructuralObject,
            Open,
            Some(UUID.fromString("a2d39ea3-6216-4f93-b078-62c7896b174c"))
          )
        ),
        2
      )
    }

  forAll(missingTitleInDbScenarios) { (test, titleFromDb, titleFromPreservica, descriptionFromDb, descriptionFromPreservica, result) =>
    "handler" should s"call the DDB client's 'getAttributeValues' and entities client's 'entitiesByIdentifier' 3x and $result if $test" in {
      missingTitleAndDescriptionTestSetup(
        titleFromPreservica,
        descriptionFromPreservica,
        titleFromDb,
        descriptionFromDb,
        result,
        titleFromPreservica.get
      )
    }
  }

  forAll(missingDescriptionInDbScenarios) { (test, titleFromDb, titleFromPreservica, descriptionFromDb, descriptionFromPreservica, result) =>
    "handler" should s"call the DDB client's 'getAttributeValues' and entities client's 'entitiesByIdentifier' 3x and $result if $test" in {
      missingTitleAndDescriptionTestSetup(
        titleFromPreservica,
        descriptionFromPreservica,
        titleFromDb,
        descriptionFromDb,
        result,
        titleFromDb.get
      )
    }
  }

  forAll(identifierScenarios) { (identifierFromDynamo, identifierFromPreservica, addIdentifierRequest, updateIdentifierRequest, addDescription, updateDescription) =>
    {
      "handler" should s"$addDescription and $updateDescription" in {
        val rowsWithIdentifiers = folderIdsAndRows.take(1).map { case (id, dynamoResponse) =>
          id -> dynamoResponse.copy(identifiers = identifierFromDynamo.map(ti => DynamoIdentifier(ti.name, ti.value)))
        }
        val argumentVerifier =
          ArgumentVerifier(
            convertFolderIdsAndRowsToListOfIoRows(rowsWithIdentifiers),
            getIdentifiersForEntityReturnValues = IO.pure(identifierFromPreservica.map(idp => IdentifierResponse("id", idp.name, idp.value)))
          )
        new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()

        val addIdentifierCaptor: ArgumentCaptor[PreservicaIdentifier] = ArgumentCaptor.forClass(classOf[PreservicaIdentifier])
        val updateIdentifierCaptor: ArgumentCaptor[Seq[IdentifierResponse]] =
          ArgumentCaptor.forClass(classOf[Seq[IdentifierResponse]])

        verify(argumentVerifier.mockEntityClient, times(addIdentifierRequest.size))
          .addIdentifierForEntity(any[UUID], any[EntityType], addIdentifierCaptor.capture())
        verify(argumentVerifier.mockEntityClient, times(updateIdentifierRequest.length))
          .updateEntityIdentifiers(any[Entity], updateIdentifierCaptor.capture())

        val addIdentifierValues: List[PreservicaIdentifier] = addIdentifierCaptor.getAllValues.asScala.toList
        addIdentifierValues.map(aiv => TestIdentifier(aiv.identifierName, aiv.value)) should equal(addIdentifierRequest)

        val updateIdentifierValues: Seq[PreservicaIdentifier] = updateIdentifierCaptor.getAllValues.asScala.headOption
          .getOrElse(Nil)
          .map(id => PreservicaIdentifier(id.identifierName, id.value))
        updateIdentifierValues.map(iv => TestIdentifier(iv.identifierName, iv.value)) should equal(updateIdentifierRequest)
      }
    }
  }

  "handler" should "call the DDB client's 'getAttributeValues' and entities client's 'entitiesByIdentifier' 3x, " +
    "'addEntity' and 'addIdentifiersForEntity' once if 1 folder row's Entity was not returned from the 'entitiesByIdentifier' " +
    "and 'updateEntity' once if folder's title is different from what's DDB" in {
      val ref = UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50")
      val entityWithAnOldTitle = structuralObjects(0).map(_.copy(title = Some("mock title_old_1")))
      val responseWithNoEntity = IO.pure(Seq())
      val argumentVerifier =
        ArgumentVerifier(
          convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
          entitiesWithSourceIdReturnValue = defaultEntitiesWithSourceIdReturnValues
            .updated(0, IO.pure(entityWithAnOldTitle))
            .updated(2, responseWithNoEntity),
          addEntityReturnValues = List(IO.pure(childSo))
        )

      new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()

      argumentVerifier.verifyInvocationsAndArgumentsPassed(
        folderIdsAndRows,
        3,
        addEntityRequests = List(
          AddEntityRequest(
            None,
            "mock title_1_1_1",
            Some("mock description_1_1_1"),
            StructuralObject,
            Open,
            Some(UUID.fromString("a2d39ea3-6216-4f93-b078-62c7896b174c"))
          )
        ),
        2,
        updateEntityRequests = List(
          EntityWithUpdateEntityRequest(
            entityWithAnOldTitle.find(_.ref == ref).get,
            UpdateEntityRequest(
              ref,
              "mock title_1",
              None,
              StructuralObject,
              Open,
              None
            )
          )
        )
      )
    }

  "handler" should "call the DDB client's 'getAttributeValues' and entities client's 'entitiesByIdentifier' 3x " +
    "and 'updateEntity' for each of the 3 folders' changes (title, description, both) if each folder's " +
    "title/description is different from what's DDB" in {
      val entityWithAnOldTitle = structuralObjects(0).map(_.copy(title = Some("mock title_old_1")))
      val entityWithAnOldDescription =
        structuralObjects(1).map(_.copy(description = Some("mock description_old_1_1")))
      val entityWithAnOldTitleAndDescription = structuralObjects(2).map(
        _.copy(title = Some("mock title_old_1_1_1"), description = Some("mock description_old_1_1_1"))
      )
      val allEntities = List(entityWithAnOldTitle, entityWithAnOldDescription, entityWithAnOldTitleAndDescription)

      def findEntity(uuid: String): Entity = allEntities.flatten.find(_.ref == UUID.fromString(uuid)).get
      val argumentVerifier =
        ArgumentVerifier(
          convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
          entitiesWithSourceIdReturnValue = allEntities.map(e => IO.pure(e))
        )

      new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()

      argumentVerifier.verifyInvocationsAndArgumentsPassed(
        folderIdsAndRows,
        3,
        updateEntityRequests = List(
          EntityWithUpdateEntityRequest(
            findEntity("d7879799-a7de-4aa6-8c7b-afced66a6c50"),
            UpdateEntityRequest(
              UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50"),
              "mock title_1",
              None,
              StructuralObject,
              Open,
              None
            )
          ),
          EntityWithUpdateEntityRequest(
            findEntity("a2d39ea3-6216-4f93-b078-62c7896b174c"),
            UpdateEntityRequest(
              UUID.fromString("a2d39ea3-6216-4f93-b078-62c7896b174c"),
              "mock title_1_1",
              Some("mock description_1_1"),
              StructuralObject,
              Open,
              Some(UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50"))
            )
          ),
          EntityWithUpdateEntityRequest(
            findEntity("9dfc40be-5f44-4fa1-9c25-fbe03dd3f539"),
            UpdateEntityRequest(
              UUID.fromString("9dfc40be-5f44-4fa1-9c25-fbe03dd3f539"),
              "mock title_1_1_1",
              Some("mock description_1_1_1"),
              StructuralObject,
              Open,
              Some(UUID.fromString("a2d39ea3-6216-4f93-b078-62c7896b174c"))
            )
          )
        )
      )
    }

  "handler" should "call the DDB client's 'getAttributeValues' and entities client's 'entitiesByIdentifier' 3x " +
    "but not any add or update methods, if there is no difference between what the API and DDB returned " in {
      val argumentVerifier =
        ArgumentVerifier(
          convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
          entitiesWithSourceIdReturnValue = defaultEntitiesWithSourceIdReturnValues
        )

      new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()

      argumentVerifier.verifyInvocationsAndArgumentsPassed(
        folderIdsAndRows,
        3
      )
    }

  "handler" should "call the DDB client's 'getItems' method but not call anything else" +
    "in DynamoDB if an exception was thrown when it tried to get the datetime" in {
      val argumentVerifier = ArgumentVerifier(
        getAttributeValuesReturnValue = IO.raiseError(
          ResourceNotFoundException
            .builder()
            .message("Table name not found")
            .build()
        )
      )

      val thrownException = intercept[ResourceNotFoundException] {
        new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be("Table name not found")

      argumentVerifier.verifyInvocationsAndArgumentsPassed(folderIdsAndRows, 0)
    }

  "handler" should "call the DDB client's 'getItems' method and throw an exception when sorted parent folder path length isn't '0, 1, 2'" in {
    val lastElementFolderRow = folderIdsAndRows(UUID.fromString("93f5a200-9ee7-423d-827c-aad823182ad2"))
    val lastElementFolderRowsWithTooShortOfAParentPath =
      lastElementFolderRow.copy(parentPath = Option("e88e433a-1f3e-48c5-b15f-234c0e663c27"))
    val folderIdsAndRowsWithParentPathMistake =
      folderIdsAndRows + (UUID.fromString("93f5a200-9ee7-423d-827c-aad823182ad2") -> lastElementFolderRowsWithTooShortOfAParentPath)

    val argumentVerifier = ArgumentVerifier(convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRowsWithParentPathMistake))

    val thrownException = intercept[Exception] {
      new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
    }

    thrownException.getMessage should be(
      "The lengths of the parent paths should increase by 1 for each subfolder (from 0 to N); instead it was 0, 1, 1"
    )

    argumentVerifier.verifyInvocationsAndArgumentsPassed(folderIdsAndRowsWithParentPathMistake, 0)
  }

  "handler" should "only call the DDB client's 'getItems' method and throw an exception if the parent path of a folder " +
    "does not match folder before it (after sorting)" in {
      val lastElementFolderRow = folderIdsAndRows(UUID.fromString("93f5a200-9ee7-423d-827c-aad823182ad2"))
      val lastElementFolderRowsWithIncorrectParentPath =
        lastElementFolderRow.copy(parentPath = Option("f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176/137bb3f9-3ae4-4e69-9d06-e7d569968ed2"))
      val folderIdsAndRowsWithParentPathMistake =
        folderIdsAndRows + (UUID.fromString("93f5a200-9ee7-423d-827c-aad823182ad2") -> lastElementFolderRowsWithIncorrectParentPath)

      val argumentVerifier = ArgumentVerifier(convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRowsWithParentPathMistake))

      val thrownException = intercept[Exception] {
        new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be(
        "The parent ref of subfolder 93f5a200-9ee7-423d-827c-aad823182ad2 is 137bb3f9-3ae4-4e69-9d06-e7d569968ed2: " +
          "this does not match the id of its presumed parent e88e433a-1f3e-48c5-b15f-234c0e663c27"
      )

      argumentVerifier.verifyInvocationsAndArgumentsPassed(folderIdsAndRowsWithParentPathMistake, 0)
    }

  "handler" should "call the DDB client's 'getAttributeValues' method and call entities client's 'entitiesByIdentifier' method 3x but " +
    "throw an exception if the API returns an Exception when attempting to get an entity by its identifier" in {
      val argumentVerifier = ArgumentVerifier(
        convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
        entitiesWithSourceIdReturnValue = List(IO.raiseError(new Exception("API has encountered and issue")), IO.pure(Nil), IO.pure(Nil))
      )

      val thrownException = intercept[Exception] {
        new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be("API has encountered and issue")

      argumentVerifier.verifyInvocationsAndArgumentsPassed(folderIdsAndRows, 3)
    }

  "handler" should "call the DDB client's 'getAttributeValues' method and call entities client's 'entitiesByIdentifier' method 3x " +
    "but throw an exception if more than 1 entity has the same SourceID" in {
      val argumentVerifier = ArgumentVerifier(
        convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
        entitiesWithSourceIdReturnValue = List(
          IO.pure(
            Seq(
              Entity(
                Some(StructuralObject),
                UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50"),
                Some("mock title_1"),
                Some("mock description_1"),
                deleted = false,
                Some(StructuralObject.entityPath)
              ),
              Entity(
                Some(StructuralObject),
                UUID.fromString("124b0e7b-cf01-4d61-b284-c5db1adece32"),
                Some("mock title_2"),
                Some("Another SO with the same SourceID description_1"),
                deleted = false,
                Some(StructuralObject.entityPath)
              )
            )
          ),
          IO.pure(Nil),
          IO.pure(Nil)
        )
      )

      val thrownException = intercept[Exception] {
        new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be(
        "There is more than 1 entity with the same SourceID as f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176"
      )

      argumentVerifier.verifyInvocationsAndArgumentsPassed(folderIdsAndRows, 3)
    }

  "handler" should "call the DDB client's 'getAttributeValues' method and call entities client's 'entitiesByIdentifier' method 3x " +
    "and addEntity once but throw an exception if the API returns an Exception when attempting to create the SO" in {
      val responseWithNoEntity = IO.pure(Seq())
      val argumentVerifier =
        ArgumentVerifier(
          convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
          entitiesWithSourceIdReturnValue = defaultEntitiesWithSourceIdReturnValues.updated(2, responseWithNoEntity),
          addEntityReturnValues = List(
            IO.raiseError(new Exception("API has encountered an issue adding entity")),
            IO.raiseError(new Exception("API has encountered an issue adding entity")),
            IO.raiseError(new Exception("API has encountered an issue adding entity"))
          )
        )

      val thrownException = intercept[Exception] {
        new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be("API has encountered an issue adding entity")

      argumentVerifier.verifyInvocationsAndArgumentsPassed(
        folderIdsAndRows,
        3,
        addEntityRequests = List(
          AddEntityRequest(
            None,
            "mock title_1_1_1",
            Some("mock description_1_1_1"),
            StructuralObject,
            Open,
            Some(childSo)
          )
        )
      )
    }

  "handler" should "call the DDB client's 'getAttributeValues' method and call entities client's 'entitiesByIdentifier' method 3x " +
    "and 'addEntity' and 'addIdentifiersForEntity' once but throw an exception if the API returns an Exception when attempting to add an identifier" in {
      val responseWithNoEntity = IO.pure(Seq())
      val argumentVerifier =
        ArgumentVerifier(
          convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
          entitiesWithSourceIdReturnValue = defaultEntitiesWithSourceIdReturnValues.updated(2, responseWithNoEntity),
          addEntityReturnValues = List(IO.pure(childSo)),
          addIdentifierReturnValue = IO.raiseError(new Exception("API has encountered an issue adding identifier"))
        )

      val thrownException = intercept[Exception] {
        new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be("API has encountered an issue adding identifier")

      argumentVerifier.verifyInvocationsAndArgumentsPassed(
        folderIdsAndRows,
        3,
        addEntityRequests = List(
          AddEntityRequest(
            None,
            "mock title_1_1_1",
            Some("mock description_1_1_1"),
            StructuralObject,
            Open,
            Some(UUID.fromString("a2d39ea3-6216-4f93-b078-62c7896b174c"))
          )
        ),
        2
      )
    }

  "handler" should "call the DDB client's 'getAttributeValues' method and call entities client's 'entitiesByIdentifier' method 3x " +
    "but throw an exception if any of the entities returned from the API are not SOs" in {
      val contentObjectResponse = IO {
        structuralObjects(0).map {
          _.copy(entityType = Some(ContentObject), path = Some("content-objects"))
        }
      }

      val argumentVerifier = ArgumentVerifier(
        convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
        entitiesWithSourceIdReturnValue = defaultEntitiesWithSourceIdReturnValues.updated(2, contentObjectResponse)
      )

      val thrownException = intercept[Exception] {
        new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be(
        "The entity type for folder id 93f5a200-9ee7-423d-827c-aad823182ad2 should be 'StructuralObject' but it is ContentObject"
      )

      argumentVerifier.verifyInvocationsAndArgumentsPassed(folderIdsAndRows, 3)
    }

  "handler" should "call the DDB client's 'getAttributeValues' and entities client's 'entitiesByIdentifier'" +
    "but throw an exception if any of the parents of the entities' returned, don't match the parents of folders found in the DB" in {
      val argumentVerifier = ArgumentVerifier(
        convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
        List(
          IO.pure(structuralObjects(0)),
          IO.pure(structuralObjects(1).map(_.copy(parent = Some(UUID.fromString("c5e50662-2b3d-4924-8e4b-53a543800507"))))),
          IO.pure(structuralObjects(2))
        )
      )

      val thrownException = intercept[Exception] {
        new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be(
        "API returned a parent ref of 'c5e50662-2b3d-4924-8e4b-53a543800507' for entity a2d39ea3-6216-4f93-b078-62c7896b174c " +
          "instead of expected 'd7879799-a7de-4aa6-8c7b-afced66a6c50'"
      )

      argumentVerifier.verifyInvocationsAndArgumentsPassed(folderIdsAndRows, 3)
    }

  "handler" should "call the DDB client's 'getAttributeValues' and entities client's 'entitiesByIdentifier'" +
    "but throw an exception if the parent ref of the top-level folder is populated" in {
      val argumentVerifier = ArgumentVerifier(
        convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
        List(
          IO.pure(structuralObjects(0).map(_.copy(parent = Some(UUID.fromString("c5e50662-2b3d-4924-8e4b-53a543800507"))))),
          IO.pure(structuralObjects(1)),
          IO.pure(structuralObjects(2))
        )
      )

      val thrownException = intercept[Exception] {
        new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be(
        "API returned a parent ref of 'c5e50662-2b3d-4924-8e4b-53a543800507' for entity d7879799-a7de-4aa6-8c7b-afced66a6c50 " +
          "instead of expected ''"
      )

      argumentVerifier.verifyInvocationsAndArgumentsPassed(folderIdsAndRows, 3)
    }

  "handler" should "call the DDB client's 'getAttributeValues' and entities client's 'entitiesByIdentifier' " +
    "but throw an exception if an entity has no security tag" in {
      val argumentVerifier = ArgumentVerifier(
        convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
        List(
          IO.pure(structuralObjects(0).map(_.copy(securityTag = None))),
          IO.pure(structuralObjects(1)),
          IO.pure(structuralObjects(2))
        )
      )

      val thrownException = intercept[Exception] {
        new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be(
        "Security tag 'None' is unexpected for SO ref 'd7879799-a7de-4aa6-8c7b-afced66a6c50'"
      )

      argumentVerifier.verifyInvocationsAndArgumentsPassed(folderIdsAndRows, 3)
    }

  "handler" should "call the DDB client's 'getAttributeValues' and entities client's 'entitiesByIdentifier' " +
    "but throws an exception if an entity has a security tag with a value other that 'open' or 'closed'" in {
      val argumentVerifier = ArgumentVerifier(
        convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
        List(
          IO.pure(structuralObjects(0).map(_.copy(securityTag = Some(null)))),
          IO.pure(structuralObjects(1)),
          IO.pure(structuralObjects(2))
        )
      )

      val thrownException = intercept[Exception] {
        new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be(
        "Security tag 'Some(null)' is unexpected for SO ref 'd7879799-a7de-4aa6-8c7b-afced66a6c50'"
      )

      argumentVerifier.verifyInvocationsAndArgumentsPassed(folderIdsAndRows, 3)
    }

  "handler" should "call the DDB client's 'getAttributeValues' and entities client's 'entitiesByIdentifier' " +
    "and 'updateEntity' method 3x but throws an exception if the API returns an Exception when attempting to update an SO" in {
      val entityWithAnOldTitle = structuralObjects(0).map(_.copy(title = Some("mock title_old_1")))
      val ref = UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50")
      val argumentVerifier = ArgumentVerifier(
        convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows),
        entitiesWithSourceIdReturnValue = defaultEntitiesWithSourceIdReturnValues.updated(0, IO.pure(entityWithAnOldTitle)),
        updateEntityReturnValues = IO.raiseError(new Exception("API has encountered and issue"))
      )

      val thrownException = intercept[Exception] {
        new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()
      }

      thrownException.getMessage should be("API has encountered and issue")

      argumentVerifier.verifyInvocationsAndArgumentsPassed(
        folderIdsAndRows,
        3,
        updateEntityRequests = List(
          EntityWithUpdateEntityRequest(
            entityWithAnOldTitle.find(_.ref == ref).get,
            UpdateEntityRequest(
              ref,
              "mock title_1",
              None,
              StructuralObject,
              Open,
              None
            )
          )
        )
      )
    }

  private def missingTitleAndDescriptionTestSetup(
      titleFromPreservica: Option[String],
      descriptionFromPreservica: Option[String],
      titleFromDb: Option[String],
      descriptionFromDb: Option[String],
      result: String,
      titleToUpdate: String
  ): Unit = {

    val entityWithAnOldTitle =
      structuralObjects(0).map(_.copy(title = titleFromPreservica, description = descriptionFromPreservica))

    val folderIdsAndRows1stIdModified = folderIdsAndRows.map { case (folderId, response) =>
      if (folderId == UUID.fromString("f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176"))
        folderId -> response.copy(title = titleFromDb, description = descriptionFromDb)
      else folderId -> response
    }

    val argumentVerifier =
      ArgumentVerifier(
        convertFolderIdsAndRowsToListOfIoRows(folderIdsAndRows1stIdModified),
        entitiesWithSourceIdReturnValue = defaultEntitiesWithSourceIdReturnValues.updated(0, IO.pure(entityWithAnOldTitle))
      )

    new Lambda().handler(input, config, argumentVerifier.dependencies).unsafeRunSync()

    val updateRequest =
      if (result == "make no calls to 'updateEntity'") Nil
      else
        List(
          EntityWithUpdateEntityRequest(
            entityWithAnOldTitle.head,
            UpdateEntityRequest(
              UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50"),
              titleToUpdate,
              descriptionFromDb,
              StructuralObject,
              Open,
              None
            )
          )
        )

    argumentVerifier.verifyInvocationsAndArgumentsPassed(
      folderIdsAndRows1stIdModified,
      3,
      updateEntityRequests = updateRequest
    )
  }
}
