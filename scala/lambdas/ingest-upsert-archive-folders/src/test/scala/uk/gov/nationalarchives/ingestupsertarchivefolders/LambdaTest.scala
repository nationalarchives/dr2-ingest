package uk.gov.nationalarchives.ingestupsertarchivefolders

import cats.syntax.all.*
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.Identifier as PreservicaIdentifier
import uk.gov.nationalarchives.ingestupsertarchivefolders.testUtils.ExternalServicesTestUtils

import java.util.UUID

class LambdaTest extends ExternalServicesTestUtils with EitherValues {

  "handler" should "add one entity and identifier to the preservation system if one is not present" in {
    val folderOne = generateItem("1")
    val folderTwo = generateItem("1_1")
    val folderThree = generateItem("1_1_1")
    val entityWithIdentifiersOne = generateEntity("1", folderOne.name)
    val entityWithIdentifiersTwo = generateEntity("1_1", folderTwo.name)
    val entitiesWithIdentifiers = List(entityWithIdentifiersOne, entityWithIdentifiersTwo)
    val (resultEntities, _) = runLambda(List(folderOne, folderTwo, folderThree), entitiesWithIdentifiers)

    val result = resultEntities.find(_.entity.title.contains("mock title_1_1_1"))
    result.isDefined should be(true)
    val identifier = result.get.identifiers.find(id => id.identifierName == "SourceID" && id.value == "mock name_1_1_1")
    identifier.isDefined should be(true)
  }

  "handler" should "create three entities with the correct parent hierarchy if none are in the preservation system" in {
    val folderOne = generateItem("1")
    val folderTwo = generateItem("1_1", folderOne.id.toString.some)
    val folderThree = generateItem("1_1_1", s"${folderOne.id.toString}/${folderTwo.id.toString}".some)
    val (entities, _) = runLambda(List(folderOne, folderTwo, folderThree), Nil)

    entities.size should equal(3)
    validateParentHierarchy(entities.map(_.entity)) should equal(true)
  }

  "handler" should "create an entity using the item name if the title is missing " in {
    val folderOne = generateItem("1").copy(potentialTitle = None)
    val (entities, _) = runLambda(List(folderOne), Nil)

    entities.size should equal(1)
    val entity = entities.head

    entity.entity.title.get should equal(folderOne.name)
    entity.identifiers.find(_.identifierName == "SourceID").get.value should equal(folderOne.name)
  }

  forAll(missingTitleInDbScenarios ++ missingDescriptionInDbScenarios) { (test, titleFromDb, titleFromPreservica, descriptionFromDb, descriptionFromPreservica) =>
    "handler" should s"update the title and description correctly in the preservation system if $test" in {
      val folder = generateItem().copy(potentialTitle = titleFromDb, potentialDescription = descriptionFromDb)
      val entityWithIdentifiers = generateEntity("", folder.name)
      val updatedEntity = entityWithIdentifiers.entity.copy(title = titleFromPreservica, description = descriptionFromPreservica)

      val (entitiesResponse, _) = runLambda(List(folder), List(entityWithIdentifiers.copy(entity = updatedEntity)))

      val expectedTitle = titleFromDb <+> titleFromPreservica
      val expectedDescription = descriptionFromDb <+> descriptionFromPreservica

      entitiesResponse.head.entity.title should equal(expectedTitle)
      entitiesResponse.head.entity.description should equal(expectedDescription)

    }
  }

  forAll(identifierScenarios) { (identifierFromDynamo, identifierFromPreservica, expectedResult) =>
    {
      "handler" should s"update $identifierFromPreservica to $identifierFromDynamo correctly in the preservation system" in {
        val folder = generateItem(identifiers = identifierFromDynamo).copy(name = identifierFromDynamo.head.value)
        val sourceIdentifier = PreservicaIdentifier("SourceID", folder.name)
        val entitiesWithIdentifier = generateEntity().copy(identifiers = sourceIdentifier :: identifierFromPreservica)

        val (entities, _) = runLambda(List(folder), List(entitiesWithIdentifier))

        entities.size should equal(1)
        val resultIdentifiers = entities.head.identifiers
          .filterNot(_.identifierName == "SourceID")
          .sortBy(_.identifierName)

        resultIdentifiers should equal(expectedResult.sortBy(_.identifierName))
      }
    }
  }

  "handler" should "raise an error if there is an error getting the dynamo items" in {
    val (_, err) = runLambda(Nil, Nil, true)

    err.getMessage should be("table-name not found")
  }

  "handler" should "return an error if the parent folder structure is invalid" in {
    val invalidParentRef = UUID.randomUUID()
    val folderOne = generateItem("1")
    val folderTwo = generateItem("1_1", folderOne.id.toString.some)
    val entityWithIdentifiers = generateEntity("1", folderTwo.name, invalidParentRef.some)
    val (_, err) = runLambda(List(folderOne, folderTwo), List(entityWithIdentifiers))

    err.getMessage should be(
      s"API returned a parent ref of '$invalidParentRef' for entity ${entityWithIdentifiers.entity.ref} instead of expected ''"
    )
  }

  "handler" should "return an error if the preservation system returns an error getting the entities by identifier" in {

    val (_, err) = runLambda(Nil, Nil, preservicaErrors = PreservicaErrors(true).some)
    err.getMessage should be("API has encountered and issue")
  }

  "handler" should "throw an exception if more than 1 entity has the same SourceID" in {
    val folderOne = generateItem("1")
    val folderTwo = generateItem("1_1")
    val entityWithIdentifiersOne = generateEntity("1", folderOne.name)
    val entityWithIdentifiersTwo = generateEntity("1", folderOne.name)
    val (_, err) = runLambda(List(folderOne, folderTwo), List(entityWithIdentifiersOne, entityWithIdentifiersTwo))

    err.getMessage should be("There is more than 1 entity with these SourceIDs: mock name_1")

  }

  "handler" should "return an error if there are errors creating the entities" in {
    val (entities, err) = runLambda(List(generateItem()), Nil, preservicaErrors = PreservicaErrors(addEntity = true).some)

    err.getMessage should be("API has encountered an issue adding entity")
    entities.size should equal(0)
  }

  "handler" should "call the DDB client's 'getAttributeValues' method and call entities client's 'entitiesByIdentifier' method 3x " +
    "but throw an exception if any of the entities returned from the API are not SOs" in {

      val folderOne = generateItem("1")
      val entityWithIdentifiers = generateEntity("1", folderOne.name)
      val entity = entityWithIdentifiers.entity.copy(entityType = ContentObject.some)

      val (entities, err) = runLambda(List(folderOne), List(entityWithIdentifiers.copy(entity = entity)))

      err.getMessage should be(
        s"The entity type for folder id ${folderOne.id} should be 'StructuralObject' but it is ContentObject"
      )
    }

  "handler" should "throw an exception if the parent ref from the preservation system doesn't match the one from Dynamo" in {
    val invalidParent = UUID.randomUUID
    val folder = generateItem()
    val entityWithIdentifiers = generateEntity(identifierValue = folder.name, parent = invalidParent.some)

    val (_, err) = runLambda(List(folder), List(entityWithIdentifiers))

    err.getMessage should be(s"API returned a parent ref of '$invalidParent' for entity ${entityWithIdentifiers.entity.ref} instead of expected ''")
  }

  "handler" should "return an error if an entity has no security tag" in {
    val folder = generateItem()
    val entityWithIdentifiers = generateEntity(identifierValue = folder.name)
    val updatedEntity = entityWithIdentifiers.entity.copy(securityTag = None)

    val (_, err) = runLambda(List(folder), List(entityWithIdentifiers.copy(entity = updatedEntity)))

    err.getMessage should be(s"Security tag 'None' is unexpected for SO ref '${updatedEntity.ref}'")
  }

  "handler" should "return an error if an entity has a security tag which is not open, closed nor unknown" in {
    val folder = generateItem()
    val entityWithIdentifiers = generateEntity(identifierValue = folder.name)
    val updatedEntity = entityWithIdentifiers.entity.copy(securityTag = Some(null))

    val (_, err) = runLambda(List(folder), List(entityWithIdentifiers.copy(entity = updatedEntity)))

    err.getMessage should be(s"Security tag 'Some(null)' is unexpected for SO ref '${updatedEntity.ref}'")
  }

  "handler" should "return an error if there is an error updating the entity" in {
    val folder = generateItem("1")
    val entityWithIdentifiers = generateEntity("1_1", identifierValue = folder.name)
    val (_, err) = runLambda(List(folder), List(entityWithIdentifiers), preservicaErrors = PreservicaErrors(updateEntity = true).some)

    err.getMessage should equal("API has encountered an issue updating an entity")
  }
}
