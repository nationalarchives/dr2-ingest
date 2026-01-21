package uk.gov.nationalarchives.ingestassetreconciler

import cats.syntax.all.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1, TableFor2}
import org.scalatest.{BeforeAndAfterEach, EitherValues}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Checksum
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.ingestassetreconciler.Lambda.{Config, Failures, StateOutput}
import uk.gov.nationalarchives.ingestassetreconciler.Lambda.FailureReason.*
import uk.gov.nationalarchives.ingestassetreconciler.testUtils.ExternalServicesTestUtils.*

import java.util.UUID

class LambdaTest extends AnyFlatSpec with BeforeAndAfterEach with TableDrivenPropertyChecks with EitherValues {
  val config: Config = Config("", "", "test-table")

  case class State(checksum: String, title: String, name: String)
  case class DbEntityStates(dynamoState: State, entityState: State, wasReconciled: Boolean)

  val contentObjectApiVsDdbStates: TableFor1[DbEntityStates] = Table(
    "State",
    DbEntityStates(State("checksum", "title", "name"), State("checksum", "title", "name"), true),
    DbEntityStates(State("checksum", "title", "name"), State("checksum", "title", "anotherName"), true),
    DbEntityStates(State("checksum", "title", "name"), State("checksum", "anotherTitle", "name"), false),
    DbEntityStates(State("checksum", "title", "name"), State("checksum", "anotherTitle", "anotherName"), false),
    DbEntityStates(State("checksum", "title", "name"), State("anotherChecksum", "title", "name"), false),
    DbEntityStates(State("checksum", "title", "name"), State("anotherChecksum", "title", "anotherName"), false),
    DbEntityStates(State("checksum", "title", "name"), State("anotherChecksum", "anotherTitle", "name"), false),
    DbEntityStates(State("checksum", "title", "name"), State("anotherChecksum", "anotherTitle", "anotherName"), false)
  )

  val uncommonButAcceptableFileExtensionStates: TableFor2[String, String] = Table(
    ("Child of Asset DocxTitle", "Entity Title"),
    ("", "TestTitle.docx"),
    ("fileNameWithNoExtension", "fileNameWithNoExtension"),
    ("file.name.with.dots.but.no_real_extension", "file.name.with.dots.but.no_real_extension"),
    ("file.name.with.dots.and.extension.docx", "file.name.with.dots.and.extension"),
    ("file.name.with.dots.and.extension", "file.name.with.dots.and.extension.docx")
  )

  val uncommonButUnacceptableFileExtensionStates: TableFor2[String, String] = Table(
    ("Child of Asset DocxTitle", "Entity Title"),
    ("file.name.with.dots.and.ext.docx", "file.name.with..more...dots.than.expected.and.ext.docx"),
    ("file.name.with..more...dots.than.expected.and.ext.docx", "file.name.with.dots.and.ext.docx")
  )

  "handler" should "return an error if the asset is not found in Dynamo" in {
    val id = UUID.randomUUID
    val (_, _, res) = runLambda(generateInput(id), Nil, Nil)
    res.left.value.getMessage should equal(s"No asset found for $id from $batchId in the files table")
  }

  "handler" should "return an error if the Dynamo entry does not have a type of 'Asset'" in {
    val asset = generateAsset.copy(`type` = ArchiveFolder)
    val (_, _, res) = runLambda(generateInput(asset.id), List(AssetWithChildren(asset, Nil)), Nil)
    res.left.value.getMessage should equal(s"Object ${asset.id} in the files table is of type 'ArchiveFolder' and not 'Asset'")
  }

  "handler" should "return an error if more than one entity has the same asset name as its SourceId" in {
    val asset = generateAsset
    val entities = List(generateFullEntity(asset.id), generateFullEntity(asset.id))
    val (_, _, res) = runLambda(generateInput(asset.id), List(AssetWithChildren(asset, Nil)), entities)

    res.left.value.getMessage should equal(s"More than 1 entity found in Preservation System, using SourceID '${asset.id}'")
  }

  "handler" should "return an error if no children are found for the asset" in {
    val asset = generateAsset.copy(childCount = 0)
    val entities = List(generateFullEntity(asset.id))
    val (_, _, res) = runLambda(generateInput(asset.id), List(AssetWithChildren(asset, Nil)), entities)

    res.left.value.getMessage should equal(s"No children were found for ${asset.id} from $batchId in the files table")
  }

  "handler" should "return a 'wasReconciled' value of 'false' with no 'Failures' if there were no entities that had the asset name as the SourceID" in {
    val asset = generateAsset
    val (_, _, res) = runLambda(generateInput(asset.id), List(AssetWithChildren(asset, Nil)), Nil)
    res should equal(Right(StateOutput(false, List(Failures(NoEntityFoundWithSourceId, Nil)), asset.id, None)))
  }

  "handler" should "return a 'wasReconciled' value of 'false' and a 'No entity found' 'reason' if there were no Content Objects belonging to the asset" in {
    val asset = generateAsset
    val children = List(generateFile)
    val entities = List(generateFullEntity(asset.id).copy(contentObjects = Nil))
    val (_, _, res) = runLambda(generateInput(asset.id), List(AssetWithChildren(asset, children)), entities)

    val stateOutput = res.value
    val failure = stateOutput.failures.head

    stateOutput.wasReconciled should equal(false)
    failure.failureReason should equal(NoContentObjects)
    failure.childIds.head should equal(children.head.id)
  }

  forAll(contentObjectApiVsDdbStates) { state =>
    "handler" should s"return was reconciled ${state.wasReconciled} for state $state" in {
      val dynamoState = state.dynamoState
      val entityState = state.entityState
      val asset = generateAsset
      val entities = List(generateFullEntity(asset.id, entityState.title.some, entityState.checksum.some))
      val file = generateFile.copy(checksums = List(Checksum("sha256", dynamoState.checksum)), potentialTitle = dynamoState.title.some, name = dynamoState.name)
      val (_, _, res) = runLambda(generateInput(asset.id), List(AssetWithChildren(asset, List(file))), entities)

      val stateOutput = res.value

      stateOutput.wasReconciled should equal(state.wasReconciled)
      if !stateOutput.wasReconciled then
        val failure = stateOutput.failures.head
        failure.failureReason should equal(TitleChecksumMismatch)
        failure.childIds should equal(List(file.id))
    }
  }

  forAll(uncommonButUnacceptableFileExtensionStates) { (childTitle, entityTitle) =>
    "handler" should s"return a 'wasReconciled' value of 'false' for DDB title $childTitle and entity title $entityTitle" in {
      val asset = generateAsset
      val entities = List(generateFullEntity(asset.id, entityTitle.some))
      val file = generateFile.copy(potentialTitle = childTitle.some, name = "TestTitle")
      val (_, _, res) = runLambda(generateInput(asset.id), List(AssetWithChildren(asset, List(file))), entities)

      val stateOutput = res.value
      stateOutput.wasReconciled should equal(false)
    }
  }

  "handler" should "return an error if the child count and the number of children don't match" in {
    val asset = generateAsset.copy(childCount = 3)
    val entities = List(generateFullEntity(asset.id))
    val file = generateFile
    val (_, _, res) = runLambda(generateInput(asset.id), List(AssetWithChildren(asset, List(file, file))), entities)

    val ex = res.left.value
    ex.getMessage should equal(s"Asset id ${asset.id}: has a 'childCount' of 3 in the files table but only 2 children were found in the files table")
  }

  "handler" should "return a 'wasReconciled' value of 'true' and an empty 'reason' if COs could be reconciled" in {
    val asset = generateAsset
    val fullEntity = generateFullEntity(asset.id, "title".some)
    val (_, _, res) = runLambda(generateInput(asset.id), List(AssetWithChildren(asset, List(generateFile))), List(fullEntity))

    res.value.ioRef.get should equal(fullEntity.entity.ref)
    res.value.wasReconciled should equal(true)
  }

  "handler" should "return a 'wasReconciled' value of 'true' and an empty 'reason' if there are multiple checksums to match in Dynamo" in {
    val asset = generateAsset
    val dynamoFile = generateFile
    val twoChecksumFile = dynamoFile.copy(checksums = Checksum("sha1", "checksum2") :: dynamoFile.checksums)
    val fullEntity = generateFullEntity(asset.id, "title".some)
    val (_, _, res) = runLambda(generateInput(asset.id), List(AssetWithChildren(asset, List(twoChecksumFile))), List(fullEntity))

    res.value.ioRef.get should equal(fullEntity.entity.ref)
    res.value.wasReconciled should equal(true)
  }

  "handler" should "return a 'wasReconciled' value of 'false' and a 'TitleChecksumMismatch' reason if one of two checksums doesn't match" in {
    val asset = generateAsset
    val dynamoFile = generateFile
    val twoChecksumFile = dynamoFile.copy(checksums = Checksum("sha1", "checksum1") :: dynamoFile.checksums)
    val fullEntity = generateFullEntity(asset.id, "title".some)
    val (_, _, res) = runLambda(generateInput(asset.id), List(AssetWithChildren(asset, List(twoChecksumFile))), List(fullEntity))

    res.value.wasReconciled should equal(false)
    res.value.failures.head.failureReason.toString should equal("TitleChecksumMismatch")
  }

  forAll(uncommonButAcceptableFileExtensionStates) { (childTitle, entityTitle) =>
    "handler" should s"return a 'wasReconciled' value of 'true' if the DDB title is $childTitle and the entity title is $entityTitle" in {
      val asset = generateAsset
      val entities = List(generateFullEntity(asset.id, entityTitle.some))
      val file = generateFile.copy(potentialTitle = childTitle.some, name = "TestTitle")
      val (_, _, res) = runLambda(generateInput(asset.id), List(AssetWithChildren(asset, List(file))), entities)

      val stateOutput = res.value
      stateOutput.ioRef.get should equal(entities.head.entity.ref)
      stateOutput.wasReconciled should equal(true)
    }
  }

  "handler" should "return a 'wasReconciled' value of 'false' if the child title is missing" in {
    val asset = generateAsset
    val entities = List(generateFullEntity(asset.id))
    val file = generateFile.copy(potentialTitle = None)
    val (_, _, res) = runLambda(generateInput(asset.id), List(AssetWithChildren(asset, List(file))), entities)

    val stateOutput = res.value
    stateOutput.wasReconciled should equal(false)
  }
}
