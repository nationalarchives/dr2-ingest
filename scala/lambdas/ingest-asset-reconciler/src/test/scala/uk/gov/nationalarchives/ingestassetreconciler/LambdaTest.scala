package uk.gov.nationalarchives.ingestassetreconciler

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.tomakehurst.wiremock.WireMockServer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor3, TableFor4, TableFor6}
import uk.gov.nationalarchives.ingestassetreconciler.Lambda.Config
import uk.gov.nationalarchives.ingestassetreconciler.testUtils.ExternalServicesTestUtils

class LambdaTest extends AnyFlatSpec with BeforeAndAfterEach with TableDrivenPropertyChecks {
  val dynamoServer = new WireMockServer(9005)
  val config: Config = Config("", "", "", "test-table")

  override def beforeEach(): Unit = {
    dynamoServer.start()
  }

  override def afterEach(): Unit = {
    dynamoServer.resetAll()
    dynamoServer.stop()
  }

  val testUtils = new ExternalServicesTestUtils(dynamoServer)

  import testUtils._

  private val defaultDocxChecksum = "f7523c5d03a2c850fa06b5bbfed4c216f6368826"
  private val defaultJsonChecksum = "a8cfe9e6b5c10a26046c849cd3776734626e74a2"
  private val defaultDocxTitle = "TestTitle"
  private val defaultJsonName = "TEST-ID"

  private val nonMatchingChecksumValue = "non-matchingChecksum"
  private val nonMatchingDocxTitle = "non-matchingDocxTitle"
  private val nonMatchingJsonName = "non-matchingJsonName"

  private val docxFileIdInList = List("a25d33f3-7726-4fb3-8e6f-f66358451c4e")
  private val jsonFileIdInList = List("feedd76d-e368-45c8-96e3-c37671476793")

  val contentObjectApiVsDdbStates: TableFor6[String, String, String, String, List[String], String] = Table(
    (
      "DDB docxChecksum",
      "DDB jsonChecksum",
      "DDB docx title",
      "DDB json name",
      "ids that failed to match",
      "reason for failure"
    ),
    (
      defaultDocxChecksum,
      defaultJsonChecksum,
      defaultDocxTitle,
      nonMatchingJsonName,
      jsonFileIdInList,
      "json file name doesn't match"
    ),
    (
      defaultDocxChecksum,
      defaultJsonChecksum,
      nonMatchingDocxTitle,
      s"$defaultJsonName.json",
      docxFileIdInList,
      "docx file title doesn't match"
    ),
    (
      defaultDocxChecksum,
      defaultJsonChecksum,
      nonMatchingDocxTitle,
      nonMatchingJsonName,
      docxFileIdInList ++ jsonFileIdInList,
      "docx file title & json file name doesn't match"
    ),
    (
      defaultDocxChecksum,
      nonMatchingChecksumValue,
      defaultDocxTitle,
      s"$defaultJsonName.json",
      jsonFileIdInList,
      "json checksum doesn't match"
    ),
    (
      defaultDocxChecksum,
      nonMatchingChecksumValue,
      defaultDocxTitle,
      nonMatchingJsonName,
      jsonFileIdInList,
      "json checksum & json file name don't match"
    ),
    (
      defaultDocxChecksum,
      nonMatchingChecksumValue,
      nonMatchingDocxTitle,
      s"$defaultJsonName.json",
      docxFileIdInList ++ jsonFileIdInList,
      "json checksum & docx file title don't match"
    ),
    (
      defaultDocxChecksum,
      nonMatchingChecksumValue,
      nonMatchingDocxTitle,
      nonMatchingJsonName,
      docxFileIdInList ++ jsonFileIdInList,
      "json checksum, docx file title & json file name don't match"
    ),
    (
      nonMatchingChecksumValue,
      defaultJsonChecksum,
      defaultDocxTitle,
      s"$defaultJsonName.json",
      docxFileIdInList,
      "docx checksum doesn't match"
    ),
    (
      nonMatchingChecksumValue,
      defaultJsonChecksum,
      defaultDocxTitle,
      nonMatchingJsonName,
      docxFileIdInList ++ jsonFileIdInList,
      "docx checksum & json file name don't match"
    ),
    (
      nonMatchingChecksumValue,
      defaultJsonChecksum,
      nonMatchingDocxTitle,
      s"$defaultJsonName.json",
      docxFileIdInList,
      "docx checksum & docx file title don't match"
    ),
    (
      nonMatchingChecksumValue,
      defaultJsonChecksum,
      nonMatchingDocxTitle,
      nonMatchingJsonName,
      docxFileIdInList ++ jsonFileIdInList,
      "docx checksum, docx file title & json file name don't match"
    ),
    (
      nonMatchingChecksumValue,
      nonMatchingChecksumValue,
      defaultDocxTitle,
      s"$defaultJsonName.json",
      docxFileIdInList ++ jsonFileIdInList,
      "docx checksum & json checksum don't match"
    ),
    (
      nonMatchingChecksumValue,
      nonMatchingChecksumValue,
      defaultDocxTitle,
      nonMatchingJsonName,
      docxFileIdInList ++ jsonFileIdInList,
      "docx checksum, json checksum & json file name don't match"
    ),
    (
      nonMatchingChecksumValue,
      nonMatchingChecksumValue,
      nonMatchingDocxTitle,
      s"$defaultJsonName.json",
      docxFileIdInList ++ jsonFileIdInList,
      "docx checksum, json checksum & docx file title don't match"
    ),
    (
      nonMatchingChecksumValue,
      nonMatchingChecksumValue,
      nonMatchingDocxTitle,
      nonMatchingJsonName,
      docxFileIdInList ++ jsonFileIdInList,
      "docx checksum, json checksum, docx file title & json file name don't match"
    )
  )

  val uncommonButAcceptableFileExtensionStates: TableFor3[String, String, String] = Table(
    ("Child of Asset DocxTitle", "Entity Title", "State of the title's file extension"),
    ("", s"$docxTitle.docx", "Asset child file title is empty"),
    ("fileNameWithNoExtension", "fileNameWithNoExtension", "Asset child file title has no extension"),
    ("file.name.with.dots.but.no_real_extension", "file.name.with.dots.but.no_real_extension", "Asset child file title has dots but no extension"),
    ("file.name.with.dots.and.extension.docx", "file.name.with.dots.and.extension", "Asset child's file title has an extension but the entity title doesn't"),
    ("file.name.with.dots.and.extension", "file.name.with.dots.and.extension.docx", "Entity title has an extension but the Asset child's file title doesn't")
  )

  val uncommonButUnacceptableFileExtensionStates: TableFor4[String, String, List[String], String] = Table(
    ("Child of Asset DocxTitle", "Entity Title", "ids that failed to match", "State of the title's file extension"),
    (
      "file.name.with.dots.and.ext.docx",
      "file.name.with..more...dots.than.expected.and.ext.docx",
      docxFileIdInList,
      "Asset child's file title has fewer dots in it than the Entity title"
    ),
    (
      "file.name.with..more...dots.than.expected.and.ext.docx",
      "file.name.with.dots.and.ext.docx",
      docxFileIdInList,
      "Asset child's file title has more dots in it than the Entity title"
    )
  )

  "handler" should "return an error if the asset is not found in Dynamo" in {
    stubGetRequest(emptyDynamoGetResponse)
    val argumentVerifier = ArgumentVerifier()
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"No asset found for $assetId from $batchId")

    argumentVerifier.verifyInvocationsAndArgumentsPassed(0, 0, 0, 0, 1, 0)
  }

  "handler" should "return an error if the Dynamo entry does not have a type of 'Asset'" in {
    stubGetRequest(dynamoGetResponse().replace(""""S": "Asset"""", """"S": "ArchiveFolder""""))
    stubPostRequest(emptyDynamoPostResponse)
    val argumentVerifier = ArgumentVerifier()
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"Object $assetId is of type ArchiveFolder and not 'Asset'")

    argumentVerifier.verifyInvocationsAndArgumentsPassed(0, 0, 0, 0, 1, 0)
  }

  "handler" should "return an error if there were no entities that had the asset name as the SourceID" in {
    stubGetRequest(dynamoGetResponse())
    stubPostRequest(dynamoPostResponse)
    val argumentVerifier = ArgumentVerifier(entitiesWithIdentifier = IO.pure(Nil))
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"No entity found using SourceID '68b1c80b-36b8-4f0f-94d6-92589002d87e'")

    argumentVerifier.verifyInvocationsAndArgumentsPassed(1, 0, 0, 0, 1, 0)
  }

  "handler" should "return an error if more than one entity has the same asset name as its SourceId" in {
    stubGetRequest(dynamoGetResponse())
    stubPostRequest(dynamoPostResponse)
    val argumentVerifier = ArgumentVerifier(entitiesWithIdentifier = IO.pure(twoEntitiesWithSameDetails))

    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"More than one entity found using SourceID '68b1c80b-36b8-4f0f-94d6-92589002d87e'")
    argumentVerifier.verifyInvocationsAndArgumentsPassed(1, 0, 0, 0, 1, 0)
  }

  "handler" should "return an error if no children are found for the asset" in {
    stubGetRequest(dynamoGetResponse(0))
    stubPostRequest(emptyDynamoPostResponse)
    val argumentVerifier = ArgumentVerifier()
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"No children were found for $assetId from $batchId")

    argumentVerifier.verifyInvocationsAndArgumentsPassed(
      numOfGetBitstreamInfoRequests = 0,
      numOfGetUrlsToIoRepresentationsRequests = 0,
      numOfGetContentObjectsFromRepresentationRequests = 0
    )
  }

  "handler" should "return a 'wasReconciled' value of 'false' and a 'No entity found' 'reason' if there were no Content Objects belonging to the asset" in {
    stubGetRequest(dynamoGetResponse())
    stubPostRequest(dynamoPostResponse)

    val argumentVerifier = ArgumentVerifier(contentObjectsFromReps = IO.pure(Nil))
    val stateOutput = new Lambda().handler(input, config, dependencies).unsafeRunSync()

    stateOutput.wasReconciled should equal(false)
    stateOutput.reason should equal(
      "There were no Content Objects returned for entity ref '354f47cf-3ca2-4a4e-8181-81b714334f00'"
    )

    argumentVerifier.verifyInvocationsAndArgumentsPassed(numOfGetBitstreamInfoRequests = 0)
  }

  forAll(contentObjectApiVsDdbStates) { (docxChecksum, jsonChecksum, docxTitle, jsonName, idsThatFailed, reasonForFailure) =>
    "handler" should s"return a 'wasReconciled' value of 'false' and a 'reason' message that contains " +
      s"these ids: $idsThatFailed if $reasonForFailure " in {
        val representationTypeMap = Map(childIdDocx.toString -> "Access", childIdJson.toString -> "Preservation")
        val updatedDynamoPostResponse = dynamoPostResponse
          .replace(s""""S": "$defaultDocxChecksum"""", s""""S": "$docxChecksum"""")
          .replace(s""""S": "$defaultJsonChecksum"""", s""""S": "$jsonChecksum"""")
          .replace(s""""S": "$defaultDocxTitle"""", s""""S": "$docxTitle"""")
          .replace(s""""S": "$defaultDocxTitle.docx"""", s""""S": "$docxTitle.docx"""")
          .replace(s""""S": "$defaultJsonName.json"""", s""""S": "$jsonName"""")

        stubGetRequest(dynamoGetResponse())
        stubPostRequest(updatedDynamoPostResponse)

        val argumentVerifier = ArgumentVerifier()

        val stateOutput = new Lambda().handler(input, config, dependencies).unsafeRunSync()

        val expectedReason = idsThatFailed
          .map { failedId =>
            s":alert-noflash-slow: Reconciliation Failure - Out of the *1* files expected to be ingested for `assetId` " +
              s"'*68b1c80b-36b8-4f0f-94d6-92589002d87e*' with `representationType` *${representationTypeMap(failedId)}*, " +
              s"a _*checksum*_ and _*title*_ could not be matched with a file on Preservica for:\n1. $failedId"
          }
          .sorted
          .mkString("\n")
          .trim

        stateOutput.wasReconciled should equal(false)
        stateOutput.reason should equal(expectedReason)

        argumentVerifier.verifyInvocationsAndArgumentsPassed()
      }
  }

  forAll(uncommonButUnacceptableFileExtensionStates) { (childOfAssetDocxTitle, entityTitle, idsThatFailed, reasonForFailure) =>
    "handler" should s"return a 'wasReconciled' value of 'false' and a 'reason' message that contains " +
      s"these ids: $idsThatFailed if $reasonForFailure " in {
        val updatedDynamoPostResponse = dynamoPostResponse
          .replace(s""""S": "$defaultDocxTitle"""", s""""S": "$childOfAssetDocxTitle"""")

        stubGetRequest(dynamoGetResponse())
        stubPostRequest(updatedDynamoPostResponse)

        val bitstreamWithUpdatedTitle =
          Seq(IO.pure(Seq(defaultDocxBitStreamInfo.copy(potentialCoTitle = Some(entityTitle)))), IO.pure(Seq(defaultJsonBitStreamInfo)))

        val argumentVerifier = ArgumentVerifier(bitstreamInfo = bitstreamWithUpdatedTitle)

        val stateOutput = new Lambda().handler(input, config, dependencies).unsafeRunSync()

        val expectedReason = idsThatFailed
          .map { failedId =>
            s":alert-noflash-slow: Reconciliation Failure - Out of the *1* files expected to be ingested for `assetId` " +
              "'*68b1c80b-36b8-4f0f-94d6-92589002d87e*' with `representationType` *Access*, " +
              s"a _*checksum*_ and _*title*_ could not be matched with a file on Preservica for:\n1. $failedId"
          }
          .sorted
          .mkString("\n")
          .trim

        stateOutput.wasReconciled should equal(false)
        stateOutput.reason should equal(expectedReason)

        argumentVerifier.verifyInvocationsAndArgumentsPassed()
      }
  }

  "handler" should "return an error if the child count and the number of children don't match" in {
    stubGetRequest(dynamoGetResponse(3))
    stubPostRequest(dynamoPostResponse)
    ArgumentVerifier()
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"Asset id $assetId: has 3 children in the files table but found 2 children in the Preservation system")
  }

  "handler" should "return a 'wasReconciled' value of 'true' and an empty 'reason' if COs could be reconciled" in {
    stubGetRequest(dynamoGetResponse())
    stubPostRequest(dynamoPostResponse)
    stubLockTableGetRequest(dynamoLockTableGetResponse)

    val argumentVerifier = ArgumentVerifier()

    val stateOutput = new Lambda().handler(input, config, dependencies).unsafeRunSync()

    stateOutput.reason should equal("")
    stateOutput.wasReconciled should equal(true)
    stateOutput.assetId should equal(assetId)

    argumentVerifier.verifyInvocationsAndArgumentsPassed(numOfFileTableGetRequests = 1, numOfFileTableUpdateRequests = 1)
  }

  forAll(uncommonButAcceptableFileExtensionStates) { (childOfAssetDocxTitle, entityTitle, stateOfTitleExtension) =>
    "handler" should s"return a 'wasReconciled' value of 'true' and an empty 'reason' if $stateOfTitleExtension" in {
      val updatedDynamoPostResponse = dynamoPostResponse
        .replace(s""""S": "$defaultDocxTitle"""", s""""S": "$childOfAssetDocxTitle"""")

      stubGetRequest(dynamoGetResponse())
      stubPostRequest(updatedDynamoPostResponse)
      stubLockTableGetRequest(dynamoLockTableGetResponse)

      val bitstreamWithUpdatedTitle =
        Seq(IO.pure(Seq(defaultDocxBitStreamInfo.copy(potentialCoTitle = Some(entityTitle)))), IO.pure(Seq(defaultJsonBitStreamInfo)))

      val argumentVerifier = ArgumentVerifier(bitstreamInfo = bitstreamWithUpdatedTitle)

      val stateOutput = new Lambda().handler(input, config, dependencies).unsafeRunSync()

      stateOutput.reason should equal("")
      stateOutput.wasReconciled should equal(true)
      stateOutput.assetId should equal(assetId)

      argumentVerifier.verifyInvocationsAndArgumentsPassed(numOfFileTableGetRequests = 1, numOfFileTableUpdateRequests = 1)
    }
  }

  "handler" should "return a 'wasReconciled' value of 'true' and an empty 'reason' if COs could be reconciled, " +
    "even if one of the Asset's child's title, was not present in the table" in {
      stubGetRequest(dynamoGetResponse())

      val updatedDynamoPostResponse = dynamoPostResponse.replace(
        """      "title": {
        |        "S": ""
        |      },
        |""".stripMargin,
        ""
      )

      stubPostRequest(updatedDynamoPostResponse)
      stubLockTableGetRequest(dynamoLockTableGetResponse)

      val argumentVerifier = ArgumentVerifier()

      val stateOutput = new Lambda().handler(input, config, dependencies).unsafeRunSync()

      stateOutput.reason should equal("")
      stateOutput.wasReconciled should equal(true)
      stateOutput.assetId should equal(assetId)

      argumentVerifier.verifyInvocationsAndArgumentsPassed(numOfFileTableGetRequests = 1, numOfFileTableUpdateRequests = 1)
    }
}
