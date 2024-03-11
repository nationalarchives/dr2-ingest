package uk.gov.nationalarchives

import cats.effect.IO
import com.github.tomakehurst.wiremock.WireMockServer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor6}
import uk.gov.nationalarchives.Lambda.StateOutput
import uk.gov.nationalarchives.testUtils.ExternalServicesTestUtils
import upickle.default
import upickle.default.{macroR, read}

class LambdaTest extends AnyFlatSpec with BeforeAndAfterEach with TableDrivenPropertyChecks {
  implicit val stateDataReader: default.Reader[StateOutput] = macroR[StateOutput]
  val dynamoServer = new WireMockServer(9005)

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

  "handleRequest" should "return an error if the asset is not found in Dynamo" in {
    stubGetRequest(emptyDynamoGetResponse)
    val testLambda = TestLambda()
    val ex = intercept[Exception] {
      testLambda.handleRequest(standardInput, outputStream, null)
    }
    ex.getMessage should equal(s"No asset found for $assetId from $batchId")

    testLambda.verifyInvocationsAndArgumentsPassed(0, 0, 0, 0)
  }

  "handleRequest" should "return an error if the Dynamo entry does not have a type of 'Asset'" in {
    stubGetRequest(dynamoGetResponse.replace(""""S": "Asset"""", """"S": "ArchiveFolder""""))
    stubPostRequest(emptyDynamoPostResponse)
    val testLambda = TestLambda()
    val ex = intercept[Exception] {
      testLambda.handleRequest(standardInput, outputStream, null)
    }
    ex.getMessage should equal(s"Object $assetId is of type ArchiveFolder and not 'Asset'")

    testLambda.verifyInvocationsAndArgumentsPassed(0, 0, 0, 0)
  }

  "handleRequest" should "return an error if there were no entities that had the asset name as the SourceId" in {
    stubGetRequest(dynamoGetResponse)
    stubPostRequest(dynamoPostResponse)
    val testLambda = TestLambda(entitiesWithIdentifier = IO.pure(Nil))
    val ex = intercept[Exception] {
      testLambda.handleRequest(standardInput, outputStream, null)
    }
    ex.getMessage should equal(s"No entity found using SourceId 'Test Name'")

    testLambda.verifyInvocationsAndArgumentsPassed(1, 0, 0, 0)
  }

  "handleRequest" should "return an error if no children are found for the asset" in {
    stubGetRequest(dynamoGetResponse)
    stubPostRequest(emptyDynamoPostResponse)
    val testLambda = TestLambda()
    val ex = intercept[Exception] {
      testLambda.handleRequest(standardInput, outputStream, null)
    }
    ex.getMessage should equal(s"No children were found for $assetId from $batchId")

    testLambda.verifyInvocationsAndArgumentsPassed(numOfGetBitstreamInfoRequests = 0)
  }

  "handleRequest" should "return a 'wasReconciled' value of 'false' and a 'No entity found' 'reason' if there were no Content Objects belonging to the asset" in {
    stubGetRequest(dynamoGetResponse)
    stubPostRequest(dynamoPostResponse)

    val outStream = outputStream

    val testLambda = TestLambda(contentObjectsFromReps = IO.pure(Nil))
    testLambda.handleRequest(standardInput, outStream, null)

    val stateOutput = read[StateOutput](outStream.toByteArray.map(_.toChar).mkString)

    stateOutput.wasReconciled should equal(false)
    stateOutput.reason should equal(
      "There were no Content Objects returned for entity ref '354f47cf-3ca2-4a4e-8181-81b714334f00'"
    )

    testLambda.verifyInvocationsAndArgumentsPassed(numOfGetBitstreamInfoRequests = 0)
  }

  forAll(contentObjectApiVsDdbStates) {
    (docxChecksum, jsonChecksum, docxTitle, jsonName, idsThatFailed, reasonForFailure) =>
      "handleRequest" should s"return a 'wasReconciled' value of 'false' and a 'reason' message that contains " +
        s"these ids: $idsThatFailed if $reasonForFailure " in {
          val outStream = outputStream
          val updatedDynamoPostResponse = dynamoPostResponse
            .replace(s""""S": "$defaultDocxChecksum"""", s""""S": "$docxChecksum"""")
            .replace(s""""S": "$defaultJsonChecksum"""", s""""S": "$jsonChecksum"""")
            .replace(s""""S": "$defaultDocxTitle"""", s""""S": "$docxTitle"""")
            .replace(s""""S": "$defaultDocxTitle.docx"""", s""""S": "$docxTitle.docx"""")
            .replace(s""""S": "$defaultJsonName.json"""", s""""S": "$jsonName"""")

          stubGetRequest(dynamoGetResponse)
          stubPostRequest(updatedDynamoPostResponse)

          val testLambda = TestLambda()

          testLambda.handleRequest(standardInput, outStream, null)

          val stateOutput = read[StateOutput](outStream.toByteArray.map(_.toChar).mkString)

          stateOutput.wasReconciled should equal(false)
          stateOutput.reason should equal(
            s"Out of the 2 files expected to be ingested for assetId '68b1c80b-36b8-4f0f-94d6-92589002d87e', " +
              s"a checksum could not be found for: ${idsThatFailed.mkString(", ")}"
          )

          testLambda.verifyInvocationsAndArgumentsPassed()
        }
  }

  "handleRequest" should "return a 'wasReconciled' value of 'true' and an empty 'reason' if COs could be reconciled" in {
    stubGetRequest(dynamoGetResponse)
    stubPostRequest(dynamoPostResponse)
    val outStream = outputStream

    val testLambda = TestLambda()

    testLambda.handleRequest(standardInput, outStream, null)

    val stateOutput = read[StateOutput](outStream.toByteArray.map(_.toChar).mkString)

    stateOutput.wasReconciled should equal(true)
    stateOutput.reason should equal("")

    testLambda.verifyInvocationsAndArgumentsPassed()
  }

  "handleRequest" should "return a 'wasReconciled' value of 'true' and an empty 'reason' if COs could be reconciled, " +
    "even if one of the Asset's child's title, was not present in the table" in {
      stubGetRequest(dynamoGetResponse)

      val updatedDynamoPostResponse = dynamoPostResponse.replace(
        """      "title": {
        |        "S": ""
        |      },
        |""".stripMargin,
        ""
      )
      println(updatedDynamoPostResponse)
      stubPostRequest(updatedDynamoPostResponse)
      val outStream = outputStream

      val testLambda = TestLambda()

      testLambda.handleRequest(standardInput, outStream, null)

      val stateOutput = read[StateOutput](outStream.toByteArray.map(_.toChar).mkString)

      stateOutput.wasReconciled should equal(true)
      stateOutput.reason should equal("")

      testLambda.verifyInvocationsAndArgumentsPassed()
    }
}
