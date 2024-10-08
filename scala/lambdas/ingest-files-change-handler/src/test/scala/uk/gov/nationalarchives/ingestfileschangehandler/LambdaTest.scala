package uk.gov.nationalarchives.ingestfileschangehandler

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.implicits.*
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor4}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.ingestfileschangehandler.Lambda.*
import uk.gov.nationalarchives.utils.ExternalUtils.{MessageType, MessageStatus, OutputMessage, OutputProperties, OutputParameters}
import uk.gov.nationalarchives.utils.ExternalUtils.MessageType.*
import uk.gov.nationalarchives.utils.ExternalUtils.MessageStatus.*
import uk.gov.nationalarchives.ingestfileschangehandler.Utils.*

import java.time.Instant
import java.util.UUID

class LambdaTest extends AnyFlatSpec with TableDrivenPropertyChecks with EitherValues {

  val instant: Instant = Instant.now
  val messageId: UUID = UUID.randomUUID
  val folderA: DynamoRow = DynamoRow(UUID.randomUUID, "A", ArchiveFolder, None)
  val assetA: DynamoRow = DynamoRow(UUID.randomUUID, "A", Asset, Option(s"${folderA.id}"), true)
  val fileAOne: DynamoRow = DynamoRow(UUID.randomUUID, "A", File, Option(s"${folderA.id}/${assetA.id}"))
  val fileATwo: DynamoRow = DynamoRow(UUID.randomUUID, "A", File, Option(s"${folderA.id}/${assetA.id}"))
  val folderB: DynamoRow = DynamoRow(UUID.randomUUID, "B", ArchiveFolder, None)
  val assetB: DynamoRow = DynamoRow(assetA.id, "B", Asset, Option(s"${folderB.id}"), true)
  val fileBOne: DynamoRow = DynamoRow(UUID.randomUUID, "B", File, Option(s"${folderB.id}/${assetB.id}"))
  val fileBTwo: DynamoRow = DynamoRow(UUID.randomUUID, "B", File, Option(s"${folderB.id}/${assetB.id}"))

  def outputBuilder(asset: DynamoRow, messageType: MessageType, messageStatus: MessageStatus): OutputMessage =
    OutputMessage(OutputProperties(asset.batchId, messageId, Option("correlationId"), instant, messageType), OutputParameters(asset.id, messageStatus))

  private def runLambda(rowsInTable: List[DynamoRow], event: DynamodbEvent): IO[List[OutputMessage]] = {
    for {
      rowsRef <- Ref[IO].of(rowsInTable)
      messagesRef <- Ref[IO].of[List[OutputMessage]](Nil)
      _ <- new Lambda().handler(event, Config("", "", ""), Dependencies(createDynamoClient(rowsRef), createSnsClient(messagesRef), () => instant, () => messageId))
      messages <- messagesRef.get
    } yield messages
  }

  val handlerOutputsTable: TableFor4[String, List[DynamoRow], DynamoRow, List[OutputMessage]] = Table(
    ("title", "rowsInTable", "newRowInput", "expectedOutput"),
    (
      "ingested_PS for 1 item, only sends an update message",
      List(
        folderA,
        assetA,
        fileAOne,
        fileATwo
      ),
      assetA,
      List(outputBuilder(assetA, IngestUpdate, IngestedPreservation))
    ),
    (
      "skipIngest for 1 item, only sends no message",
      List(
        folderA,
        assetA.copy(skipIngest = true, ingestedPreservica = false),
        fileAOne,
        fileATwo
      ),
      assetA.copy(skipIngest = true, ingestedPreservica = false),
      Nil
    ),
    (
      "ingested_PS and skipIngest for 1 item, only sends ingest complete message",
      List(
        folderA,
        assetA.copy(skipIngest = true, ingestedPreservica = true),
        fileAOne,
        fileATwo
      ),
      assetA.copy(skipIngest = true, ingestedPreservica = true),
      List(outputBuilder(assetA, IngestComplete, IngestedCCDisk))
    ),
    (
      "ingested_PS and skipIngest for multiple items, sends update message",
      List(
        folderA,
        assetA.copy(ingestedPreservica = true),
        fileAOne,
        fileATwo,
        folderB,
        assetB.copy(skipIngest = true, ingestedPreservica = true),
        fileBOne,
        fileBTwo
      ),
      assetB.copy(skipIngest = true, ingestedPreservica = true),
      List(outputBuilder(assetB, IngestUpdate, IngestedPreservation))
    ),
    (
      "ingested_PS and ingested_CC for 1 Asset only where all files are complete, sends complete and update message",
      List(
        folderA,
        assetA.copy(ingestedPreservica = true, ingestedCustodialCopy = true),
        fileAOne.copy(ingestedCustodialCopy = true),
        fileATwo.copy(ingestedCustodialCopy = true)
      ),
      assetA.copy(ingestedPreservica = true, ingestedCustodialCopy = true),
      List(outputBuilder(assetA, IngestUpdate, IngestedPreservation), outputBuilder(assetA, IngestComplete, IngestedCCDisk))
    ),
    (
      "ingested_PS and ingested_CC for 1 Asset only where all files are not complete, sends an ingest update message",
      List(
        folderA,
        assetA.copy(ingestedPreservica = true, ingestedCustodialCopy = true),
        fileAOne,
        fileATwo.copy(ingestedCustodialCopy = true)
      ),
      assetA.copy(ingestedPreservica = true, ingestedCustodialCopy = true),
      List(outputBuilder(assetA, IngestUpdate, IngestedPreservation))
    ),
    (
      "ingested_CC for 1 File only where not all files are complete, sends no message",
      List(
        folderA,
        assetA.copy(ingestedPreservica = true, ingestedCustodialCopy = true),
        fileAOne,
        fileATwo.copy(ingestedCustodialCopy = true)
      ),
      fileATwo.copy(ingestedCustodialCopy = true),
      List(outputBuilder(assetA, IngestUpdate, IngestedPreservation))
    ),
    (
      "ingested_CC for 1 File only where all files are complete, sends complete message",
      List(
        folderA,
        assetA.copy(ingestedPreservica = true, ingestedCustodialCopy = true),
        fileAOne.copy(ingestedCustodialCopy = true),
        fileATwo.copy(ingestedCustodialCopy = true)
      ),
      fileATwo.copy(ingestedCustodialCopy = true),
      List(outputBuilder(assetA, IngestUpdate, IngestedPreservation), outputBuilder(assetA, IngestComplete, IngestedCCDisk))
    ),
    (
      "ingested_PS and ingested_CC for an Asset where all files are complete, and the asset has been in multiple ingest batches, sends two complete messages",
      List(
        folderA,
        assetA.copy(ingestedPreservica = true, ingestedCustodialCopy = true),
        fileAOne.copy(ingestedCustodialCopy = true),
        fileATwo.copy(ingestedCustodialCopy = true),
        folderB,
        assetB.copy(skipIngest = true, ingestedPreservica = true, ingestedCustodialCopy = true),
        fileBOne,
        fileBTwo
      ),
      assetA.copy(ingestedPreservica = true, ingestedCustodialCopy = true),
      List(
        outputBuilder(assetA, IngestUpdate, IngestedPreservation),
        outputBuilder(assetA, IngestComplete, IngestedCCDisk),
        outputBuilder(assetB, IngestComplete, IngestedCCDisk)
      )
    ),
    (
      "ingested_CC for a File where all files are complete, and the asset has been in multiple ingest batches, sends two complete messages",
      List(
        folderA,
        assetA.copy(ingestedPreservica = true, ingestedCustodialCopy = true),
        fileAOne.copy(ingestedCustodialCopy = true),
        fileATwo.copy(ingestedCustodialCopy = true),
        folderB,
        assetB.copy(skipIngest = true, ingestedPreservica = true),
        fileBOne,
        fileBTwo
      ),
      fileAOne.copy(ingestedCustodialCopy = true),
      List(
        outputBuilder(assetA, IngestUpdate, IngestedPreservation),
        outputBuilder(assetA, IngestComplete, IngestedCCDisk),
        outputBuilder(assetB, IngestComplete, IngestedCCDisk)
      )
    )
  )

  val errorsTable: TableFor4[String, List[DynamoRow], DynamoRow, String] = Table(
    ("title", "rowsInTable", "newRowInput", "expectedErrorMessage"),
    (
      "Parent path missing from file",
      List(
        folderA
      ),
      fileAOne.copy(parentPath = None),
      s"Cannot find a direct parent for file ${fileAOne.id}"
    )
  )

  forAll(handlerOutputsTable) { (title, rowsInTable, newRowInput, expectedOutput) =>
    "handler" should s"given a new image with $title" in {
      val dynamoRow = if newRowInput.rowType == File then newRowInput.createFile() else newRowInput.createAsset()
      val event = DynamodbEvent(List(DynamodbStreamRecord(EventName.MODIFY, StreamRecord(newRowInput.getPrimaryKey.some, dynamoRow.some))))
      val messages = runLambda(rowsInTable, event).unsafeRunSync()

      messages.sortBy(_.properties.messageType.toString) should equal(expectedOutput.sortBy(_.properties.messageType.toString))
    }
  }

  forAll(errorsTable) { (title, rowsInTable, newRowInput, expectedErrorMessage) =>
    "handler" should s"return $expectedErrorMessage if $title" in {
      val dynamoRow = if newRowInput.rowType == File then newRowInput.createFile() else newRowInput.createAsset()
      val event = DynamodbEvent(List(DynamodbStreamRecord(EventName.MODIFY, StreamRecord(newRowInput.getPrimaryKey.some, dynamoRow.some))))
      val errorMessage = runLambda(rowsInTable, event).attempt.unsafeRunSync().left.value.getMessage

      errorMessage should equal(expectedErrorMessage)
    }
  }

  "handler" should "send an ingest started message if the event name is insert and the type is an asset" in {
    val dynamoRow = DynamoRow(UUID.randomUUID, "batchId", Asset, None).createAsset()
    val event = DynamodbEvent(List(DynamodbStreamRecord(EventName.INSERT, StreamRecord(assetA.getPrimaryKey.some, dynamoRow.some))))
    val messages = runLambda(List(assetA), event).unsafeRunSync()

    messages.size should equal(1)

    val message = messages.head
    val properties = message.properties
    val parameters = message.parameters

    properties.executionId should equal("batchId")
    properties.messageId should equal(messageId)
    properties.parentMessageId should equal(Some(correlationId))
    properties.timestamp should equal(instant)
    properties.messageType should equal(IngestUpdate)

    parameters.assetId should equal(dynamoRow.id)
    parameters.status should equal(IngestStarted)
  }

  "handler" should "not send an ingest started message if the event name is insert and the type is not an asset" in {
    val dynamoRow = DynamoRow(UUID.randomUUID, "batchId", File, None).createFile()
    val event = DynamodbEvent(List(DynamodbStreamRecord(EventName.INSERT, StreamRecord(fileAOne.getPrimaryKey.some, dynamoRow.some))))
    val messages = runLambda(List(assetA), event).unsafeRunSync()

    messages.size should equal(0)
  }

  "handler" should "not send an ingest started message if the event name is remove" in {
    val dynamoRow = DynamoRow(UUID.randomUUID, "batchId", Asset, None).createAsset()
    val event = DynamodbEvent(List(DynamodbStreamRecord(EventName.REMOVE, StreamRecord(assetA.getPrimaryKey.some, dynamoRow.some))))
    val messages = runLambda(List(assetA), event).unsafeRunSync()

    messages.size should equal(0)
  }
}
