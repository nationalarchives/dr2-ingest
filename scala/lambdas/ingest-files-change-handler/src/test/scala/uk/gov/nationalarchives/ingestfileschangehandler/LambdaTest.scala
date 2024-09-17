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
import uk.gov.nationalarchives.ingestfileschangehandler.Lambda.MessageType.*
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

  def outputBuilder(asset: DynamoRow, messageType: MessageType): OutputMessage =
    OutputMessage(OutputProperties(asset.batchId, messageId, Option("correlationId"), instant, messageType), OutputParameters(asset.id))

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
      List(outputBuilder(assetA, IngestUpdate))
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
      List(outputBuilder(assetA, IngestComplete))
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
      List(outputBuilder(assetB, IngestUpdate))
    ),
    (
      "ingested_PS and ingested_CC for 1 Asset only where all files are complete, sends complete message",
      List(
        folderA,
        assetA.copy(ingestedPreservica = true, ingestedCustodialCopy = true),
        fileAOne.copy(ingestedCustodialCopy = true),
        fileATwo.copy(ingestedCustodialCopy = true)
      ),
      assetA.copy(ingestedPreservica = true, ingestedCustodialCopy = true),
      List(outputBuilder(assetA, IngestComplete))
    ),
    (
      "ingested_PS and ingested_CC for 1 Asset only where all files are not complete, sends no message",
      List(
        folderA,
        assetA.copy(ingestedPreservica = true, ingestedCustodialCopy = true),
        fileAOne,
        fileATwo.copy(ingestedCustodialCopy = true)
      ),
      assetA.copy(ingestedPreservica = true, ingestedCustodialCopy = true),
      Nil
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
      Nil
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
      List(outputBuilder(assetA, IngestComplete))
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
      List(outputBuilder(assetA, IngestComplete), outputBuilder(assetB, IngestComplete))
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
      List(outputBuilder(assetA, IngestComplete), outputBuilder(assetB, IngestComplete))
    )
  )

  val errorsTable: TableFor4[String, List[DynamoRow], DynamoRow, String] = Table(
    ("title", "rowsInTable", "newRowInput", "expectedErrorMessage"),
    (
      "Multiple assets found for file parent",
      List(
        folderA,
        assetA,
        assetA,
        fileAOne,
        fileATwo
      ),
      fileAOne,
      s"Expected 1 parent asset, found 2 assets for file ${assetA.id}"
    )
  )

  forAll(handlerOutputsTable) { (title, rowsInTable, newRowInput, expectedOutput) =>
    "handler" should s"given a new image with $title" in {
      val dynamoRow = if newRowInput.rowType == File then newRowInput.createFile() else newRowInput.createAsset()
      val event = DynamodbEvent(List(DynamodbStreamRecord(EventName.MODIFY, StreamRecord(newRowInput.getPrimaryKey.some, dynamoRow.some))))
      val messages = (for {
        rowsRef <- Ref[IO].of(rowsInTable)
        messagesRef <- Ref[IO].of[List[OutputMessage]](Nil)
        _ <- new Lambda().handler(event, Config("", "", ""), Dependencies(createDynamoClient(rowsRef), createSnsClient(messagesRef), () => instant, () => messageId))
        messages <- messagesRef.get
      } yield messages).unsafeRunSync()

      messages.sortBy(_.properties.`type`.toString) should equal(expectedOutput.sortBy(_.properties.`type`.toString))
    }
  }

  forAll(errorsTable) { (title, rowsInTable, newRowInput, expectedErrorMessage) =>
    "handler" should s"return $expectedErrorMessage if $title" in {
      val dynamoRow = if newRowInput.rowType == File then newRowInput.createFile() else newRowInput.createAsset()
      val event = DynamodbEvent(List(DynamodbStreamRecord(EventName.MODIFY, StreamRecord(newRowInput.getPrimaryKey.some, dynamoRow.some))))
      val errorMessage = (for {
        rowsRef <- Ref[IO].of(rowsInTable)
        messagesRef <- Ref[IO].of[List[OutputMessage]](Nil)
        _ <- new Lambda().handler(event, Config("", "", ""), Dependencies(createDynamoClient(rowsRef), createSnsClient(messagesRef), () => instant, () => messageId))
        _ <- rowsRef.get
      } yield ()).attempt.unsafeRunSync().left.value.getMessage

      errorMessage should equal(expectedErrorMessage)
    }
  }
}
