package uk.gov.nationalarchives.e2etests

import cats.effect.{Async, Clock, Ref}
import cats.effect.std.AtomicCell
import cats.syntax.all.*
import fs2.Stream
import io.circe.Decoder
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.apache.commons.codec.digest.DigestUtils
import org.reactivestreams.FlowAdapters
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jFactory
import pureconfig.*
import pureconfig.generic.derivation.default.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbWriteItemRequest
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client, DASFNClient, DASQSClient}

import java.net.URI
import scala.concurrent.duration.*
import java.time.{Instant, LocalDate}
import java.util.UUID
import scala.reflect.ClassTag
import scala.util.Random

trait StepDefs[F[_]: Async] {

  given SelfAwareStructuredLogger[F] = Slf4jFactory.create[F].getLogger

  def createFiles(number: Int, emptyChecksum: Boolean = false, invalidMetadata: Boolean = false, invalidChecksum: Boolean = false)(using ref: Ref[F, List[UUID]]): F[Unit]

  def sendMessages(ref: Ref[F, List[UUID]]): F[Unit]

  def createBatch(ref: Ref[F, List[UUID]]): F[Unit]

  def waitForIngestCompleteMessages(ref: Ref[F, List[UUID]]): F[Unit]

  def waitForIngestErrorMessages(ref: Ref[F, List[UUID]]): F[Unit]

  def pollForTDRValidationMessages(ref: Ref[F, List[UUID]]): F[Unit]

}

object StepDefs {
  val config: Config = ConfigSource.default.loadOrThrow[Config]
  val ingestCompleteType = "preserve.digital.asset.ingest.complete"
  val ingestUpdateType = "preserve.digital.asset.ingest.update"
  val ccDiskStatus = "Asset has been written to custodial copy disk."
  val assetErrorStatus = "There has been an error ingesting the asset."

  def apply[F[_]: Async](messageUuidCell: AtomicCell[F, List[SqsMessage]]): StepDefs[F] = {
    val s3Client: DAS3Client[F] = DAS3Client[F]()
    val sqsClient: DASQSClient[F] = DASQSClient[F]()
    val dynamoClient: DADynamoDBClient[F] = DADynamoDBClient[F]()
    val sfnClient: DASFNClient[F] = DASFNClient[F]()
    val pollingDuration: FiniteDuration = 60.minutes
    StepDefs(messageUuidCell, s3Client, sqsClient, dynamoClient, sfnClient, pollingDuration)
  }

  def apply[F[_]: Async](
      messageUuidCell: AtomicCell[F, List[SqsMessage]],
      s3Client: DAS3Client[F],
      sqsClient: DASQSClient[F],
      dynamoClient: DADynamoDBClient[F],
      sfnClient: DASFNClient[F],
      pollingDuration: FiniteDuration
  ): StepDefs[F] = {
    new StepDefs[F] {

      private def toDynamoString(value: String): AttributeValue = AttributeValue.builder.s(value).build

      private val thisYear = LocalDate.now.getYear

      def createBatch(ref: Ref[F, List[UUID]]): F[Unit] = ref.get.flatMap { ids =>
        val groupId = s"E2E_${UUID.randomUUID}"
        val batchId = s"${groupId}_0"
        Logger[F].info(s"Creating batch $groupId with ${ids.size} ids") >>
          ids.traverse { id =>
            val input = AggregatorInputMessage(id, URI.create(s"s3://${config.inputBucket}/$id"))
            val writeItem = Map("assetId" -> toDynamoString(id.toString), "groupId" -> toDynamoString(groupId), "message" -> toDynamoString(input.asJson.noSpaces))
            dynamoClient.writeItem(DADynamoDbWriteItemRequest(config.lockTableName, writeItem, Some(s"attribute_not_exists(assetId)")))
          } >> sfnClient.startExecution(config.preingestSfnArn, SFNArguments(groupId, batchId, 0), batchId.some).void
      }

      def sendMessages(ref: Ref[F, List[UUID]]): F[Unit] = ref.get.flatMap { ids =>
        val send = sqsClient.sendMessage[SqsInputMessage](config.ingestSqsQueue)(_, None, 0)
        ids.traverse { id =>
          send(SqsInputMessage(id, config.inputBucket))
        }.void
      }

      def createFile(bucket: String, key: String, bytes: Array[Byte]): F[CompletedUpload] =
        Stream
          .emits[F, Byte](bytes)
          .chunks
          .map(_.toByteBuffer)
          .toPublisherResource
          .use { pub =>
            s3Client.upload(bucket, key, FlowAdapters.toPublisher(pub))
          }

      def makeReference(length: Int): String =
        Random.shuffle(('A' to 'Z') ++ ('0' to '9')).take(length).mkString

      def createMetadataJson(id: UUID, checksum: String, invalidMetadata: Boolean): Array[Byte] =
        def generateValue[T](ifPresent: T) = if invalidMetadata && Random.nextBoolean() then None else ifPresent.some
        def generateSeries = Random.shuffle(List(None, Some("TEST123"), Some(""))).head
        val series = if invalidMetadata then generateSeries else Some("TEST 123")
        TDRMetadata(
          series,
          generateValue(id),
          None,
          "TestBody",
          generateValue("2024-10-07 09:54:48"),
          s"E2E-$thisYear-${makeReference(4)}",
          s"$id.txt",
          checksum,
          s"Z${makeReference(5)}"
        ).asJson.noSpaces.getBytes

      def createFiles(number: Int, emptyChecksum: Boolean = false, invalidMetadata: Boolean = false, invalidChecksum: Boolean = false)(using ref: Ref[F, List[UUID]]): F[Unit] = {
        val invalidChecksumValue = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        val ids = List.fill(number)(UUID.randomUUID)
        for {
          _ <- Logger[F].info(s"$number ids created")
          createdFileIds <- ids.traverse { id =>
            for {
              checksum <- Async[F].pure(if emptyChecksum then "" else if invalidChecksum then invalidChecksumValue else DigestUtils.sha256Hex(id.toString))
              _ <- createFile(config.inputBucket, id.toString, id.toString.getBytes)
              _ <- createFile(config.inputBucket, s"$id.metadata", createMetadataJson(id, checksum, invalidMetadata))
            } yield id
          }
          _ <- updateGeneratedUuids(createdFileIds)
        } yield ()
      }

      def waitForIngestCompleteMessages(ref: Ref[F, List[UUID]]): F[Unit] =
        pollForIngestMessages(ingestCompleteType, ccDiskStatus, ref)

      def waitForIngestErrorMessages(ref: Ref[F, List[UUID]]): F[Unit] =
        pollForIngestMessages(ingestUpdateType, assetErrorStatus, ref)

      def filterByInputMessage(sqsInputMessage: SqsMessage, uuids: List[UUID]): Boolean =
        uuids.contains(sqsInputMessage.getId)

      def filterByMessageTypeStatus[T <: SqsMessage](messageType: String, messageStatus: String)(outputMessage: T, uuids: List[UUID]): Boolean =
        outputMessage match
          case outputMessage: NotificationsOutputMessage =>
            outputMessage.properties.messageType == messageType
            && outputMessage.parameters.status == messageStatus
            && uuids.contains(outputMessage.parameters.assetId)
          case _ => false

      def pollForIngestMessages(messageType: String, messageStatus: String, ref: Ref[F, List[UUID]]): F[Unit] =
        given String = config.notificationsSqsQueue

        Logger[F].info(s"Polling for $messageType messages") >>
          Clock[F].timeout(pollForMessages[NotificationsOutputMessage](filterByMessageTypeStatus(messageType, messageStatus), ref), pollingDuration)

      def pollForTDRValidationMessages(ref: Ref[F, List[UUID]]): F[Unit] =
        given String = config.copyFilesDlq

        Logger[F].info(s"Polling for TDR validation messages") >>
          Clock[F].timeout(pollForMessages[SqsInputMessage](filterByInputMessage, ref), pollingDuration)

      def updateGeneratedUuids(newIds: List[UUID])(using ref: Ref[F, List[UUID]]): F[Unit] = ref.update(_ ++ newIds)

      def pollForMessages[T <: SqsMessage](filter: (SqsMessage, List[UUID]) => Boolean, ref: Ref[F, List[UUID]])(using decoder: Decoder[T], queueUrl: String): F[Unit] = {
        for {
          uuids <- ref.get
          messages <- sqsClient.receiveMessages[T](queueUrl)
          newMessages <- messageUuidCell.updateAndGet(_ ++ messages.map(_.message))
          _ <- messages.traverse(msg => sqsClient.deleteMessage(queueUrl, msg.receiptHandle))
          newInputUuids <- ref.updateAndGet { uuids =>
            uuids.diff(newMessages.filter(msg => filter(msg, uuids)).map(_.getId))
          }
          _ <-
            if newInputUuids.isEmpty then Async[F].unit
            else if messages.isEmpty then Async[F].sleep(60.seconds) >> pollForMessages(filter, ref)
            else pollForMessages(filter, ref)
        } yield ()
      }
    }
  }

  case class Config(inputBucket: String, ingestSqsQueue: String, notificationsSqsQueue: String, lockTableName: String, preingestSfnArn: String, copyFilesDlq: String)
      derives ConfigReader

  case class TDRMetadata(
      Series: Option[String],
      UUID: Option[UUID],
      description: Option[String],
      TransferringBody: String,
      TransferInitiatedDatetime: Option[String],
      ConsignmentReference: String,
      Filename: String,
      SHA256ServerSideChecksum: String,
      FileReference: String
  )

  case class OutputProperties(executionId: String, messageId: UUID, parentMessageId: Option[String], timestamp: Instant, messageType: String)

  case class OutputParameters(assetId: UUID, status: String)

  sealed trait SqsMessage:
    def getId: UUID

  case class SqsInputMessage(fileId: UUID, bucket: String) extends SqsMessage {
    override def getId: UUID = fileId
  }

  case class NotificationsOutputMessage(properties: OutputProperties, parameters: OutputParameters) extends SqsMessage {
    def getId: UUID = parameters.assetId
  }

  case class AggregatorInputMessage(id: UUID, location: URI)

  case class SFNArguments(groupId: String, batchId: String, waitFor: Int, retryCount: Int = 0)

}
