package uk.gov.nationalarchives.postingeststatechangehandler

import cats.effect.{IO, Outcome}
import cats.syntax.all.*
import io.circe.*
import io.circe.Decoder.Result
import io.circe.generic.semiauto.deriveDecoder
import io.circe.jawn.decode
import org.scanamo.{DynamoArray, DynamoObject, DynamoReadError, DynamoValue}
import pureconfig.ConfigReader
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbRequest
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{*, given}
import uk.gov.nationalarchives.postingeststatechangehandler.Lambda.{*, given}
import uk.gov.nationalarchives.utils.EventCodecs.given
import uk.gov.nationalarchives.utils.ExternalUtils.MessageStatus.{IngestedCCDisk, IngestedPreservation}
import uk.gov.nationalarchives.utils.ExternalUtils.MessageType.{IngestComplete, IngestUpdate}
import uk.gov.nationalarchives.utils.ExternalUtils.{OutputMessage, OutputParameters, OutputProperties}
import uk.gov.nationalarchives.utils.PostingestUtils.{OutputQueueMessage, Queue}
import uk.gov.nationalarchives.utils.{Generators, LambdaRunner}
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient, DASQSClient}
import scala.jdk.CollectionConverters.*
import java.time.Instant
import java.util.UUID

class Lambda extends LambdaRunner[DynamodbEvent, Unit, Config, Dependencies]:

  override def handler: (DynamodbEvent, Config, Dependencies) => IO[Unit] = (event, config, dependencies) => {
    def getPrimaryKey(item: PostIngestStateTableItem) =
      PostIngestStatePrimaryKey(PostIngestStatePartitionKey(item.assetId), PostIngestStateSortKey(item.batchId))

    def updateItem(item: PostIngestStateTableItem, queueAlias: String): IO[Unit] = {
      val batchId = AttributeValue.builder().s(item.batchId).build()
      val postIngestQueue = AttributeValue.builder().s(queueAlias).build()
      val dateTimeNow = dependencies.instantGenerator()
      val dateTimeNowIso = AttributeValue.builder().s(dateTimeNow.toString).build()

      dependencies.daDynamoDbClient
        .updateAttributeValues(
          DADynamoDbRequest(
            config.stateTableName,
            postIngestStatePkFormat.write(getPrimaryKey(item)).toAttributeValue.m().asScala.toMap,
            Map(queue -> postIngestQueue, firstQueued -> dateTimeNowIso, lastQueued -> dateTimeNowIso),
            Some(s"attribute_exists($assetId)")
          )
        )
        .void
    }

    def deleteItemFromTable(item: PostIngestStateTableItem) =
      dependencies.daDynamoDbClient.deleteItems(config.stateTableName, List(getPrimaryKey(item))).void

    def sendMessageToQueue(queueUrl: String, message: OutputQueueMessage): IO[Unit] =
      dependencies.daSqsClient.sendMessage(queueUrl)(message).void

    def sendOutputMessage(item: PostIngestStateTableItem, newQueueAlias: Option[String] = None): IO[Unit] = {
      val (messageType, messageStatus) = newQueueAlias match
        case Some("CC")             => (IngestUpdate, IngestedPreservation)
        case Some(unsupportedQueue) => throw new Exception(s"A 'messageType' and 'messageStatus' implementation exist for queue $unsupportedQueue")
        case None                   => (IngestComplete, IngestedCCDisk)

      val message = OutputMessage(
        OutputProperties(item.batchId, dependencies.uuidGenerator(), item.potentialCorrelationId, dependencies.instantGenerator(), messageType),
        OutputParameters(item.assetId, messageStatus)
      )
      dependencies.daSnsClient.publish(config.topicArn)(message :: Nil).void
    }

    def updateTableAndSendToSqs(newItem: PostIngestStateTableItem, queue: Queue) =
      updateItem(newItem, queue.queueAlias) >>
        sendMessageToQueue(
          queue.queueUrl,
          OutputQueueMessage(newItem.assetId, newItem.batchId, queue.resultAttrName, newItem.input)
        )

    def getInsertFibers(queues: List[Queue]) = event.Records
      .filter(_.eventName == EventName.INSERT)
      .parTraverse { record =>
        val queue1 = queues.find(_.queueOrder == 1).get
        val newImage = record.dynamodb.newImage.get
        val processInsertRecord = updateTableAndSendToSqs(newImage, queue1) >> sendOutputMessage(newImage, Some(queue1.queueAlias))
        processInsertRecord.start
      }

    def newResultDiffersFromOld(newResult: Option[String], oldResult: Option[String]) = newResult.getOrElse("") != oldResult.getOrElse("")

    def getModifyFibers(queues: List[Queue]) = {
      val numOfQueues = queues.length
      event.Records
        .filter(_.eventName == EventName.MODIFY)
        .parTraverse { record =>
          val processModifyRecord =
            (record.dynamodb.oldImage, record.dynamodb.newImage) match
              case (Some(oldItem), Some(newItem)) =>
                val potentialQueue = queues.find(queue => queue.getResult(newItem) != queue.getResult(oldItem))

                potentialQueue match {
                  case Some(queue) =>
                    if queue.queueOrder == numOfQueues then deleteItemFromTable(newItem) >> sendOutputMessage(newItem) // new item has met final check; time to delete it from queue
                    else updateTableAndSendToSqs(newItem, queue) >> sendOutputMessage(newItem, Some(queue.queueAlias))
                  case _ => IO.unit
                }

              case _ => IO.raiseError(new Exception("MODIFY Event was triggered but either an OldImage, NewImage or both don't exist"))

          processModifyRecord.start
        }
    }

    for {
      queues <- IO.fromEither(decode[List[Queue]](config.queues)).map(_.sortBy(_.queueOrder))
      queuePropsAndValues = queues.flatMap(queue => queue.productElementNames.zip(queue.productIterator))
      queuesWithSameValue = queuePropsAndValues.groupBy(identity).filter { case (_, propsAndVals) => propsAndVals.length > 1 }
      _ <- IO.raiseWhen(queuesWithSameValue.nonEmpty) {
        val queueMessage = queuesWithSameValue.keys.map { case (property, value) =>
          s"Property: $property, Value: $value"
        }

        new Exception(s"The values in each queue should be unique but there is more than 1 queue with:\n${queueMessage.mkString("\n")}")
      }
      insert <- getInsertFibers(queues)
      modify <- getModifyFibers(queues)
      allResults <- (insert ++ modify).parTraverse(_.join)
      _ <- allResults.traverse {
        case Outcome.Errored(e) => IO.raiseError(e)
        case _                  => IO.unit
      }
    } yield ()
  }

  override def dependencies(config: Config): IO[Dependencies] = IO(
    Dependencies(DADynamoDBClient[IO](), DASNSClient[IO](), DASQSClient[IO](), () => Generators().generateInstant, () => Generators().generateRandomUuid)
  )

object Lambda:
  given Decoder[DynamodbEvent] = deriveDecoder[DynamodbEvent]

  private def jsonToDynamoValue(json: JsonObject): DynamoValue = {
    json("S").flatMap(_.asString).map(DynamoValue.fromString) <+>
      json("N").flatMap(_.asString).map(_.toLong).map(DynamoValue.fromNumber) <+>
      json("BOOL").flatMap(_.asBoolean).map(DynamoValue.fromBoolean) <+>
      json("L").flatMap(_.asArray).map { vec =>
        DynamoValue.fromDynamoArray(DynamoArray(vec.flatMap(_.asObject).map(jsonToDynamoValue)))
      } <+>
      json("M").flatMap(_.asObject).map { obj =>
        DynamoValue.fromMap {
          obj.toMap.flatMap { case (key, json) =>
            json.asObject.map(j => key -> jsonToDynamoValue(j))
          }
        }
      }
  }.getOrElse(DynamoValue.nil)

  given Decoder[DynamoObject] = (c: HCursor) =>
    for {
      keys <- c.keys.toRight(DecodingFailure.fromThrowable(new Exception("No keys found"), Nil))
    } yield {
      DynamoObject.fromIterable {
        keys.map { key =>
          key -> c
            .downField(key)
            .as[Json]
            .toOption
            .flatMap(_.asObject)
            .map(jsonToDynamoValue)
            .getOrElse(DynamoValue.nil)
        }
      }
    }

  extension [T](dynamoResponse: Either[DynamoReadError, T])
    private def toCirceError: Result[T] =
      dynamoResponse.left.map(err => DecodingFailure.fromThrowable(new Exception(err.show), Nil))

  given Decoder[StreamRecord] = (c: HCursor) =>
    for {
      potentialOldImage <- c.downField("OldImage").as[Option[DynamoObject]]
      potentialNewImage <- c.downField("NewImage").as[Option[DynamoObject]]
      oldItem <- imageOrError(potentialOldImage)
      newItem <- imageOrError(potentialNewImage)
      key <- c.downField("Keys").as[DynamoObject]
      key <- postIngestStatePkFormat.read(key.toDynamoValue).toCirceError
    } yield StreamRecord(key.some, oldItem, newItem)

  private def imageOrError(potentialImage: Option[DynamoObject]) = {
    potentialImage match {
      case Some(image) => postIngestStatusTableItemFormat.read(image.toDynamoValue).toCirceError.map(Option.apply)
      case None        => Right(None)
    }
  }

  given Decoder[List[DynamodbStreamRecord]] = Decoder.decodeList[Option[DynamodbStreamRecord]].map(_.flatten)

  given Decoder[Option[DynamodbStreamRecord]] = (c: HCursor) =>
    for {
      eventName <- c.downField("eventName").as[String]
      potentialStreamRecord <-
        if eventName == EventName.REMOVE.toString then Right(None)
        else c.downField("dynamodb").as[StreamRecord].map(streamRecord => Some(DynamodbStreamRecord(EventName.valueOf(eventName), streamRecord)))
    } yield potentialStreamRecord

  enum EventName:
    case MODIFY, INSERT, REMOVE

  case class Dependencies(
      daDynamoDbClient: DADynamoDBClient[IO],
      daSnsClient: DASNSClient[IO],
      daSqsClient: DASQSClient[IO],
      instantGenerator: () => Instant,
      uuidGenerator: () => UUID
  )

  case class Config(stateTableName: String, stateGsiName: String, topicArn: String, queues: String) derives ConfigReader

  case class DynamodbEvent(Records: List[DynamodbStreamRecord])

  case class DynamodbStreamRecord(eventName: EventName, dynamodb: StreamRecord)

  case class StreamRecord(keys: Option[PostIngestStatePrimaryKey], oldImage: Option[PostIngestStateTableItem], newImage: Option[PostIngestStateTableItem])
