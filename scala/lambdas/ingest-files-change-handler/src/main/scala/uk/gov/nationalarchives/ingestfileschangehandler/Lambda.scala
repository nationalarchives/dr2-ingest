package uk.gov.nationalarchives.ingestfileschangehandler

import cats.effect.{IO, Outcome}
import cats.syntax.all.*
import io.circe.*
import io.circe.Decoder.Result
import io.circe.generic.semiauto.deriveDecoder
import org.scanamo.syntax.*
import org.scanamo.{DynamoArray, DynamoObject, DynamoReadError, DynamoValue}
import pureconfig.ConfigReader
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}
import uk.gov.nationalarchives.DADynamoDBClient.given
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.given
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.utils.Generators
import uk.gov.nationalarchives.ingestfileschangehandler.Lambda.*
import uk.gov.nationalarchives.utils.ExternalUtils.MessageType.*
import uk.gov.nationalarchives.utils.EventCodecs.given
import uk.gov.nationalarchives.utils.ExternalUtils.{MessageStatus, MessageType, OutputMessage, OutputParameters, OutputProperties}
import uk.gov.nationalarchives.utils.ExternalUtils.MessageStatus.*
import uk.gov.nationalarchives.utils.LambdaRunner

import java.time.Instant
import java.util.UUID
import scala.annotation.static

class Lambda extends LambdaRunner[DynamodbEvent, Unit, Config, Dependencies]:

  override def handler: (DynamodbEvent, Config, Dependencies) => IO[Unit] = (event, config, dependencies) => {
    def getPrimaryKey[T <: DynamoItem](row: T) =
      FilesTablePrimaryKey(FilesTablePartitionKey(row.id), FilesTableSortKey(row.batchId))

    def sendIngestedPreservation(assetDynamoItem: AssetDynamoItem): IO[Unit] =
      sendOutputMessage(assetDynamoItem, IngestUpdate, IngestedPreservation)

    def sendIngestedCCDisk(assetDynamoItem: AssetDynamoItem): IO[Unit] =
      sendOutputMessage(assetDynamoItem, IngestComplete, IngestedCCDisk)

    def sendIngestStarted(assetDynamoItem: AssetDynamoItem): IO[Unit] =
      sendOutputMessage(assetDynamoItem, IngestUpdate, IngestStarted)

    def sendOutputMessage(asset: AssetDynamoItem, messageType: MessageType, messageStatus: MessageStatus): IO[Unit] = {
      val message = OutputMessage(
        OutputProperties(asset.batchId, dependencies.uuidGenerator(), asset.correlationId, dependencies.instantGenerator(), messageType),
        OutputParameters(asset.id, messageStatus)
      )
      dependencies.daSnsClient.publish(config.topicArn)(message :: Nil).void
    }

    def sendMessageAndDelete(asset: AssetDynamoItem, children: List[FileDynamoItem]) = for {
      _ <- sendIngestedCCDisk(asset)
      childKeys <- IO.pure(children.map(getPrimaryKey))
      assetKeys <- IO.pure(List(asset).map(getPrimaryKey))
      _ <- dependencies.daDynamoDbClient.deleteItems(config.dynamoTableName, childKeys ++ assetKeys)
    } yield ()

    def sendMessageAndDeleteSkippedAssets(asset: AssetDynamoItem) = {
      for {
        assetsSameId <- dependencies.daDynamoDbClient.queryItems[AssetDynamoItem](config.dynamoTableName, "id" === asset.id.toString)
        skippedAssets <- IO.pure(assetsSameId.filter(asset => asset.skipIngest && asset.ingestedPreservica))
        _ <- skippedAssets.map(asset => sendIngestedCCDisk(asset)).sequence
        _ <- dependencies.daDynamoDbClient.deleteItems(config.dynamoTableName, skippedAssets.map(getPrimaryKey))
      } yield ()
    }

    def childrenOfAsset(asset: AssetDynamoItem): IO[List[FileDynamoItem]] = {
      val childrenParentPath = s"${asset.potentialParentPath.map(path => s"$path/").getOrElse("")}${asset.id}"
      dependencies.daDynamoDbClient
        .queryItems[FileDynamoItem](config.dynamoTableName, "batchId" === asset.batchId and "parentPath" === childrenParentPath, Option(config.dynamoGsiName))
    }

    def getParentAsset(fileRow: FileDynamoItem): IO[Option[AssetDynamoItem]] = for {
      parentId <- IO.fromOption {
        fileRow.potentialParentPath
          .flatMap(_.split('/').lastOption)
          .map(UUID.fromString)
      }(new Exception(s"Cannot find a direct parent for file ${fileRow.id}"))
      parentPrimaryKey <- IO.pure(FilesTablePrimaryKey(FilesTablePartitionKey(parentId), FilesTableSortKey(fileRow.batchId)))
      parentAssets <- dependencies.daDynamoDbClient.getItems[AssetDynamoItem, FilesTablePrimaryKey](List(parentPrimaryKey), config.dynamoTableName)
    } yield parentAssets.headOption

    def processIngestedPreservica(asset: AssetDynamoItem) =
      if asset.ingestedPreservica && !asset.skipIngest then sendIngestedPreservation(asset)
      else if asset.ingestedPreservica && asset.skipIngest then
        for {
          children <- childrenOfAsset(asset)
          assetKey <- IO.pure(getPrimaryKey(asset))
          childKeys <- IO.pure(children.map(getPrimaryKey))
          assetsSameId <- dependencies.daDynamoDbClient.queryItems[AssetDynamoItem](config.dynamoTableName, "id" === asset.id.toString)
          _ <- IO.whenA(assetsSameId.length == 1) {
            dependencies.daDynamoDbClient.deleteItems(config.dynamoTableName, assetKey :: childKeys) >> sendIngestedCCDisk(asset)
          }
          _ <- IO.whenA(assetsSameId.length > 1) {
            dependencies.daDynamoDbClient.deleteItems(config.dynamoTableName, childKeys) >> sendIngestedPreservation(asset)
          }
        } yield ()
      else IO.unit

    def processIngestedPreservicaCC(asset: AssetDynamoItem) =
      if asset.ingestedPreservica && asset.ingestedCustodialCopy then
        for {
          children <- childrenOfAsset(asset)
          _ <- IO.whenA(children.forall(_.ingestedCustodialCopy))(sendMessageAndDelete(asset, children) >> sendMessageAndDeleteSkippedAssets(asset))

        } yield ()
      else IO.unit

    def processAsset(asset: AssetDynamoItem): IO[Unit] = for {
      _ <- processIngestedPreservica(asset)
      _ <- processIngestedPreservicaCC(asset)
    } yield ()

    val insertFibers = event.Records
      .filter(_.eventName == EventName.INSERT)
      .parTraverse { record =>
        val processInsertRecord = record.dynamodb.newImage match
          case Some(item) =>
            item match
              case asset: AssetDynamoItem => sendIngestStarted(asset)
              case _                      => IO.unit
          case None => IO.unit
        processInsertRecord.start
      }
    val modifyFibers = event.Records
      .filter(_.eventName == EventName.MODIFY)
      .parTraverse { record =>
        val processModifyRecord = record.dynamodb.newImage match
          case Some(newImage) =>
            newImage match
              case assetRow: AssetDynamoItem => IO(logger.info(s"Processing asset ${assetRow.id}")) >> processAsset(assetRow)
              case fileRow: FileDynamoItem   => IO(logger.info(s"Processing file ${fileRow.id}")) >> getParentAsset(fileRow).flatMap(_.traverse(processAsset)).void
              case _                         => IO.unit
          case None => IO.unit
        processModifyRecord.start
      }

    for {
      insert <- insertFibers
      modify <- modifyFibers
      allResults <- (insert ++ modify).parTraverse(_.join)
      _ <- allResults.traverse {
        case Outcome.Errored(e) => IO.raiseError(e)
        case _                  => IO.unit
      }
    } yield ()
  }

  override def dependencies(config: Config): IO[Dependencies] = IO(
    Dependencies(DADynamoDBClient[IO](), DASNSClient[IO](), () => Generators().generateInstant, () => Generators().generateRandomUuid)
  )

object Lambda:
  @static def main(args: Array[String]): Unit = new Lambda().run()
  
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
      dynamoResponse.left.map(_ => DecodingFailure.fromThrowable(new Exception("Can't format case classes"), Nil))

  given Decoder[StreamRecord] = (c: HCursor) =>
    {
      for {
        newImage <- c.downField("NewImage").as[DynamoObject]
        key <- c.downField("Keys").as[DynamoObject]
        rowType <- newImage.get("type").map(Type.valueOf).toCirceError
        tableRow: DynamoItem <- rowType.formatter.read(newImage.toDynamoValue).toCirceError
        key <- filesTablePkFormat.read(key.toDynamoValue).toCirceError
      } yield StreamRecord(key.some, tableRow.some)
    }.handleError(_ => StreamRecord(None, None))

  given Decoder[DynamodbStreamRecord] = (c: HCursor) =>
    for {
      eventName <- c.downField("eventName").as[String]
      streamRecord <- c.downField("dynamodb").as[StreamRecord]
    } yield DynamodbStreamRecord(EventName.valueOf(eventName), streamRecord)

  enum EventName:
    case MODIFY, INSERT, REMOVE

  case class Dependencies(daDynamoDbClient: DADynamoDBClient[IO], daSnsClient: DASNSClient[IO], instantGenerator: () => Instant, uuidGenerator: () => UUID)

  case class Config(dynamoTableName: String, dynamoGsiName: String, topicArn: String) derives ConfigReader

  case class DynamodbEvent(Records: List[DynamodbStreamRecord])

  case class DynamodbStreamRecord(eventName: EventName, dynamodb: StreamRecord)

  case class StreamRecord(keys: Option[FilesTablePrimaryKey], newImage: Option[DynamoItem])
