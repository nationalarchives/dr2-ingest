package uk.gov.nationalarchives.ingestfileschangehandler

import cats.effect.IO
import cats.syntax.all.*
import io.circe.Decoder.Result
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.*
import org.scanamo.{DynamoArray, DynamoObject, DynamoReadError, DynamoValue}
import org.scanamo.syntax.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}
import uk.gov.nationalarchives.DADynamoDBClient.{*, given}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.given
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.ingestfileschangehandler.Lambda.*
import uk.gov.nationalarchives.ingestfileschangehandler.Lambda.MessageType.*
import uk.gov.nationalarchives.utils.LambdaRunner
import java.time.Instant
import java.util.UUID

class Lambda extends LambdaRunner[DynamodbEvent, Unit, Config, Dependencies]:

  override def handler: (DynamodbEvent, Config, Dependencies) => IO[Unit] = (event, config, dependencies) => {
    def getPrimaryKey[T <: DynamoTable](row: T) =
      FilesTablePrimaryKey(FilesTablePartitionKey(row.id), FilesTableSortKey(row.batchId))

    def sendOutputMessage(asset: AssetDynamoTable, messageType: MessageType): IO[Unit] = {
      val message = OutputMessage(OutputProperties(dependencies.uuidGenerator(), None, dependencies.instantGenerator(), messageType), OutputParameters(asset.id))
      dependencies.daSnsClient.publish(config.topicArn)(message :: Nil).map(_ => ())
    }

    def completeAsset(asset: AssetDynamoTable, children: List[FileDynamoTable]) = {
      for {
        _ <- sendOutputMessage(asset, IngestComplete)
        childKeys <- IO.pure(children.map(getPrimaryKey))
        assetKeys <- IO.pure(List(asset).map(getPrimaryKey))
        _ <- dependencies.daDynamoDbClient.deleteItems(config.dynamoDbTable, childKeys ++ assetKeys)
        assetsSameId <- dependencies.daDynamoDbClient.queryItems[AssetDynamoTable](config.dynamoDbTable, "id" === asset.id.toString)
        completedAssets <- IO.pure(assetsSameId.filter(asset => asset.skipIngest && asset.ingestedPreservica))
        _ <- completedAssets.map(asset => sendOutputMessage(asset, IngestComplete)).sequence
        _ <- dependencies.daDynamoDbClient.deleteItems(config.dynamoDbTable, completedAssets.map(getPrimaryKey))
      } yield ()
    }

    def childrenOfAsset(asset: AssetDynamoTable): IO[List[FileDynamoTable]] = {
      val childrenParentPath = s"${asset.parentPath.map(path => s"$path/").getOrElse("")}${asset.id}"
      for {
        children <- dependencies.daDynamoDbClient
          .queryItems[FileDynamoTable](config.dynamoDbTable, "batchId" === asset.batchId and "parentPath" === childrenParentPath, Option(config.gsiName))
        _ <- IO.raiseWhen(children.length != asset.childCount)(
          new Exception(s"Asset id ${asset.id}: has ${asset.childCount} children in the files table but found ${children.length} children in the Preservation system")
        )
      } yield children
    }

    def processFile(fileRow: FileDynamoTable): IO[Unit] = for {
      parentId <- IO.fromOption {
        fileRow.parentPath
          .flatMap(_.split('/').lastOption)
          .map(UUID.fromString)
      }(new Exception(s"Cannot find a direct parent for file ${fileRow.id}"))
      primaryKey <- IO.pure(FilesTablePrimaryKey(FilesTablePartitionKey(parentId), FilesTableSortKey(fileRow.batchId)))
      parentAssetList <- dependencies.daDynamoDbClient.getItems[AssetDynamoTable, FilesTablePrimaryKey](List(primaryKey), config.dynamoDbTable)
      _ <- IO.raiseWhen(parentAssetList.length != 1)(new Exception(s"Expected 1 parent asset, found ${parentAssetList.length} assets for file $parentId"))
      _ <- processAsset(parentAssetList.head)
    } yield ()

    def processAsset(asset: AssetDynamoTable): IO[Unit] = {
      if asset.ingestedPreservica && asset.ingestedCustodialCopy then
        for {
          children <- childrenOfAsset(asset)
          _ <- IO.whenA(children.forall(_.ingestedCustodialCopy))(completeAsset(asset, children))

        } yield ()
      else if asset.ingestedPreservica && asset.skipIngest then
        for {
          children <- childrenOfAsset(asset)
          assetKey <- IO.pure(getPrimaryKey(asset))
          childKeys <- IO.pure(children.map(getPrimaryKey))
          assetsSameId <- dependencies.daDynamoDbClient.queryItems[AssetDynamoTable](config.dynamoDbTable, "id" === asset.id.toString)
          _ <- IO.whenA(assetsSameId.length == 1) {
            dependencies.daDynamoDbClient.deleteItems(config.dynamoDbTable, assetKey :: childKeys) >> sendOutputMessage(asset, IngestComplete)
          }
          _ <- IO.whenA(assetsSameId.length > 1) {
            dependencies.daDynamoDbClient.deleteItems(config.dynamoDbTable, childKeys) >> sendOutputMessage(asset, IngestUpdate)
          }
        } yield ()
      else if asset.ingestedPreservica then
        sendOutputMessage(asset, IngestUpdate)
      else
        IO.unit
    }

    event.Records.filter(_.eventName == EventName.MODIFY).map { record =>
      record.dynamodb.newImage match
        case assetRow: AssetDynamoTable => processAsset(assetRow)
        case fileRow: FileDynamoTable => processFile(fileRow)
        case _ => IO.unit
    }.sequence.map(_ => ())
  }

  override def dependencies(config: Config): IO[Dependencies] = IO(Dependencies(DADynamoDBClient[IO](), DASNSClient[IO](), () => Instant.now, () => UUID.randomUUID))

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
          obj.toMap.flatMap {
            case (key, json) => json.asObject.map(j => key -> jsonToDynamoValue(j))
          }
        }
      }
  }.getOrElse(DynamoValue.nil)

  given Decoder[DynamoObject] = (c: HCursor) => for {
    keys <- c.keys.toRight(DecodingFailure.fromThrowable(new Exception("No keys found"), Nil))
  } yield {
    DynamoObject.fromIterable {
      keys.map { key =>
        key -> c.downField(key).as[Json]
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

  given Decoder[StreamRecord] = (c: HCursor) => for {
    newImage <- c.downField("NewImage").as[DynamoObject]
    key <- c.downField("Keys").as[DynamoObject]
    tableRow: DynamoTable <- archiveFolderTableFormat.read(newImage.toDynamoValue).toCirceError
    key <- filesTablePkFormat.read(key.toDynamoValue).toCirceError
  } yield {
    StreamRecord(key, tableRow)
  }

  given Decoder[DynamodbStreamRecord] = (c: HCursor) => for {
    eventName <- c.downField("eventName").as[String]
    streamRecord <- c.downField("dynamodb").as[StreamRecord]
  } yield DynamodbStreamRecord(EventName.valueOf(eventName), streamRecord)

  enum EventName:
    case MODIFY, INSERT, DELETE

  case class Dependencies(daDynamoDbClient: DADynamoDBClient[IO], daSnsClient: DASNSClient[IO], instantGenerator: () => Instant, uuidGenerator: () => UUID)

  case class Config(dynamoDbTable: String, gsiName: String, topicArn: String) derives ConfigReader

  case class DynamodbEvent(Records: List[DynamodbStreamRecord])

  case class DynamodbStreamRecord(eventName: EventName, dynamodb: StreamRecord)

  case class StreamRecord(keys: FilesTablePrimaryKey, newImage: DynamoTable)


  enum MessageType:
    override def toString: String = this match
      case IngestUpdate => "preserve.digital.asset.ingest.update"
      case IngestComplete => "preserve.digital.asset.ingest.complete"
    case IngestUpdate, IngestComplete

  given Encoder[MessageType] = deriveEncoder[MessageType]
  given Encoder[OutputProperties] = deriveEncoder[OutputProperties]
  given Encoder[OutputParameters] = deriveEncoder[OutputParameters]
  given Encoder[OutputMessage] = deriveEncoder[OutputMessage]

  case class OutputProperties(messageId: UUID, parentMessageId: Option[UUID], timestamp: Instant, `type`: MessageType)
  case class OutputParameters(assetId: UUID)
  case class OutputMessage(properties: OutputProperties, parameters: OutputParameters)

