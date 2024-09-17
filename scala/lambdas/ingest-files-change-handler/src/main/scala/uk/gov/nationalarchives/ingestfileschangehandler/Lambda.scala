package uk.gov.nationalarchives.ingestfileschangehandler

import cats.effect.IO
import cats.syntax.all.*
import io.circe.*
import io.circe.Decoder.Result
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.scanamo.syntax.*
import org.scanamo.{DynamoArray, DynamoObject, DynamoReadError, DynamoValue}
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}
import uk.gov.nationalarchives.DADynamoDBClient.given
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.given
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.utils.Generators
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
      val message = OutputMessage(
        OutputProperties(asset.batchId, dependencies.uuidGenerator(), asset.correlationId, dependencies.instantGenerator(), messageType),
        OutputParameters(asset.id)
      )
      dependencies.daSnsClient.publish(config.topicArn)(message :: Nil).map(_ => ())
    }

    def sendMessageAndDelete(asset: AssetDynamoTable, children: List[FileDynamoTable]) = for {
      _ <- sendOutputMessage(asset, IngestComplete)
      childKeys <- IO.pure(children.map(getPrimaryKey))
      assetKeys <- IO.pure(List(asset).map(getPrimaryKey))
      _ <- dependencies.daDynamoDbClient.deleteItems(config.dynamoTableName, childKeys ++ assetKeys)
    } yield ()

    def sendMessageAndDeleteSkippedAssets(asset: AssetDynamoTable) = {
      for {
        assetsSameId <- dependencies.daDynamoDbClient.queryItems[AssetDynamoTable](config.dynamoTableName, "id" === asset.id.toString)
        skippedAssets <- IO.pure(assetsSameId.filter(asset => asset.skipIngest && asset.ingestedPreservica))
        _ <- skippedAssets.map(asset => sendOutputMessage(asset, IngestComplete)).sequence
        _ <- dependencies.daDynamoDbClient.deleteItems(config.dynamoTableName, skippedAssets.map(getPrimaryKey))
      } yield ()
    }

    def childrenOfAsset(asset: AssetDynamoTable): IO[List[FileDynamoTable]] = {
      val childrenParentPath = s"${asset.potentialParentPath.map(path => s"$path/").getOrElse("")}${asset.id}"
      dependencies.daDynamoDbClient
        .queryItems[FileDynamoTable](config.dynamoTableName, "batchId" === asset.batchId and "parentPath" === childrenParentPath, Option(config.dynamoGsiName))
    }

    def getParentAsset(fileRow: FileDynamoTable): IO[AssetDynamoTable] = for {
      parentId <- IO.fromOption {
        fileRow.potentialParentPath
          .flatMap(_.split('/').lastOption)
          .map(UUID.fromString)
      }(new Exception(s"Cannot find a direct parent for file ${fileRow.id}"))
      parentPrimaryKey <- IO.pure(FilesTablePrimaryKey(FilesTablePartitionKey(parentId), FilesTableSortKey(fileRow.batchId)))
      parentAssets <- dependencies.daDynamoDbClient.getItems[AssetDynamoTable, FilesTablePrimaryKey](List(parentPrimaryKey), config.dynamoTableName)
      _ <- IO.raiseWhen(parentAssets.length != 1)(new Exception(s"Expected 1 parent asset, found ${parentAssets.length} assets for file $parentId"))
    } yield parentAssets.head

    def processAsset(asset: AssetDynamoTable): IO[Unit] = {
      if asset.ingestedPreservica && asset.ingestedCustodialCopy then
        for {
          children <- childrenOfAsset(asset)
          _ <- IO.whenA(children.forall(_.ingestedCustodialCopy))(sendMessageAndDelete(asset, children) >> sendMessageAndDeleteSkippedAssets(asset))

        } yield ()
      else if asset.ingestedPreservica && asset.skipIngest then
        for {
          children <- childrenOfAsset(asset)
          assetKey <- IO.pure(getPrimaryKey(asset))
          childKeys <- IO.pure(children.map(getPrimaryKey))
          assetsSameId <- dependencies.daDynamoDbClient.queryItems[AssetDynamoTable](config.dynamoTableName, "id" === asset.id.toString)
          _ <- IO.whenA(assetsSameId.length == 1) {
            dependencies.daDynamoDbClient.deleteItems(config.dynamoTableName, assetKey :: childKeys) >> sendOutputMessage(asset, IngestComplete)
          }
          _ <- IO.whenA(assetsSameId.length > 1) {
            dependencies.daDynamoDbClient.deleteItems(config.dynamoTableName, childKeys) >> sendOutputMessage(asset, IngestUpdate)
          }
        } yield ()
      else if asset.ingestedPreservica then sendOutputMessage(asset, IngestUpdate)
      else IO.unit
    }

    event.Records
      .filter(_.eventName == EventName.MODIFY)
      .map { record =>
        record.dynamodb.newImage match
          case Some(newImage) =>
            newImage match
              case assetRow: AssetDynamoTable => logger.info(s"Processing asset ${assetRow.id}") >> processAsset(assetRow)
              case fileRow: FileDynamoTable   => logger.info(s"Processing file ${fileRow.id}") >> getParentAsset(fileRow).flatMap(processAsset)
              case _                          => IO.unit
          case None => IO.unit
      }
      .sequence
      .map(_ => ())
  }

  override def dependencies(config: Config): IO[Dependencies] = IO(
    Dependencies(DADynamoDBClient[IO](), DASNSClient[IO](), () => Generators().generateInstant, () => Generators().generateRandomUuid)
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
      dynamoResponse.left.map(_ => DecodingFailure.fromThrowable(new Exception("Can't format case classes"), Nil))

  given Decoder[StreamRecord] = (c: HCursor) =>
    {
      for {
        newImage <- c.downField("NewImage").as[DynamoObject]
        key <- c.downField("Keys").as[DynamoObject]
        rowType <- newImage.get("type").map(Type.valueOf).toCirceError
        tableRow: DynamoTable <- rowType.formatter.read(newImage.toDynamoValue).toCirceError
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

  case class StreamRecord(keys: Option[FilesTablePrimaryKey], newImage: Option[DynamoTable])

  enum MessageType:
    override def toString: String = this match
      case IngestUpdate   => "preserve.digital.asset.ingest.update"
      case IngestComplete => "preserve.digital.asset.ingest.complete"
    case IngestUpdate, IngestComplete

  given Encoder[MessageType] = deriveEncoder[MessageType]
  given Encoder[OutputProperties] = deriveEncoder[OutputProperties]
  given Encoder[OutputParameters] = deriveEncoder[OutputParameters]
  given Encoder[OutputMessage] = deriveEncoder[OutputMessage]

  case class OutputProperties(executionId: String, messageId: UUID, parentMessageId: Option[String], timestamp: Instant, `type`: MessageType)
  case class OutputParameters(assetId: UUID)
  case class OutputMessage(properties: OutputProperties, parameters: OutputParameters)
