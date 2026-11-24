package uk.gov.nationalarchives.ingestfindexistingasset

import cats.effect.*
import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.circe.generic.auto.*
import io.circe.parser.decode
import pureconfig.ConfigReader
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbRequest
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient}
import uk.gov.nationalarchives.ingestfindexistingasset.Lambda.*
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.Identifier as AssetIdentifier
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.utils.EventCodecs.given

import scala.jdk.CollectionConverters.*
import java.util.UUID

class Lambda extends LambdaRunner[SQSEvent, Unit, Config, Dependencies] {
  private val sourceId = "SourceID"

  override def handler: (
      SQSEvent,
      Config,
      Dependencies
  ) => IO[Unit] = (sqsEvent, config, dependencies) =>
    def updateSkipIngest(assetDynamoItem: AssetDynamoItem) =
      val skipIngestAttributeValue = AttributeValue.builder().bool(true).build()
      val updateRequest = DADynamoDbRequest(
        config.dynamoTableName,
        Map("id" -> AttributeValue.builder().s(assetDynamoItem.id.toString).build(), "batchId" -> AttributeValue.builder().s(assetDynamoItem.batchId).build()),
        Map("skipIngest" -> skipIngestAttributeValue)
      )
      dependencies.dynamoDbClient.updateAttributeValues(updateRequest).map(_ => ())

    sqsEvent.getRecords.asScala.toList.traverse { record =>
      IO.fromEither(decode[Input](record.getBody)).flatMap { body =>
        body.Items
          .parTraverse { item =>
            for {
              assetItems <- dependencies.dynamoDbClient.getItems[AssetDynamoItem, FilesTablePrimaryKey](
                List(FilesTablePrimaryKey(FilesTablePartitionKey(item.id), FilesTableSortKey(item.batchId))),
                config.dynamoTableName
              )
              asset <- IO.fromOption(assetItems.headOption)(
                new Exception(s"No asset found for ${item.id} from ${item.batchId}")
              )
              _ <- IO.raiseWhen(asset.`type` != Asset)(
                new Exception(s"Object ${asset.id} is of type ${asset.`type`} and not 'Asset'")
              )
              logCtx = Map("batchId" -> item.batchId, "assetId" -> asset.id.toString)
              log = logger.info(logCtx)(_)
              _ <- log(s"Asset ${asset.id} retrieved from Dynamo")
            } yield asset
          }
          .flatMap { assets =>
            val identifiers = assets.map(asset => AssetIdentifier(sourceId, asset.id.toString))

            def output(assetDynamoItem: AssetDynamoItem, exists: Boolean) = OutputItems(assetDynamoItem.id, assetDynamoItem.batchId, exists)

            def notExistsOutput(assetDynamoItem: AssetDynamoItem) = output(assetDynamoItem, false)

            def existsOutput(assetDynamoItem: AssetDynamoItem) = output(assetDynamoItem, true)

            for {
              (existingAssets, missingAssets) <- dependencies.entityClient.entitiesPerIdentifier(identifiers).map { entityMap =>
                assets.partition(asset => entityMap.get(AssetIdentifier(sourceId, asset.id.toString)).flatMap(_.headOption).flatMap(_.entityType).contains(InformationObject))
              }
              _ <- existingAssets.parTraverse(updateSkipIngest)
              stateOutput = StateOutput(existingAssets.map(existsOutput) ++ missingAssets.map(notExistsOutput))
              _ <- dependencies.sfnClient.sendTaskSuccess(body.taskToken, Option(stateOutput))
            } yield ()
          }
          .handleErrorWith { err =>
            logger.warn(err.getMessage) >> dependencies.sfnClient.sendTaskFailure(body.taskToken, Option(err.getMessage))
          }
      }
    }.void

  override def dependencies(config: Config): IO[Dependencies] =
    Fs2Client
      .entityClient(config.secretName)
      .map(client => Dependencies(client, DADynamoDBClient[IO](), DASFNClient[IO]()))
}

object Lambda {
  case class Config(secretName: String, dynamoTableName: String) derives ConfigReader

  case class InputItems(id: UUID, batchId: String)
  case class OutputItems(id: UUID, batchId: String, assetExists: Boolean)

  case class Input(Items: List[InputItems], taskToken: String)

  case class StateOutput(items: List[OutputItems])

  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], dynamoDbClient: DADynamoDBClient[IO], sfnClient: DASFNClient[IO])
}
