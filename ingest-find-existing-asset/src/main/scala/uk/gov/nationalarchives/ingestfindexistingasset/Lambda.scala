package uk.gov.nationalarchives.ingestfindexistingasset

import cats.effect.*
import io.circe.generic.auto.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbRequest
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.DADynamoDBClient
import uk.gov.nationalarchives.ingestfindexistingasset.Lambda.*
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.{Identifier => PreservicaIdentifier, *}
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.utils.LambdaRunner

import java.util.UUID

class Lambda extends LambdaRunner[Input, StateOutput, Config, Dependencies] {
  private val sourceId = "SourceID"
  override def handler: (
      Input,
      Config,
      Dependencies
  ) => IO[StateOutput] = (input, config, dependencies) =>
    for {
      assetItems <- dependencies.dynamoDbClient.getItems[AssetDynamoTable, FilesTablePartitionKey](
        List(FilesTablePartitionKey(input.id)),
        config.dynamoTableName
      )
      asset <- IO.fromOption(assetItems.headOption)(
        new Exception(s"No asset found for ${input.id} from ${input.batchId}")
      )
      _ <- IO.raiseWhen(asset.`type` != Asset)(
        new Exception(s"Object ${asset.id} is of type ${asset.`type`} and not 'Asset'")
      )
      logCtx = Map("batchId" -> input.batchId, "assetId" -> asset.id.toString)
      log = logger.info(logCtx)(_)
      _ <- log(s"Asset ${asset.id} retrieved from Dynamo")

      entitiesWithAssetName <- dependencies.entityClient.entitiesByIdentifier(PreservicaIdentifier(sourceId, asset.name))
      assetExists = entitiesWithAssetName.headOption.flatMap(_.entityType).contains(InformationObject)

      _ <- IO.whenA(assetExists) {
        val skipIngestAttributeValue = AttributeValue.builder().bool(assetExists).build()
        val updateRequest = DADynamoDbRequest(
          config.dynamoTableName,
          Map("id" -> AttributeValue.builder().s(input.id.toString).build()),
          Map("skipIngest" -> Some(skipIngestAttributeValue))
        )
        dependencies.dynamoDbClient.updateAttributeValues(updateRequest).map(_ => ())
      }
    } yield StateOutput(assetExists)

  override def dependencies(config: Config): IO[Dependencies] =
    Fs2Client
      .entityClient(config.apiUrl, config.secretName)
      .map(client => Dependencies(client, DADynamoDBClient[IO]()))
}

object Lambda {
  case class Config(apiUrl: String, secretName: String, dynamoTableName: String) derives ConfigReader

  case class Input(id: UUID, batchId: String)

  case class StateOutput(assetExists: Boolean)

  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], dynamoDbClient: DADynamoDBClient[IO])
}
