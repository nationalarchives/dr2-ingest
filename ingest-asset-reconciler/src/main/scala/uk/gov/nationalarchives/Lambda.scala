package uk.gov.nationalarchives

import cats.effect.*
import cats.implicits.*
import io.circe.generic.auto.*
import org.scanamo.syntax.*
import pureconfig.generic.derivation.default.*
import pureconfig.ConfigReader
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DynamoFormatters.Type.*
import uk.gov.nationalarchives.DynamoFormatters.*
import uk.gov.nationalarchives.DADynamoDBClient.{given, *}
import uk.gov.nationalarchives.Lambda.*
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType.*
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client

import java.util.UUID

class Lambda extends LambdaRunner[Input, StateOutput, Config, Dependencies] {

  private val sourceId = "SourceID"

  private def childrenOfAsset(
      daDynamoDBClient: DADynamoDBClient[IO],
      asset: AssetDynamoTable,
      tableName: String,
      gsiName: String
  ): IO[List[FileDynamoTable]] = {
    val childrenParentPath = s"${asset.parentPath.map(path => s"$path/").getOrElse("")}${asset.id}"
    daDynamoDBClient
      .queryItems[FileDynamoTable](
        tableName,
        gsiName,
        "batchId" === asset.batchId and "parentPath" === childrenParentPath
      )
  }

  override def handler: (
      Input,
      Config,
      Dependencies
  ) => IO[StateOutput] = (input, config, dependencies) =>
    for {
      assetItems <- dependencies.dynamoDbClient.getItems[AssetDynamoTable, PartitionKey](
        List(PartitionKey(input.assetId)),
        config.dynamoTableName
      )
      asset <- IO.fromOption(assetItems.headOption)(
        new Exception(s"No asset found for ${input.assetId} from ${input.batchId}")
      )
      _ <- IO.raiseWhen(asset.`type` != Asset)(
        new Exception(s"Object ${asset.id} is of type ${asset.`type`} and not 'Asset'")
      )

      logCtx = Map("batchId" -> input.batchId, "assetId" -> asset.id.toString)
      log = logger.info(logCtx)(_)
      _ <- log(s"Asset ${asset.id} retrieved from Dynamo")

      entitiesWithAssetName <- dependencies.entityClient.entitiesByIdentifier(Identifier(sourceId, asset.name))
      entity <- IO.fromOption(entitiesWithAssetName.headOption)(
        new Exception(s"No entity found using SourceId '${asset.name}'")
      )
      children <- childrenOfAsset(dependencies.dynamoDbClient, asset, config.dynamoTableName, config.dynamoGsiName)
      _ <- IO.fromOption(children.headOption)(
        new Exception(s"No children were found for ${input.assetId} from ${input.batchId}")
      )
      _ <- log(s"${children.length} children found for asset ${asset.id}")
      childrenGroupedByRepType = children.groupBy(_.representationType match {
        case DynamoFormatters.FileRepresentationType.PreservationRepresentationType => Preservation
        case DynamoFormatters.FileRepresentationType.AccessRepresentationType       => Access
      })

      stateOutputs <- childrenGroupedByRepType
        .map { case (representationType, childrenForRepresentationType) =>
          for {
            urlsToIoRepresentations <- dependencies.entityClient.getUrlsToIoRepresentations(entity.ref, Some(representationType))
            contentObjects <- urlsToIoRepresentations.map { urlToIoRepresentation =>
              val generationVersion = urlToIoRepresentation.reverse.takeWhile(_ != '/').toInt
              dependencies.entityClient.getContentObjectsFromRepresentation(entity.ref, representationType, generationVersion)
            }.flatSequence

            _ <- log("Content Objects, belonging to the representation, have been retrieved from API")

            stateOutput <-
              if (contentObjects.isEmpty)
                IO.pure(
                  StateOutput(wasReconciled = false, s"There were no Content Objects returned for entity ref '${entity.ref}'")
                )
              else {
                for {
                  bitstreamInfoPerContentObject <- contentObjects
                    .map(co => dependencies.entityClient.getBitstreamInfo(co.ref))
                    .flatSequence

                  _ <- log(s"Bitstreams of Content Objects have been retrieved from API")
                } yield {
                  val childrenThatDidNotMatchOnChecksum =
                    childrenForRepresentationType.filter { assetChild =>
                      val bitstreamWithSameChecksum = bitstreamInfoPerContentObject.find { bitstreamInfoForCo =>
                        assetChild.checksumSha256 == bitstreamInfoForCo.fixity.value &&
                        bitstreamInfoForCo.potentialCoTitle.exists { titleOfCo => // DDB titles don't have file extensions, CO titles do
                          val titleOfCoWithoutExtension = titleOfCo.split('.').dropRight(1).mkString(".")
                          lazy val fileNameWithoutExtension = assetChild.name.split('.').dropRight(1).mkString(".")
                          val titleOrFileName = assetChild.title.getOrElse(fileNameWithoutExtension)
                          val assetChildTitle = if (titleOrFileName.isEmpty) fileNameWithoutExtension else titleOrFileName
                          titleOfCoWithoutExtension == assetChildTitle
                        }
                      }

                      bitstreamWithSameChecksum.isEmpty
                    }

                  if (childrenThatDidNotMatchOnChecksum.isEmpty) StateOutput(wasReconciled = true, "")
                  else {
                    val idsOfChildrenThatDidNotMatchOnChecksum = childrenThatDidNotMatchOnChecksum.map(_.id)
                    StateOutput(
                      wasReconciled = false,
                      s"Out of the ${childrenForRepresentationType.length} files expected to be ingested for assetId '${input.assetId}' with representationType $representationType, " +
                        s"a checksum could not be found for: ${idsOfChildrenThatDidNotMatchOnChecksum.mkString(", ")}"
                    )
                  }
                }
              }
          } yield stateOutput

        }
        .toList
        .sequence
    } yield StateOutput(stateOutputs.forall(_.wasReconciled), stateOutputs.map(_.reason).sorted.toSet.mkString("\n").trim)
  override def dependencies(config: Config): IO[Dependencies] = Fs2Client.entityClient(config.apiUrl, config.secretName).map(client => Dependencies(client, DADynamoDBClient[IO]()))
}

object Lambda {

  case class Input(executionId: String, batchId: String, assetId: UUID)

  case class StateOutput(wasReconciled: Boolean, reason: String)

  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], dynamoDbClient: DADynamoDBClient[IO])
  case class Config(apiUrl: String, secretName: String, dynamoGsiName: String, dynamoTableName: String) derives ConfigReader
}
