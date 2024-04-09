package uk.gov.nationalarchives

import cats.effect._
import cats.implicits._
import io.circe.generic.auto._
import org.scanamo.syntax._
import pureconfig.generic.auto._
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient._
import uk.gov.nationalarchives.DynamoFormatters._
import uk.gov.nationalarchives.Lambda._
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.{Access, Preservation}
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import upickle.default
import upickle.default._

import java.util.UUID
import scala.math.abs

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

  private def stripFileExtension(title: String) = title.split('.').dropRight(1).mkString(".")

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
        case DynamoFormatters.PreservationRepresentationType => Preservation
        case DynamoFormatters.AccessRepresentationType       => Access
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
                          lazy val fileNameWithoutExtension = assetChild.name
                          val potentialAssetChildTitleOrFileName = assetChild.title.getOrElse("")
                          val assetChildTitleOrFileName = if (potentialAssetChildTitleOrFileName.isEmpty) fileNameWithoutExtension else potentialAssetChildTitleOrFileName

                          val numOfDotsInTitleOrFileName = assetChildTitleOrFileName.count(_ == '.')
                          val numOfDotsInTitleOfCo = titleOfCo.count(_ == '.')
                          val differenceInNumberOfDots = numOfDotsInTitleOrFileName - numOfDotsInTitleOfCo

                          val (titleOfCoWithoutExtension, assetChildTitleOrFileNameWithoutExtension) =
                            if (numOfDotsInTitleOrFileName == numOfDotsInTitleOfCo) (titleOfCo, assetChildTitleOrFileName)
                            else if (differenceInNumberOfDots == 1) (titleOfCo, stripFileExtension(assetChildTitleOrFileName))
                            else if (abs(differenceInNumberOfDots) > 1) (titleOfCo, assetChildTitleOrFileName) // then let it fail the equality comparison below
                            else (stripFileExtension(titleOfCo), assetChildTitleOrFileName)

                          titleOfCoWithoutExtension == assetChildTitleOrFileNameWithoutExtension
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
  implicit val stateDataWriter: default.Writer[StateOutput] = macroW[StateOutput]
  case class Input(executionId: String, batchId: String, assetId: UUID)

  case class StateOutput(wasReconciled: Boolean, reason: String)

  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], dynamoDbClient: DADynamoDBClient[IO])
  case class Config(apiUrl: String, secretName: String, dynamoGsiName: String, dynamoTableName: String)
}
