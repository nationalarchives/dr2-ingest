package uk.gov.nationalarchives

import cats.effect.*
import cats.implicits.*
import io.circe.generic.auto.*
import io.circe.parser.decode
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

  private def coTitleMatchesAssetChildTitle(potentialCoTitle: Option[String], assetChild: FileDynamoTable): Boolean = {
    potentialCoTitle.exists { titleOfCo => // DDB titles don't have file extensions, CO titles do
      lazy val fileNameWithoutExtension = assetChild.name
      val potentialAssetChildTitleOrFileName = assetChild.title.getOrElse("")
      val assetChildTitleOrFileName = if potentialAssetChildTitleOrFileName.isEmpty then fileNameWithoutExtension else potentialAssetChildTitleOrFileName

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

  override def handler: (
      Input,
      Config,
      Dependencies
  ) => IO[StateOutput] = (input, config, dependencies) =>
    for {
      assetId <- IO.pure(input.assetId)
      assetItems <- dependencies.dynamoDbClient.getItems[AssetDynamoTable, PartitionKey](
        List(PartitionKey(assetId)),
        config.dynamoTableName
      )

      batchId = input.batchId
      asset <- IO.fromOption(assetItems.headOption)(
        new Exception(s"No asset found for $assetId from $batchId")
      )
      _ <- IO.raiseWhen(asset.`type` != Asset)(
        new Exception(s"Object ${asset.id} is of type ${asset.`type`} and not 'Asset'")
      )

      logCtx = Map("batchId" -> batchId, "assetId" -> asset.id.toString)
      log = logger.info(logCtx)(_)
      _ <- log(s"Asset ${asset.id} retrieved from Dynamo")

      entitiesWithAssetName <- dependencies.entityClient.entitiesByIdentifier(Identifier(sourceId, asset.name))
      entity <- IO.fromOption(entitiesWithAssetName.headOption)(
        new Exception(s"No entity found using SourceId '${asset.name}'")
      )

      children <- childrenOfAsset(dependencies.dynamoDbClient, asset, config.dynamoTableName, config.dynamoGsiName)
      _ <- IO.fromOption(children.headOption)(
        new Exception(s"No children were found for $assetId from $batchId")
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
                        coTitleMatchesAssetChildTitle(bitstreamInfoForCo.potentialCoTitle, assetChild)
                      }

                      bitstreamWithSameChecksum.isEmpty
                    }

                  if (childrenThatDidNotMatchOnChecksum.isEmpty) StateOutput(wasReconciled = true, "")
                  else {
                    val idsOfChildrenThatDidNotMatchOnChecksum = childrenThatDidNotMatchOnChecksum.map(_.id)
                    StateOutput(
                      wasReconciled = false,
                      s"Out of the ${childrenForRepresentationType.length} files expected to be ingested for assetId '$assetId' with representationType $representationType, " +
                        s"a checksum and title could not be matched with a file on Preservica for: ${idsOfChildrenThatDidNotMatchOnChecksum.mkString(", ")}"
                    )
                  }
                }
              }
          } yield stateOutput
        }
        .toList
        .sequence
      allReconciled = stateOutputs.forall(_.wasReconciled)
      combinedOutputs = StateOutput(allReconciled, stateOutputs.map(_.reason).sorted.toSet.mkString("\n").trim)

      finalOutput <-
        if (allReconciled)
          for {
            items <- dependencies.dynamoDbClient.getItems[IngestLockTable, PartitionKey](
              List(PartitionKey(assetId)),
              config.dynamoLockTableName
            )

            attributes <- IO.fromOption(items.headOption)(
              new Exception(s"No items found for assetId '$assetId' from batchId '$batchId'")
            )

            message <- IO.fromEither(decode[AssetMessage](attributes.message.replace('\'', '\"')))
            executionId = message.executionId

            _ <- IO.raiseWhen(executionId != batchId) {
              new Exception(s"executionId '$executionId' belonging to assetId '$assetId' does not equal '$batchId'")
            }
          } yield combinedOutputs.copy(
            reconciliationSnsMessage = Some(
              ReconciliationSnsMessage(
                "Asset was reconciled",
                assetId,
                NewMessageProperties(dependencies.newMessageId, message.messageId, batchId)
              )
            )
          )
        else IO.pure(combinedOutputs)
    } yield finalOutput
  override def dependencies(config: Config): IO[Dependencies] =
    Fs2Client
      .entityClient(config.apiUrl, config.secretName)
      .map(client => Dependencies(client, DADynamoDBClient[IO](), UUID.randomUUID()))
}

object Lambda {

  case class Input(executionId: String, batchId: String, assetId: UUID)

  case class AssetMessage(messageId: UUID, parentMessageId: Option[UUID] = None, executionId: String)

  case class NewMessageProperties(messageId: UUID, parentMessageId: UUID, executionId: String)

  case class ReconciliationSnsMessage(reconciliationUpdate: String, assetId: UUID, properties: NewMessageProperties)

  case class StateOutput(wasReconciled: Boolean, reason: String, reconciliationSnsMessage: Option[ReconciliationSnsMessage] = None)

  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], dynamoDbClient: DADynamoDBClient[IO], newMessageId: UUID)
  case class Config(apiUrl: String, secretName: String, dynamoGsiName: String, dynamoTableName: String, dynamoLockTableName: String) derives ConfigReader
}
