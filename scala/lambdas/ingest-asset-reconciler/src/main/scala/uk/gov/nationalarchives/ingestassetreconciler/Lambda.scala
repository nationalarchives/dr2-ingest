package uk.gov.nationalarchives.ingestassetreconciler

import cats.effect.*
import cats.implicits.*
import io.circe.generic.auto.*
import org.scanamo.syntax.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient.given
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.DADynamoDBClient
import uk.gov.nationalarchives.dp.client.Client.BitStreamInfo
import uk.gov.nationalarchives.dp.client.{Client, EntityClient}
import uk.gov.nationalarchives.dp.client.EntityClient.Identifier as PreservicaIdentifier
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType.*
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters
import uk.gov.nationalarchives.ingestassetreconciler.Lambda.*
import uk.gov.nationalarchives.utils.LambdaRunner

import java.time.OffsetDateTime
import java.util.UUID
import scala.math.abs

class Lambda extends LambdaRunner[Input, StateOutput, Config, Dependencies] {

  private val sourceId = "SourceID"

  private def childrenOfAsset(
      daDynamoDBClient: DADynamoDBClient[IO],
      asset: AssetDynamoItem,
      tableName: String,
      gsiName: String
  ): IO[List[FileDynamoItem]] = {
    val childrenParentPath = s"${asset.potentialParentPath.map(path => s"$path/").getOrElse("")}${asset.id}"
    daDynamoDBClient
      .queryItems[FileDynamoItem](
        tableName,
        "batchId" === asset.batchId and "parentPath" === childrenParentPath,
        Option(gsiName)
      )
  }

  private def stripFileExtension(title: String) = title.split('.').dropRight(1).mkString(".")

  private def coTitleMatchesAssetChildTitle(potentialCoTitle: Option[String], assetChild: FileDynamoItem): Boolean =
    potentialCoTitle.exists { titleOfCo => // DDB titles don't have file extensions, CO titles do
      lazy val fileNameWithoutExtension = assetChild.name
      val potentialAssetChildTitleOrFileName = assetChild.potentialTitle.getOrElse("")
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

  private def doesChecksumMatchFixity(item: DynamoFormatters.FileDynamoItem, fixities: List[Client.Fixity]): Boolean = {
    val sortedChecksums = item.checksums.sortBy(_.algorithm.toUpperCase)
    val sortedFixities = fixities.sortBy(_.algorithm)

    sortedChecksums.zip(sortedFixities).forall { case (dynamoItemChecksum, preservedFixity) =>
      dynamoItemChecksum.algorithm.toUpperCase == preservedFixity.algorithm && dynamoItemChecksum.fingerprint == preservedFixity.value
    }
  }

  private def verifyFilesInDdbAreInPreservica(
      childrenForRepresentationType: List[FileDynamoItem],
      bitstreamInfoPerContentObject: Seq[BitStreamInfo],
      assetId: UUID,
      representationType: RepresentationType
  ) = {
    val childrenThatDidNotMatchOnChecksum =
      childrenForRepresentationType.filter { assetChild =>
        val bitstreamWithSameChecksum = bitstreamInfoPerContentObject.find { bitstreamInfoForCo =>
          doesChecksumMatchFixity(assetChild, bitstreamInfoForCo.fixities) &&
          coTitleMatchesAssetChildTitle(bitstreamInfoForCo.potentialCoTitle, assetChild)
        }

        bitstreamWithSameChecksum.isEmpty
      }

    if (childrenThatDidNotMatchOnChecksum.isEmpty) StateOutput(wasReconciled = true, "", assetId)
    else
      StateOutput(
        wasReconciled = false,
        s":alert-noflash-slow: Reconciliation Failure - Out of the *${childrenForRepresentationType.length}* files expected " +
          s"to be ingested for `assetId` '*$assetId*' with `representationType` *$representationType*, " +
          s"a _*checksum*_ and _*title*_ could not be matched with a file on Preservica for:\n" +
          childrenThatDidNotMatchOnChecksum.zip(LazyList.from(1)).map((child, index) => s"$index. ${child.id}").mkString("\n"),
        assetId
      )
  }

  override def handler: (
      Input,
      Config,
      Dependencies
  ) => IO[StateOutput] = (input, config, dependencies) =>
    for {
      assetId <- IO.pure(input.assetId)
      assetItems <- dependencies.dynamoDbClient.getItems[AssetDynamoItem, FilesTablePrimaryKey](
        List(FilesTablePrimaryKey(FilesTablePartitionKey(assetId), FilesTableSortKey(input.batchId))),
        config.dynamoTableName
      )

      batchId = input.batchId
      asset <- IO.fromOption(assetItems.headOption)(
        new Exception(s"No asset found for $assetId from $batchId")
      )
      _ <- IO.raiseWhen(asset.`type` != Asset)(
        new Exception(s"Object $assetId is of type ${asset.`type`} and not 'Asset'")
      )

      logCtx = Map("batchId" -> batchId, "assetId" -> assetId.toString)
      log = logger.info(logCtx)(_)
      _ <- log(s"Asset $assetId retrieved from Dynamo")

      entitiesWithAssetId <- dependencies.entityClient.entitiesByIdentifier(PreservicaIdentifier(sourceId, assetId.toString))
      _ <- IO.raiseWhen(entitiesWithAssetId.length > 1)(
        new Exception(s"More than one entity found using $sourceId '$assetId'")
      )
      entity <- IO.fromOption(entitiesWithAssetId.headOption)(
        new Exception(s"No entity found using $sourceId '$assetId'")
      )

      children <- childrenOfAsset(dependencies.dynamoDbClient, asset, config.dynamoTableName, config.dynamoGsiName)
      _ <- IO.raiseWhen(asset.childCount != children.length)(
        new Exception(s"Asset id $assetId: has ${asset.childCount} children in the files table but found ${children.length} children in the Preservation system")
      )
      _ <- IO.fromOption(children.headOption)(
        new Exception(s"No children were found for $assetId from $batchId")
      )
      _ <- log(s"${children.length} children found for asset $assetId")
      childrenGroupedByRepType = children.groupBy(_.representationType match {
        case FileRepresentationType.PreservationRepresentationType => Preservation
        case FileRepresentationType.AccessRepresentationType       => Access
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
                  StateOutput(wasReconciled = false, s"There were no Content Objects returned for entity ref '${entity.ref}'", assetId)
                )
              else
                for {
                  bitstreamInfoPerContentObject <- contentObjects
                    .map(co => dependencies.entityClient.getBitstreamInfo(co.ref))
                    .flatSequence

                  _ <- log(s"Bitstreams of Content Objects have been retrieved from API")
                } yield verifyFilesInDdbAreInPreservica(childrenForRepresentationType, bitstreamInfoPerContentObject, assetId, representationType)
          } yield stateOutput
        }
        .toList
        .sequence
      allReconciled = stateOutputs.forall(_.wasReconciled)
    } yield StateOutput(allReconciled, stateOutputs.map(_.reason).sorted.toSet.mkString("\n").trim, assetId)

  override def dependencies(config: Config): IO[Dependencies] =
    Fs2Client
      .entityClient(config.apiUrl, config.secretName)
      .map(client => Dependencies(client, DADynamoDBClient[IO](), UUID.randomUUID(), () => OffsetDateTime.now()))
}

object Lambda {

  case class Input(executionId: String, batchId: String, assetId: UUID)

  case class AssetMessage(messageId: UUID, parentMessageId: Option[UUID] = None, executionId: Option[String])

  case class StateOutput(wasReconciled: Boolean, reason: String, assetId: UUID)

  case class Dependencies(entityClient: EntityClient[IO, Fs2Streams[IO]], dynamoDbClient: DADynamoDBClient[IO], newMessageId: UUID, datetime: () => OffsetDateTime)

  case class Config(apiUrl: String, secretName: String, dynamoGsiName: String, dynamoTableName: String) derives ConfigReader
}
