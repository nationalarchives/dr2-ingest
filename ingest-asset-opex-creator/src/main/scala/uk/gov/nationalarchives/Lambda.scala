package uk.gov.nationalarchives

import cats.effect.*
import cats.implicits.*
import fs2.Stream
import io.circe.generic.auto.*
import org.reactivestreams.FlowAdapters
import org.scanamo.syntax.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.DADynamoDBClient.{given, *}
import uk.gov.nationalarchives.DynamoFormatters.Type.*
import uk.gov.nationalarchives.DynamoFormatters.*
import uk.gov.nationalarchives.Lambda.*

import java.time.OffsetDateTime
import java.util.UUID

class Lambda extends LambdaRunner[Input, Unit, Config, Dependencies] {

  override def dependencies(config: Config): IO[Dependencies] = IO(Dependencies(DADynamoDBClient[IO](), DAS3Client[IO](), XMLCreator(OffsetDateTime.now())))

  override def handler: (Input, Config, Dependencies) => IO[Unit] = { (input, config, dependencies) =>
    val xmlCreator = dependencies.xmlCreator
    val dynamoClient = dependencies.dynamoClient
    val s3Client = dependencies.s3Client
    for {
      assetItems <- dynamoClient.getItems[AssetDynamoTable, FilesTablePartitionKey](List(FilesTablePartitionKey(input.id)), config.dynamoTableName)
      asset <- IO.fromOption(assetItems.headOption)(
        new Exception(s"No asset found for ${input.id} and ${input.batchId}")
      )
      fileReference = asset.identifiers.find(_.identifierName == "BornDigitalRef").map(_.value).orNull
      log = logger.info(Map("batchRef" -> input.batchId, "fileReference" -> fileReference, "assetId" -> asset.id.toString))(_)
      _ <- IO.whenA(asset.`type` != Asset)(IO.raiseError(new Exception(s"Object ${asset.id} is of type ${asset.`type`} and not 'Asset'")))
      _ <- log(s"Asset ${asset.id} retrieved from Dynamo")

      children <- childrenOfAsset(dynamoClient, asset, config.dynamoTableName, config.dynamoGsiName)
      _ <- IO.fromOption(children.headOption)(new Exception(s"No children found for ${input.id} and ${input.batchId}"))
      _ <- log(s"${children.length} children found for asset ${asset.id}")

      _ <- log(s"Starting copy from ${input.sourceBucket} to ${config.destinationBucket}")
      _ <- children.map(child => copyFromSourceToDestination(s3Client, input, config.destinationBucket, asset, child, xmlCreator)).sequence
      xip <- xmlCreator.createXip(asset, children.sortBy(_.sortOrder))
      _ <- uploadXMLToS3(s3Client, xip, config.destinationBucket, s"${assetPath(input, asset)}/${asset.id}.xip")
      _ <- log(s"XIP ${assetPath(input, asset)}/${asset.id}.xip uploaded to ${config.destinationBucket}")

      opex <- xmlCreator.createOpex(asset, children, xip.getBytes.length, asset.identifiers)
      _ <- uploadXMLToS3(s3Client, opex, config.destinationBucket, s"${parentPath(input, asset)}/${asset.id}.pax.opex")
      _ <- log(s"OPEX ${parentPath(input, asset)}/${asset.id}.pax.opex uploaded to ${config.destinationBucket}")
    } yield ()
  }

  private def uploadXMLToS3(s3Client: DAS3Client[IO], xmlString: String, destinationBucket: String, key: String): IO[CompletedUpload] =
    Stream.emits[IO, Byte](xmlString.getBytes).chunks.map(_.toByteBuffer).toPublisherResource.use { publisher =>
      s3Client.upload(destinationBucket, key, xmlString.getBytes.length, FlowAdapters.toPublisher(publisher))
    }

  private def copyFromSourceToDestination(
      s3Client: DAS3Client[IO],
      input: Input,
      destinationBucket: String,
      asset: AssetDynamoTable,
      child: FileDynamoTable,
      xmlCreator: XMLCreator
  ) = {
    s3Client.copy(
      input.sourceBucket,
      s"${input.batchId}/data/${child.id}",
      destinationBucket,
      destinationPath(input, asset, child, xmlCreator)
    )
  }

  private def parentPath(input: Input, asset: AssetDynamoTable) = s"opex/${input.executionName}${asset.parentPath.map(path => s"/$path").getOrElse("")}"

  private def assetPath(input: Input, asset: AssetDynamoTable) = s"${parentPath(input, asset)}/${asset.id}.pax"

  private def destinationPath(input: Input, asset: AssetDynamoTable, child: FileDynamoTable, xmlCreator: XMLCreator) =
    s"${assetPath(input, asset)}/${xmlCreator.bitstreamPath(child)}/${xmlCreator.childFileName(child)}"

  private def childrenOfAsset(dynamoClient: DADynamoDBClient[IO], asset: AssetDynamoTable, tableName: String, gsiName: String): IO[List[FileDynamoTable]] = {
    val childrenParentPath = s"${asset.parentPath.map(path => s"$path/").getOrElse("")}${asset.id}"
    dynamoClient
      .queryItems[FileDynamoTable](tableName, gsiName, "batchId" === asset.batchId and "parentPath" === childrenParentPath)
  }
}

object Lambda {

  case class Input(id: UUID, batchId: String, executionName: String, sourceBucket: String)

  case class Config(dynamoTableName: String, dynamoGsiName: String, destinationBucket: String) derives ConfigReader

  case class Dependencies(dynamoClient: DADynamoDBClient[IO], s3Client: DAS3Client[IO], xmlCreator: XMLCreator)
}
