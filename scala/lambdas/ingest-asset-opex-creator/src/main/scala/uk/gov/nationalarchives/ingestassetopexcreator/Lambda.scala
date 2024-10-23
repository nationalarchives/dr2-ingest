package uk.gov.nationalarchives.ingestassetopexcreator

import cats.effect.*
import cats.implicits.*
import fs2.Stream
import io.circe.generic.auto.*
import org.reactivestreams.FlowAdapters
import org.scanamo.syntax.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.dp.client.ValidateXmlAgainstXsd
import uk.gov.nationalarchives.dp.client.ValidateXmlAgainstXsd.PreservicaSchema.{OpexMetadataSchema, XipXsdSchemaV7}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.ingestassetopexcreator.Lambda.*
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import uk.gov.nationalarchives.DADynamoDBClient.given
import java.time.OffsetDateTime
import java.util.UUID

class Lambda extends LambdaRunner[Input, Unit, Config, Dependencies] {

  override def dependencies(config: Config): IO[Dependencies] = IO(Dependencies(DADynamoDBClient[IO](), DAS3Client[IO](), XMLCreator(OffsetDateTime.now())))

  override def handler: (Input, Config, Dependencies) => IO[Unit] = { (input, config, dependencies) =>
    val xmlCreator = dependencies.xmlCreator
    val dynamoClient = dependencies.dynamoClient
    val s3Client = dependencies.s3Client
    input.items
      .filterNot(_.assetExists)
      .parTraverse { item =>
        for {
          assetItems <- dynamoClient.getItems[AssetDynamoItem, FilesTablePrimaryKey](
            List(FilesTablePrimaryKey(FilesTablePartitionKey(item.id), FilesTableSortKey(item.batchId))),
            config.dynamoTableName
          )
          asset <- IO.fromOption(assetItems.headOption)(
            new Exception(s"No asset found for ${item.id} and ${item.batchId}")
          )
          fileReference = asset.identifiers.find(_.identifierName == "BornDigitalRef").map(_.value).orNull
          log = logger.info(Map("batchRef" -> item.batchId, "fileReference" -> fileReference, "assetId" -> asset.id.toString))(_)
          _ <- IO.whenA(asset.`type` != Asset)(IO.raiseError(new Exception(s"Object ${asset.id} is of type ${asset.`type`} and not 'Asset'")))
          _ <- log(s"Asset ${asset.id} retrieved from Dynamo")

          children <- childrenOfAsset(dynamoClient, asset, config.dynamoTableName, config.dynamoGsiName)
          _ <- IO.raiseWhen(children.length != asset.childCount)(
            new Exception(s"Asset id ${asset.id}: has ${asset.childCount} children in the files table but found ${children.length} children in the Preservation system")
          )
          _ <- IO.fromOption(children.headOption)(new Exception(s"No children found for ${item.id} and ${item.batchId}"))
          _ <- log(s"${children.length} children found for asset ${asset.id}")

          _ <- log(s"Starting copy from ${children.map(_.location).mkString(", ")} to ${config.destinationBucket}")
          _ <- children.parTraverse(child => copyFromSourceToDestination(s3Client, item, config.destinationBucket, asset, child, xmlCreator))
          xip <- xmlCreator.createXip(asset, children.sortBy(_.sortOrder))
          _ <- ValidateXmlAgainstXsd[IO](XipXsdSchemaV7).xmlStringIsValid(xip)
          _ <- uploadXMLToS3(s3Client, xip, config.destinationBucket, s"${assetPath(item, asset)}/${asset.id}.xip")
          _ <- log(s"XIP ${assetPath(item, asset)}/${asset.id}.xip uploaded to ${config.destinationBucket}")

          opex <- xmlCreator.createOpex(asset, children, xip.getBytes.length, asset.identifiers)
          _ <- ValidateXmlAgainstXsd[IO](OpexMetadataSchema).xmlStringIsValid(opex)
          _ <- uploadXMLToS3(s3Client, opex, config.destinationBucket, s"${parentPath(item, asset)}/${asset.id}.pax.opex")
          _ <- log(s"OPEX ${parentPath(item, asset)}/${asset.id}.pax.opex uploaded to ${config.destinationBucket}")
        } yield ()
      }
      .void
  }

  private def uploadXMLToS3(s3Client: DAS3Client[IO], xmlString: String, destinationBucket: String, key: String): IO[CompletedUpload] =
    Stream.emits[IO, Byte](xmlString.getBytes).chunks.map(_.toByteBuffer).toPublisherResource.use { publisher =>
      s3Client.upload(destinationBucket, key, FlowAdapters.toPublisher(publisher))
    }

  private def copyFromSourceToDestination(
      s3Client: DAS3Client[IO],
      item: InputItem,
      destinationBucket: String,
      asset: AssetDynamoItem,
      child: FileDynamoItem,
      xmlCreator: XMLCreator
  ) = {
    s3Client.copy(
      child.location.getHost,
      child.location.getPath.drop(1),
      destinationBucket,
      destinationPath(item, asset, child, xmlCreator)
    )
  }

  private def parentPath(item: InputItem, asset: AssetDynamoItem) = s"opex/${item.batchId}${asset.potentialParentPath.map(path => s"/$path").getOrElse("")}"

  private def assetPath(item: InputItem, asset: AssetDynamoItem) = s"${parentPath(item, asset)}/${asset.id}.pax"

  private def destinationPath(item: InputItem, asset: AssetDynamoItem, child: FileDynamoItem, xmlCreator: XMLCreator) =
    s"${assetPath(item, asset)}/${xmlCreator.bitstreamPath(child)}/${xmlCreator.childFileName(child)}"

  private def childrenOfAsset(dynamoClient: DADynamoDBClient[IO], asset: AssetDynamoItem, tableName: String, gsiName: String): IO[List[FileDynamoItem]] = {
    val childrenParentPath = s"${asset.potentialParentPath.map(path => s"$path/").getOrElse("")}${asset.id}"
    dynamoClient
      .queryItems[FileDynamoItem](tableName, "batchId" === asset.batchId and "parentPath" === childrenParentPath, Option(gsiName))
  }
}

object Lambda {

  case class InputItem(id: UUID, batchId: String, assetExists: Boolean)

  case class Input(items: List[InputItem])

  case class Config(dynamoTableName: String, dynamoGsiName: String, destinationBucket: String) derives ConfigReader

  case class Dependencies(dynamoClient: DADynamoDBClient[IO], s3Client: DAS3Client[IO], xmlCreator: XMLCreator)
}
