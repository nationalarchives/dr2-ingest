package uk.gov.nationalarchives.ingestfolderopexcreator

import cats.effect.IO
import cats.implicits.*
import fs2.Stream
import io.circe.generic.auto.*
import org.reactivestreams.FlowAdapters
import org.scanamo.*
import org.scanamo.syntax.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.DADynamoDBClient.given
import uk.gov.nationalarchives.dp.client.ValidateXmlAgainstXsd.PreservicaSchema
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client.xmlValidator
import uk.gov.nationalarchives.ingestfolderopexcreator.Lambda.*
import uk.gov.nationalarchives.utils.LambdaRunner

import java.util.UUID
import scala.jdk.CollectionConverters.MapHasAsScala

class Lambda extends LambdaRunner[Input, Unit, Config, Dependencies] {

  private def toFolderOrAssetTable[T <: DynamoTable](dynamoValue: DynamoValue)(using dynamoFormat: DynamoFormat[T]): Either[DynamoReadError, FolderOrAssetTable] =
    dynamoFormat.read(dynamoValue).map { table =>
      FolderOrAssetTable(table.batchId, table.id, table.parentPath, table.name, table.`type`, table.title, table.description, table.identifiers)
    }

  given DynamoFormat[FolderOrAssetTable] = new DynamoFormat[FolderOrAssetTable] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, FolderOrAssetTable] =
      dynamoValue.toAttributeValue.m().asScala.toMap.get("type").map(_.s()) match {
        case Some(rowType) =>
          rowType match {
            case "Asset"                           => toFolderOrAssetTable[AssetDynamoTable](dynamoValue)
            case "ArchiveFolder" | "ContentFolder" => toFolderOrAssetTable[ArchiveFolderDynamoTable](dynamoValue)
            case _                                 => Left(TypeCoercionError(new RuntimeException("Row is not an 'Asset' or a 'Folder'")))
          }
        case None => Left[DynamoReadError, FolderOrAssetTable](MissingProperty)
      }

    // We're not using write but we have to have this overridden
    override def write(t: FolderOrAssetTable): DynamoValue = DynamoValue.nil
  }

  private def isFolder(rowType: Type) = List(ContentFolder, ArchiveFolder).contains(rowType)

  private def generateKey(executionName: String, folder: DynamoTable) =
    s"opex/$executionName/${formatParentPath(folder.parentPath)}${folder.id}/${folder.id}.opex"

  private def formatParentPath(potentialParentPath: Option[String]): String = potentialParentPath.map(parentPath => s"$parentPath/").getOrElse("")

  private def uploadXMLToS3(s3Client: DAS3Client[IO], xmlString: String, destinationBucket: String, key: String): IO[CompletedUpload] =
    Stream.emits[IO, Byte](xmlString.getBytes).chunks.map(_.toByteBuffer).toPublisherResource.use { publisher =>
      s3Client.upload(destinationBucket, key, xmlString.getBytes.length, FlowAdapters.toPublisher(publisher))
    }

  private def getAssetRowsWithFileSize(s3Client: DAS3Client[IO], children: List[FolderOrAssetTable], bucketName: String, executionName: String): IO[List[AssetWithFileSize]] = {
    children.collect {
      case child @ asset if child.`type` == Asset =>
        val key = s"opex/$executionName/${formatParentPath(asset.parentPath)}${asset.id}.pax.opex"
        s3Client
          .headObject(bucketName, key)
          .map(headResponse => AssetWithFileSize(asset, headResponse.contentLength()))
    }.sequence
  }

  private def childrenOfFolder(dynamoClient: DADynamoDBClient[IO], asset: ArchiveFolderDynamoTable, tableName: String, gsiName: String): IO[List[FolderOrAssetTable]] = {
    val childrenParentPath = s"${asset.parentPath.getOrElse("")}/${asset.id}".stripPrefix("/")
    dynamoClient
      .queryItems[FolderOrAssetTable](tableName, gsiName, "batchId" === asset.batchId and "parentPath" === childrenParentPath)
  }
  override def handler: (
      Input,
      Config,
      Dependencies
  ) => IO[Unit] = (input, config, dependencies) =>
    for {
      folderItems <- dependencies.dynamoClient
        .getItems[ArchiveFolderDynamoTable, FilesTablePartitionKey](List(FilesTablePartitionKey(input.id, input.batchId)), config.dynamoTableName)
      folder <- IO.fromOption(folderItems.headOption)(
        new Exception(s"No folder found for ${input.id} and ${input.batchId}")
      )
      idCode = folder.identifiers.find(_.identifierName == "Code").map(_.value).orNull
      log = logger.info(Map("batchRef" -> input.batchId, "folderId" -> folder.id.toString, "idCode" -> idCode))(_)

      _ <- IO.whenA(!isFolder(folder.`type`))(IO.raiseError(new Exception(s"Object ${folder.id} is of type ${folder.`type`} and not 'ContentFolder' or 'ArchiveFolder'")))
      _ <- log(s"Fetched ${folderItems.length} folder items from Dynamo")

      children <- childrenOfFolder(dependencies.dynamoClient, folder, config.dynamoTableName, config.dynamoGsiName)
      _ <- IO.raiseWhen(folder.childCount != children.length)(
        new Exception(s"Folder id ${folder.id}: has ${folder.childCount} children in the files table but found ${children.length} children in the Preservation system")
      )
      _ <- IO.fromOption(children.headOption)(new Exception(s"No children found for ${input.id} and ${input.batchId}"))
      _ <- log(s"Fetched ${children.length} children from Dynamo")

      assetRows <- getAssetRowsWithFileSize(dependencies.s3Client, children, config.bucketName, input.executionName)
      _ <- log("File sizes for assets fetched from S3")

      folderRows <- IO.pure(children.filter(child => isFolder(child.`type`)))
      folderOpex <- dependencies.xmlCreator.createFolderOpex(folder, assetRows, folderRows, folder.identifiers)
      _ <- xmlValidator(PreservicaSchema.OpexMetadataSchema).xmlStringIsValid("""<?xml version="1.0" encoding="UTF-8"?>""" + folderOpex)
      key = generateKey(input.executionName, folder)
      _ <- uploadXMLToS3(dependencies.s3Client, folderOpex, config.bucketName, key)
      _ <- log(s"Uploaded OPEX $key to S3 ${config.bucketName}")
    } yield ()

  override def dependencies(config: Config): IO[Dependencies] = {
    val xmlCreator: XMLCreator = XMLCreator()
    val dynamoClient: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
    val s3Client: DAS3Client[IO] = DAS3Client[IO]()
    IO(Dependencies(dynamoClient, s3Client, xmlCreator))
  }
}

object Lambda {
  case class Config(dynamoTableName: String, bucketName: String, dynamoGsiName: String) derives ConfigReader

  case class Input(id: UUID, batchId: String, executionName: String)

  case class AssetWithFileSize(asset: FolderOrAssetTable, fileSize: Long)

  case class FolderOrAssetTable(
      batchId: String,
      id: UUID,
      parentPath: Option[String],
      name: String,
      `type`: Type,
      title: Option[String],
      description: Option[String],
      identifiers: List[Identifier]
  )

  case class Dependencies(dynamoClient: DADynamoDBClient[IO], s3Client: DAS3Client[IO], xmlCreator: XMLCreator)

}
