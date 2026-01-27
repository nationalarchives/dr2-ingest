package uk.gov.nationalarchives.ingestfolderopexcreator

import cats.effect.IO
import cats.implicits.*
import fs2.Stream
import io.circe.generic.auto.*
import org.reactivestreams.FlowAdapters
import org.scanamo.*
import org.scanamo.syntax.*
import pureconfig.ConfigReader
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.DADynamoDBClient.given
import uk.gov.nationalarchives.dp.client.ValidateXmlAgainstXsd.PreservicaSchema
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client.xmlValidator
import uk.gov.nationalarchives.ingestfolderopexcreator.Lambda.*
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.dp.client.EntityClient.SecurityTag.*

import java.util.UUID
import scala.jdk.CollectionConverters.MapHasAsScala

class Lambda extends LambdaRunner[Input, Unit, Config, Dependencies] {

  private def toFolderOrAssetItem[T <: DynamoItem](dynamoValue: DynamoValue)(using dynamoFormat: DynamoFormat[T]): Either[DynamoReadError, FolderOrAssetItem] =
    dynamoFormat.read(dynamoValue).map {
      case item @ (asset: AssetDynamoItem) =>
        AssetItem(item.batchId, item.id, item.potentialParentPath, item.`type`, item.potentialTitle, item.potentialDescription, item.identifiers, asset.skipIngest)
      case item @ (contentFolder: ContentFolderDynamoItem) =>
        FolderItem(
          item.batchId,
          item.id,
          item.potentialParentPath,
          contentFolder.name,
          item.`type`,
          item.potentialTitle,
          item.potentialDescription,
          item.identifiers
        )
      case item @ (archiveFolder: ArchiveFolderDynamoItem) =>
        FolderItem(
          item.batchId,
          item.id,
          item.potentialParentPath,
          archiveFolder.name,
          item.`type`,
          item.potentialTitle,
          item.potentialDescription,
          item.identifiers
        )
      case _ => throw new RuntimeException("Row is not an 'Asset' or a 'Folder'")
    }

  given DynamoFormat[FolderOrAssetItem] = new DynamoFormat[FolderOrAssetItem] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, FolderOrAssetItem] =
      dynamoValue.toAttributeValue.m().asScala.toMap.get("type").map(_.s()) match {
        case Some(rowType) =>
          rowType match {
            case "Asset"                           => toFolderOrAssetItem[AssetDynamoItem](dynamoValue)
            case "ArchiveFolder" | "ContentFolder" => toFolderOrAssetItem[ArchiveFolderDynamoItem](dynamoValue)
            case _                                 => Left(TypeCoercionError(new RuntimeException("Row is not an 'Asset' or a 'Folder'")))
          }
        case None => Left[DynamoReadError, FolderOrAssetItem](MissingProperty)
      }

    // We're not using write but we have to have this overridden
    override def write(t: FolderOrAssetItem): DynamoValue = DynamoValue.nil
  }

  private def isFolder(rowType: Type) = List(ContentFolder, ArchiveFolder).contains(rowType)

  private def generateKey(executionName: String, folder: DynamoItem) =
    s"opex/$executionName/${formatParentPath(folder.potentialParentPath)}${folder.id}/${folder.id}.opex"

  private def formatParentPath(potentialParentPath: Option[String]): String = potentialParentPath.map(parentPath => s"$parentPath/").getOrElse("")

  override def handler: (
      Input,
      Config,
      Dependencies
  ) => IO[Unit] = (input, config, dependencies) =>
    def childrenOfFolder(asset: ArchiveFolderDynamoItem, tableName: String, gsiName: String): IO[List[FolderOrAssetItem]] = {
      val childrenParentPath = s"${asset.potentialParentPath.getOrElse("")}/${asset.id}".stripPrefix("/")
      dependencies.dynamoClient
        .queryItems[FolderOrAssetItem](tableName, "batchId" === asset.batchId and "parentPath" === childrenParentPath, Option(gsiName))
    }

    def uploadXMLToS3(xmlString: String, destinationBucket: String, key: String): IO[CompletedUpload] =
      Stream.emits[IO, Byte](xmlString.getBytes).chunks.map(_.toByteBuffer).toPublisherResource.use { publisher =>
        dependencies.s3Client.upload(destinationBucket, key, FlowAdapters.toPublisher(publisher))
      }

    def getAssetRowsWithFileSize(children: List[FolderOrAssetItem], bucketName: String, executionName: String): IO[List[AssetWithFileSize]] =
      children.collect {
        case child @ asset if child.`type` == Asset =>
          val key = s"opex/$executionName/${formatParentPath(asset.parentPath)}${asset.id}.pax.opex"
          dependencies.s3Client
            .headObject(bucketName, key)
            .map(headResponse => AssetWithFileSize(asset, headResponse.contentLength()))
      }.sequence

    for {
      folderItems <- dependencies.dynamoClient
        .getItems[ArchiveFolderDynamoItem, FilesTablePrimaryKey](
          List(FilesTablePrimaryKey(FilesTablePartitionKey(input.id), FilesTableSortKey(input.batchId))),
          config.dynamoTableName
        )
      folder <- IO.fromOption(folderItems.headOption)(
        new Exception(s"No folder found for ${input.id} and ${input.batchId}")
      )
      idCode = folder.identifiers.find(_.identifierName == "Code").map(_.value).orNull
      log = logger.info(Map("batchRef" -> input.batchId, "folderId" -> folder.id.toString, "idCode" -> idCode))(_)

      _ <- IO.whenA(!isFolder(folder.`type`))(IO.raiseError(new Exception(s"Object ${folder.id} is of type ${folder.`type`} and not 'ContentFolder' or 'ArchiveFolder'")))
      _ <- log(s"Fetched ${folderItems.length} folder items from Dynamo")

      children <- childrenOfFolder(folder, config.dynamoTableName, config.dynamoGsiName)
      _ <- IO.raiseWhen(folder.childCount != children.length)(
        new Exception(s"Folder id ${folder.id}: has ${folder.childCount} children in the files table but found ${children.length} children in the Preservation system")
      )
      childrenWithoutSkip <- IO(children.filterNot(_.skipIngest))
      _ <- IO.fromOption(children.headOption)(new Exception(s"No children found for ${input.id} and ${input.batchId}"))
      _ <- log(s"Fetched ${children.length} children from Dynamo")

      assetRows <- getAssetRowsWithFileSize(childrenWithoutSkip, config.bucketName, input.executionName)
      _ <- log("File sizes for assets fetched from S3")

      folderRows <- IO.pure(childrenWithoutSkip.filter(child => isFolder(child.`type`)))
      folderOpex <- dependencies.xmlCreator.createFolderOpex(folder, assetRows, folderRows, folder.identifiers, Unknown)
      _ <- xmlValidator(PreservicaSchema.OpexMetadataSchema).xmlStringIsValid("""<?xml version="1.0" encoding="UTF-8"?>""" + folderOpex)
      key = generateKey(input.executionName, folder)
      _ <- uploadXMLToS3(folderOpex, config.bucketName, key)
      _ <- log(s"Uploaded OPEX $key to S3 ${config.bucketName}")
    } yield ()

  override def dependencies(config: Config): IO[Dependencies] = {
    val xmlCreator: XMLCreator = XMLCreator()
    val dynamoClient: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
    val s3Client: DAS3Client[IO] = DAS3Client[IO](config.roleArn, lambdaName)
    IO(Dependencies(dynamoClient, s3Client, xmlCreator))
  }
}

object Lambda {
  case class Config(dynamoTableName: String, bucketName: String, dynamoGsiName: String, roleArn: String) derives ConfigReader

  case class Input(id: UUID, batchId: String, executionName: String)

  case class AssetWithFileSize(asset: FolderOrAssetItem, fileSize: Long)

  sealed trait FolderOrAssetItem {
    def batchId: String
    def id: UUID
    def parentPath: Option[String]
    def `type`: Type
    def title: Option[String]
    def description: Option[String]
    def identifiers: List[Identifier]
    def skipIngest: Boolean
  }

  case class FolderItem(
      batchId: String,
      id: UUID,
      parentPath: Option[String],
      name: String,
      `type`: Type,
      title: Option[String],
      description: Option[String],
      identifiers: List[Identifier],
      skipIngest: Boolean = false
  ) extends FolderOrAssetItem

  case class AssetItem(
      batchId: String,
      id: UUID,
      parentPath: Option[String],
      `type`: Type,
      title: Option[String],
      description: Option[String],
      identifiers: List[Identifier],
      skipIngest: Boolean = false
  ) extends FolderOrAssetItem

  case class Dependencies(dynamoClient: DADynamoDBClient[IO], s3Client: DAS3Client[IO], xmlCreator: XMLCreator)

}
