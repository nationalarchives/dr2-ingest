package uk.gov.nationalarchives

import cats.effect._
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import fs2.Stream
import org.reactivestreams.FlowAdapters
import org.scanamo.syntax._
import org.typelevel.log4cats.slf4j.Slf4jFactory
import org.typelevel.log4cats.{LoggerName, SelfAwareStructuredLogger}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.DADynamoDBClient._
import uk.gov.nationalarchives.DynamoFormatters._
import uk.gov.nationalarchives.Lambda._
import upickle.default._

import java.io.{InputStream, OutputStream}
import java.time.OffsetDateTime
import java.util.UUID

class Lambda extends RequestStreamHandler {
  val dynamoClient: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  val s3Client: DAS3Client[IO] = DAS3Client[IO]()

  implicit val loggerName: LoggerName = LoggerName(sys.env("AWS_LAMBDA_FUNCTION_NAME"))
  private val logger: SelfAwareStructuredLogger[IO] = Slf4jFactory.create[IO].getLogger

  def getXmlCreator: XMLCreator = {
    val ingestDateTime: OffsetDateTime = OffsetDateTime.now()
    XMLCreator(ingestDateTime)
  }

  override def handleRequest(inputStream: InputStream, outputStream: OutputStream, context: Context): Unit = {
    val inputString = inputStream.readAllBytes().map(_.toChar).mkString
    val input = read[Input](inputString)
    val xmlCreator = getXmlCreator
    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      assetItems <- dynamoClient.getItems[AssetDynamoTable, PartitionKey](List(PartitionKey(input.id)), config.dynamoTableName)
      asset <- IO.fromOption(assetItems.headOption)(
        new Exception(s"No asset found for ${input.id} and ${input.batchId}")
      )
      fileReference = asset.identifiers.find(_.identifierName == "BornDigitalRef").map(_.value).orNull
      log = logger.info(Map("batchRef" -> input.batchId, "fileReference" -> fileReference, "assetId" -> asset.id.toString))(_)
      _ <- if (asset.`type` != Asset) IO.raiseError(new Exception(s"Object ${asset.id} is of type ${asset.`type`} and not 'Asset'")) else IO.unit
      _ <- log(s"Asset ${asset.id} retrieved from Dynamo")

      children <- childrenOfAsset(asset, config.dynamoTableName, config.dynamoGsiName)
      _ <- IO.fromOption(children.headOption)(new Exception(s"No children found for ${input.id} and ${input.batchId}"))
      _ <- log(s"${children.length} children found for asset ${asset.id}")

      _ <- log(s"Starting copy from ${input.sourceBucket} to ${config.destinationBucket}")
      _ <- children.map(child => copyFromSourceToDestination(input, config.destinationBucket, asset, child, xmlCreator)).sequence
      xip <- xmlCreator.createXip(asset, children.sortBy(_.sortOrder))
      _ <- uploadXMLToS3(xip, config.destinationBucket, s"${assetPath(input, asset)}/${asset.id}.xip")
      _ <- log(s"XIP ${assetPath(input, asset)}/${asset.id}.xip uploaded to ${config.destinationBucket}")

      opex <- xmlCreator.createOpex(asset, children, xip.getBytes.length, asset.identifiers)
      _ <- uploadXMLToS3(opex, config.destinationBucket, s"${parentPath(input, asset)}/${asset.id}.pax.opex")
      _ <- log(s"OPEX ${parentPath(input, asset)}/${asset.id}.pax.opex uploaded to ${config.destinationBucket}")
    } yield ()
  }.onError(logLambdaError).unsafeRunSync()

  private def logLambdaError(error: Throwable): IO[Unit] = logger.error(error)("Error ingest asset opex creator")

  private def uploadXMLToS3(xmlString: String, destinationBucket: String, key: String): IO[CompletedUpload] =
    Stream.emits[IO, Byte](xmlString.getBytes).chunks.map(_.toByteBuffer).toPublisherResource.use { publisher =>
      s3Client.upload(destinationBucket, key, xmlString.getBytes.length, FlowAdapters.toPublisher(publisher))
    }

  private def copyFromSourceToDestination(input: Input, destinationBucket: String, asset: AssetDynamoTable, child: FileDynamoTable, xmlCreator: XMLCreator) = {
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

  private def childrenOfAsset(asset: AssetDynamoTable, tableName: String, gsiName: String): IO[List[FileDynamoTable]] = {
    val childrenParentPath = s"${asset.parentPath.map(path => s"$path/").getOrElse("")}${asset.id}"
    dynamoClient
      .queryItems[FileDynamoTable](tableName, gsiName, "batchId" === asset.batchId and "parentPath" === childrenParentPath)
  }
}

object Lambda {
  implicit val treInputReader: Reader[Input] = macroR[Input]

  case class Input(id: UUID, batchId: String, executionName: String, sourceBucket: String)

  private case class Config(dynamoTableName: String, dynamoGsiName: String, destinationBucket: String)
}
