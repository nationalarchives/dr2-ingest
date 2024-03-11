package uk.gov.nationalarchives

import cats.effect._
import cats.implicits._
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import org.scanamo.syntax._
import org.typelevel.log4cats.slf4j.Slf4jFactory
import org.typelevel.log4cats.{LoggerName, SelfAwareStructuredLogger}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient._
import uk.gov.nationalarchives.DynamoFormatters._
import uk.gov.nationalarchives.Lambda._
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient._
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import upickle.default
import upickle.default._

import java.io.{InputStream, OutputStream}
import java.util.UUID

class Lambda extends RequestStreamHandler {
  private val configIo: IO[Config] = ConfigSource.default.loadF[IO, Config]()
  implicit val inputReader: Reader[Input] = macroR[Input]
  val dADynamoDBClient: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  private val sourceId = "SourceID"

  implicit val loggerName: LoggerName = LoggerName("Ingest Asset Reconciler")
  private val logger: SelfAwareStructuredLogger[IO] = Slf4jFactory.create[IO].getLogger

  lazy val entitiesClientIO: IO[EntityClient[IO, Fs2Streams[IO]]] = configIo.flatMap { config =>
    Fs2Client.entityClient(config.apiUrl, config.secretName)
  }

  override def handleRequest(inputStream: InputStream, output: OutputStream, context: Context): Unit = {

    for {
      config <- configIo
      input <- IO {
        val inputString = inputStream.readAllBytes().map(_.toChar).mkString
        read[Input](inputString)
      }
      assetItems <- dADynamoDBClient.getItems[AssetDynamoTable, PartitionKey](
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

      entitiesClient <- entitiesClientIO
      entitiesWithAssetName <- entitiesClient.entitiesByIdentifier(Identifier(sourceId, asset.name))
      entity <- IO.fromOption(entitiesWithAssetName.headOption)(
        new Exception(s"No entity found using SourceId '${asset.name}'")
      )

      urlsToIoRepresentations <- entitiesClient.getUrlsToIoRepresentations(entity.ref, Some(Preservation))
      contentObjects <- urlsToIoRepresentations.map { urlToIoRepresentation =>
        val generationVersion = urlToIoRepresentation.reverse.takeWhile(_ != '/').toInt
        entitiesClient.getContentObjectsFromRepresentation(entity.ref, Preservation, generationVersion)
      }.flatSequence

      _ <- log("Content Objects, belonging to the representation, have been retrieved from API")

      stateOutput <-
        if (contentObjects.isEmpty)
          IO.pure(
            StateOutput(wasReconciled = false, s"There were no Content Objects returned for entity ref '${entity.ref}'")
          )
        else {
          for {
            children <- childrenOfAsset(asset, config.dynamoTableName, config.dynamoGsiName)
            _ <- IO.fromOption(children.headOption)(
              new Exception(s"No children were found for ${input.assetId} from ${input.batchId}")
            )
            _ <- log(s"${children.length} children found for asset ${asset.id}")

            bitstreamInfoPerContentObject <- contentObjects
              .map(co => entitiesClient.getBitstreamInfo(co.ref))
              .flatSequence

            _ <- log(s"Bitstreams of Content Objects have been retrieved from API")
          } yield {
            val childrenThatDidNotMatchOnChecksum =
              children.filter { assetChild =>
                val bitstreamWithSameChecksum = bitstreamInfoPerContentObject.find { bitstreamInfoForCo =>
                  assetChild.checksumSha256 == bitstreamInfoForCo.fixity.value &&
                  bitstreamInfoForCo.potentialCoTitle.exists {
                    titleOfCo => // DDB titles don't have file extensions, CO titles do
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
                s"Out of the ${children.length} files expected to be ingested for assetId '${input.assetId}', " +
                  s"a checksum could not be found for: ${idsOfChildrenThatDidNotMatchOnChecksum.mkString(", ")}"
              )
            }
          }
        }
    } yield output.write(write(stateOutput).getBytes())
  }.onError(logLambdaError).unsafeRunSync()

  private def logLambdaError(error: Throwable): IO[Unit] = logger.error(error)("Error running asset reconciler")

  private def childrenOfAsset(
      asset: AssetDynamoTable,
      tableName: String,
      gsiName: String
  ): IO[List[FileDynamoTable]] = {
    val childrenParentPath = s"${asset.parentPath.map(path => s"$path/").getOrElse("")}${asset.id}"
    dADynamoDBClient
      .queryItems[FileDynamoTable](
        tableName,
        gsiName,
        "batchId" === asset.batchId and "parentPath" === childrenParentPath
      )
  }
}

object Lambda {
  implicit val stateDataWriter: default.Writer[StateOutput] = macroW[StateOutput]
  case class Input(executionId: String, batchId: String, assetId: UUID)

  case class StateOutput(wasReconciled: Boolean, reason: String)

  private case class Config(apiUrl: String, secretName: String, dynamoGsiName: String, dynamoTableName: String)
}
