package uk.gov.nationalarchives.preingestcourtdocpackagebuilder

import uk.gov.nationalarchives.utils.LambdaRunner
import Lambda.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import cats.effect.IO
import fs2.Stream
import org.reactivestreams.FlowAdapters
import pureconfig.ConfigReader
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import uk.gov.nationalarchives.DADynamoDBClient.given
import uk.gov.nationalarchives.utils.ExternalUtils.{*, given}
import org.scanamo.syntax.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.IngestLockTableItem

import java.net.URI
import java.nio.ByteBuffer
import java.util.UUID

class Lambda extends LambdaRunner[Input, Output, Config, Dependencies]:

  override def handler: (Input, Config, Dependencies) => IO[Output] = (input, config, dependencies) => {

    def lockTableItems: IO[List[IngestLockTableItem]] =
      dependencies.dynamoDbClient
        .queryItems[IngestLockTableItem](config.lockTableName, "groupId" === input.groupId, Option(config.lockTableGsiName))

    def uploadMetadata(metadata: List[MetadataObject], s3Location: URI) =
      Stream
        .eval(IO.pure(metadata.asJson.noSpaces))
        .map(s => ByteBuffer.wrap(s.getBytes()))
        .toPublisherResource
        .use { pub =>
          dependencies.s3Client.upload(s3Location.getHost, s3Location.getPath.drop(1), FlowAdapters.toPublisher(pub))
        }
        .void

    def processItems(
        lockTableItems: List[IngestLockTableItem],
        archiveFolders: List[ArchiveFolderMetadataObject] = Nil,
        allMetadata: List[MetadataObject] = Nil
    ): IO[List[MetadataObject]] = {
      if lockTableItems.isEmpty then IO.pure(allMetadata)
      else {
        val item = lockTableItems.head

        dependencies.metadataBuilder.createMetadata(item).flatMap { metadata =>
          val archiveFolder = metadata.collectFirst { case a: ArchiveFolderMetadataObject => a }
          val potentialExistingFolder = archiveFolders
            .find(folder => archiveFolder.map(_.name).contains(folder.name))
          val updatedMetadata = potentialExistingFolder
            .map { existing =>
              metadata.flatMap {
                case af: ArchiveFolderMetadataObject if af.name == existing.name => None
                case as: AssetMetadataObject                                     => Option(as.copy(parentId = Option(existing.id)))
                case rest                                                        => Option(rest)
              }
            }
            .getOrElse(metadata)

          processItems(lockTableItems.tail, potentialExistingFolder.orElse(archiveFolder).toList ++ archiveFolders, allMetadata ++ updatedMetadata)
        }
      }
    }

    for
      items <- lockTableItems
      allMetadata <- processItems(items)
      metadataUri = URI.create(s"s3://${config.rawCacheBucket}/${input.batchId}/metadata.json")
      _ <- uploadMetadata(allMetadata, metadataUri)
    yield StepFunctionInput(input.batchId, input.groupId, metadataUri, input.retryCount, "")
  }

  override def dependencies(config: Config): IO[Dependencies] = {
    val s3Client = DAS3Client[IO]()
    val metadataBuilder = MetadataBuilder(() => UUID.randomUUID, s3Client, SeriesMapper(), UriProcessor())
    IO.pure(Dependencies(DADynamoDBClient[IO](), s3Client, metadataBuilder))
  }

object Lambda:

  case class Dependencies(dynamoDbClient: DADynamoDBClient[IO], s3Client: DAS3Client[IO], metadataBuilder: MetadataBuilder)

  case class Config(lockTableName: String, lockTableGsiName: String, rawCacheBucket: String) derives ConfigReader

  private type Output = StepFunctionInput

  case class Input(groupId: String, batchId: String, waitFor: Int, retryCount: Int = 0)
