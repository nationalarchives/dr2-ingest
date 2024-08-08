package uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler

import cats.effect.IO
import cats.implicits.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.circe.Decoder
import io.circe.generic.auto.*
import io.circe.parser.decode
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbWriteItemRequest
import uk.gov.nationalarchives.utils.EventDecoders.given
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.FileProcessor.*
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.Lambda.Dependencies
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.SeriesMapper.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{batchId, ioId, message}
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client, DASFNClient}

import java.net.URI
import java.util.UUID
import scala.jdk.CollectionConverters.*

class Lambda extends LambdaRunner[SQSEvent, Unit, Config, Dependencies] {
  override def handler: (
      SQSEvent,
      Config,
      Dependencies
  ) => IO[Unit] = (input, config, dependencies) => {
    input.getRecords.asScala.toList
      .map { record =>
        for {
          treInput <- IO.fromEither(decode[TREInput](record.getBody))
          batchRef = treInput.parameters.reference
          logCtx = Map("batchRef" -> batchRef)
          _ <- log(logCtx)(s"Processing batchRef $batchRef")

          outputBucket = config.outputBucket
          fileProcessor = new FileProcessor(treInput.parameters.s3Bucket, outputBucket, batchRef, dependencies.s3, dependencies.randomUuidGenerator)
          fileNameToFileInfo <- fileProcessor.copyFilesFromDownloadToUploadBucket(treInput.parameters.s3Key)
          _ <- log(logCtx)(s"Copied ${treInput.parameters.s3Key} from ${treInput.parameters.s3Bucket} to $outputBucket")

          metadataFileInfo <- IO.fromOption(fileNameToFileInfo.get(s"$batchRef/TRE-$batchRef-metadata.json"))(
            new RuntimeException(s"Cannot find metadata for $batchRef")
          )
          treMetadata <- fileProcessor.readJsonFromPackage(metadataFileInfo.id)
          potentialUri = treMetadata.parameters.PARSER.uri
          potentialJudgmentName = treMetadata.parameters.PARSER.name
          uriProcessor = new UriProcessor(potentialUri)
          _ <- uriProcessor.verifyJudgmentNameStartsWithPressSummaryOfIfInUri(potentialJudgmentName)

          parsedUri <- uriProcessor.getCourtAndUriWithoutDocType
          payload = treMetadata.parameters.TRE.payload
          potentialCite = treMetadata.parameters.PARSER.cite
          fileReference = treMetadata.parameters.TDR.`File-Reference`
          logWithFileRef = logger.info(logCtx ++ Map("fileReference" -> fileReference.orNull))(_)

          fileInfo <- IO.fromOption(fileNameToFileInfo.get(s"$batchRef/${payload.filename}"))(
            new RuntimeException(s"Document not found for file belonging to $batchRef")
          )

          _ <- IO.raiseWhen(fileInfo.fileSize == 0)(new Exception(s"File id '${fileInfo.id}' size is 0"))
          metadataPackage = URI.create(s"s3://$outputBucket/$batchRef/metadata.json")
          departmentAndSeries <- dependencies.seriesMapper.createDepartmentAndSeries(
            parsedUri.flatMap(_.potentialCourt),
            treInput.parameters.skipSeriesLookup
          )
          fileInfoWithUpdatedChecksum = fileInfo.copy(checksum = treMetadata.parameters.TDR.`Document-Checksum-sha256`)
          tdrUuid = treMetadata.parameters.TDR.`UUID`.toString
          metadata = fileProcessor.createMetadata(
            fileInfoWithUpdatedChecksum,
            metadataFileInfo,
            parsedUri,
            potentialCite,
            potentialJudgmentName,
            potentialUri,
            treMetadata,
            fileReference,
            departmentAndSeries.potentialDepartment,
            departmentAndSeries.potentialSeries,
            tdrUuid
          )
          _ <- fileProcessor.createMetadataJson(metadata, metadataPackage)
          _ <- logWithFileRef(s"Copied metadata.json to bucket $outputBucket")
          irrelevantFilesFromTre = fileNameToFileInfo.values.collect {
            case fi if !List(fileInfo, metadataFileInfo).contains(fi) => fi.id.toString
          }.toList
          _ <- IO.whenA(irrelevantFilesFromTre.nonEmpty) {
            dependencies.s3.deleteObjects(outputBucket, irrelevantFilesFromTre) >> logWithFileRef("Deleted unused TRE objects from the root of S3")
          }

          _ <- dependencies.dynamo.writeItem(
            DADynamoDbWriteItemRequest(
              config.dynamoLockTableName,
              Map(
                ioId -> toDynamoString(tdrUuid),
                batchId -> toDynamoString(batchRef),
                message -> toDynamoString(s"""{"messageId":"${dependencies.randomUuidGenerator()}"}""")
              ),
              Some(s"attribute_not_exists($ioId)")
            )
          )
          _ <- logWithFileRef("Written asset to lock table")

          _ <- dependencies.sfn.startExecution(config.sfnArn, Output(batchRef, metadataPackage), Option(batchRef))
          _ <- logWithFileRef("Started step function execution")
        } yield ()
      }
      .sequence
      .map(_.head)
  }

  private def toDynamoString(value: String): AttributeValue = AttributeValue.builder.s(value).build

  override def dependencies(config: Config): IO[Dependencies] = IO {
    val s3: DAS3Client[IO] = DAS3Client[IO]()
    val sfn: DASFNClient[IO] = DASFNClient[IO]()
    val dynamo: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
    val randomUuidGenerator: () => UUID = () => UUID.randomUUID
    val seriesMapper: SeriesMapper = SeriesMapper()
    Dependencies(s3, sfn, dynamo, randomUuidGenerator, seriesMapper)
  }
}

object Lambda {
  case class Dependencies(s3: DAS3Client[IO], sfn: DASFNClient[IO], dynamo: DADynamoDBClient[IO], randomUuidGenerator: () => UUID, seriesMapper: SeriesMapper)
}
