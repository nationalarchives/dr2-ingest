package uk.gov.nationalarchives

import cats.effect.IO
import cats.implicits.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.circe.Decoder
import io.circe.generic.auto.*
import io.circe.parser.decode
import org.scanamo.{DynamoFormat, DynamoObject, DynamoReadError, DynamoValue}
import pureconfig.*
import pureconfig.module.catseffect.syntax.*
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import uk.gov.nationalarchives.DADynamoDBClient.DADynamoDbWriteItemRequest
import uk.gov.nationalarchives.EventDecoders.given
import uk.gov.nationalarchives.FileProcessor.*
import uk.gov.nationalarchives.Lambda.{Dependencies, DynamoItems}

import java.util.UUID
import scala.jdk.CollectionConverters.*

class Lambda extends LambdaRunner[SQSEvent, Unit, Config, Dependencies] {

  given DynamoFormat[DynamoItems] = new DynamoFormat[DynamoItems] {
    // We're not using read but we have to have this overridden
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, DynamoItems] = Right(DynamoItems("", "", ""))

    override def write(items: DynamoItems): DynamoValue = {
      val valuesAsDynamoValues = items.productIterator.map(value => DynamoValue.fromString(value.toString))
      val dynamoValuesMap: Map[String, DynamoValue] = (items.productElementNames zip valuesAsDynamoValues).toMap
      DynamoValue.fromDynamoObject(DynamoObject(dynamoValuesMap))
    }
  }

  override def handler: (
      SQSEvent,
      Config,
      Dependencies
  ) => IO[Unit] = (input, _, dependencies) => {
    input.getRecords.asScala.toList
      .map { record =>
        for {
          treInput <- IO.fromEither(decode[TREInput](record.getBody))
          batchRef = treInput.parameters.reference
          logCtx = Map("batchRef" -> batchRef)
          _ <- log(logCtx)(s"Processing batchRef $batchRef")

          config <- ConfigSource.default.loadF[IO, Config]()
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
          output <- dependencies.seriesMapper.createOutput(
            config.outputBucket,
            batchRef,
            parsedUri.flatMap(_.potentialCourt),
            treInput.parameters.skipSeriesLookup
          )
          fileInfoWithUpdatedChecksum = fileInfo.copy(checksum = treMetadata.parameters.TDR.`Document-Checksum-sha256`)
          tdrUuid = treMetadata.parameters.TDR.`UUID`.toString
          bagitMetadata = fileProcessor.createBagitMetadataObjects(
            fileInfoWithUpdatedChecksum,
            metadataFileInfo,
            parsedUri,
            potentialCite,
            potentialJudgmentName,
            potentialUri,
            treMetadata.parameters.TRE.reference,
            fileReference,
            output.department,
            output.series,
            tdrUuid
          )
          _ <- fileProcessor.createBagitFiles(
            bagitMetadata,
            fileInfoWithUpdatedChecksum,
            metadataFileInfo,
            treMetadata,
            output.department,
            output.series
          )
          _ <- logWithFileRef(s"Copied bagit files to $outputBucket")

          _ <- dependencies.s3.copy(outputBucket, fileInfo.id.toString, outputBucket, s"$batchRef/data/${fileInfo.id}")
          _ <- logWithFileRef(s"Copied file with id ${fileInfo.id} to data directory")

          _ <- dependencies.s3
            .copy(outputBucket, metadataFileInfo.id.toString, outputBucket, s"$batchRef/data/${metadataFileInfo.id}")
          _ <- logWithFileRef(s"Copied metadata file with id ${metadataFileInfo.id} to data directory")

          _ <- dependencies.s3.deleteObjects(outputBucket, fileNameToFileInfo.values.map(_.id.toString).toList)
          _ <- logWithFileRef("Deleted objects from the root of S3")

          _ <- dependencies.dynamo.writeItem(
            DADynamoDbWriteItemRequest(
              config.dynamoLockTableName,
              Map(
                "ioId" -> toDynamoString(tdrUuid),
                "batchId" -> toDynamoString(batchRef),
                "message" -> toDynamoString(s"""{"messageId":"${dependencies.randomUuidGenerator()}"}""")
              ),
              Some(s"attribute_not_exists($tdrUuid)")
            )
          )

          _ <- dependencies.sfn.startExecution(config.sfnArn, output, Option(batchRef))
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
  case class DynamoItems(ioId: String, batchId: String, message: String)
}
