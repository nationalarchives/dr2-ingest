package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import io.circe.generic.auto._
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import uk.gov.nationalarchives.FileProcessor._
import upickle.default._

import java.util.UUID
import scala.jdk.CollectionConverters._

class Lambda extends RequestHandler[SQSEvent, Unit] {
  val s3: DAS3Client[IO] = DAS3Client[IO]()
  val sfn: DASFNClient[IO] = DASFNClient[IO]()
  val randomUuidGenerator: () => UUID = () => UUID.randomUUID
  val seriesMapper: SeriesMapper = SeriesMapper()

  override def handleRequest(input: SQSEvent, context: Context): Unit = {
    input.getRecords.asScala.toList.map { record =>
      val treInput = read[TREInput](record.getBody)
      val inputBucket = treInput.parameters.s3Bucket
      val batchRef = treInput.parameters.reference

      for {
        config <- ConfigSource.default.loadF[IO, Config]()
        outputBucket = config.outputBucket
        fileProcessor = new FileProcessor(inputBucket, outputBucket, batchRef, s3, randomUuidGenerator)
        fileNameToFileInfo <- fileProcessor.copyFilesFromDownloadToUploadBucket(treInput.parameters.s3Key)

        metadataFileInfo <- IO.fromOption(fileNameToFileInfo.get(s"$batchRef/TRE-$batchRef-metadata.json"))(
          new RuntimeException(s"Cannot find metadata for $batchRef")
        )
        treMetadata <- fileProcessor.readJsonFromPackage(metadataFileInfo.id)
        payload = treMetadata.parameters.TRE.payload
        cite = treMetadata.parameters.PARSER.cite
        fileInfo <- IO.fromOption(fileNameToFileInfo.get(s"$batchRef/${payload.filename}"))(
          new RuntimeException(s"Document not found for file belonging to $batchRef")
        )
        output <- seriesMapper.createOutput(config.outputBucket, batchRef, cite)
        _ <- fileProcessor.createMetadataFiles(
          fileInfo.copy(checksum = payload.sha256),
          metadataFileInfo,
          cite.getOrElse("Court Documents"),
          output.department,
          output.series
        )
        _ <- s3.copy(outputBucket, fileInfo.id.toString, outputBucket, s"$batchRef/data/${fileInfo.id}")
        _ <- s3
          .copy(outputBucket, metadataFileInfo.id.toString, outputBucket, s"$batchRef/data/${metadataFileInfo.id}")

        _ <- sfn.startExecution(config.sfnArn, output, Option(s"$batchRef-${randomUuidGenerator()}"))
      } yield ()
    }.sequence
  }.unsafeRunSync()
}
