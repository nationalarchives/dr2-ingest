package uk.gov.nationalarchives.ingestmapper

import cats.effect.IO
import cats.syntax.traverse.*
import io.circe.syntax.*
import fs2.Stream
import io.circe.generic.auto.*
import org.scanamo.*
import org.scanamo.generic.semiauto.*
import pureconfig.ConfigReader
import ujson.*
import upickle.core.*
import uk.gov.nationalarchives.ingestmapper.Lambda.{BucketInfo, Config, Dependencies, Input, StateOutput}
import uk.gov.nationalarchives.ingestmapper.MetadataService.*
import uk.gov.nationalarchives.ingestmapper.MetadataService.Type.*

import java.util.UUID
import io.circe.*
import org.reactivestreams.FlowAdapters
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.utils.ExternalUtils.given
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import uk.gov.nationalarchives.utils.Generators

import java.net.URI
import java.nio.ByteBuffer
import java.time.Instant
import java.time.temporal.ChronoUnit

class Lambda extends LambdaRunner[Input, StateOutput, Config, Dependencies] {

  given Typeclass[Obj] = new Typeclass[Obj] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, Obj] =
      dynamoValue.asObject
        .map(_.toMap[String].map { valuesMap =>
          val jsonValuesMap = valuesMap.view.mapValues(Str.apply).toList
          Obj(LinkedHashMap(jsonValuesMap))
        })
        .getOrElse(Left(TypeCoercionError(new Exception("Dynamo object not found"))))

    override def write(jsonObject: Obj): DynamoValue = {
      val dynamoValuesMap: Map[String, DynamoValue] = jsonObject.value.toMap.view
        .filterNot { case (_, value) => value.isNull }
        .mapValues(processDynamoValue)
        .toMap
      DynamoValue.fromDynamoObject(DynamoObject(dynamoValuesMap))
    }
  }

  private def processDynamoValue(dynamoValue: Value): DynamoValue =
    dynamoValue match {
      case Num(value) =>
        DynamoValue.fromNumber[Long](value.toLong)
      case Arr(arr) => DynamoValue.fromDynamoArray(DynamoArray(arr.map(processDynamoValue).toList))
      case s        =>
        DynamoValue.fromString(s.str)
    }

  override def handler: (
      Input,
      Config,
      Dependencies
  ) => IO[StateOutput] = (input, config, dependencies) =>
    for {
      log <- IO(log(Map("batchRef" -> input.batchId)))
      _ <- log(s"Processing batchRef ${input.batchId}")

      hundredDaysFromNowInEpochSecs <- IO {
        val hundredDaysFromNow: Instant = dependencies.time().plus(100, ChronoUnit.DAYS)
        Num(hundredDaysFromNow.getEpochSecond.toDouble)
      }

      metadataJson <- dependencies.metadataService.parseMetadataJson(input)
      metadataJsonWithTtl <- IO(metadataJson.map(obj => Obj.from(obj.value ++ Map("ttl" -> hundredDaysFromNowInEpochSecs))))
      _ <- dependencies.dynamo.writeItems(config.dynamoTableName, metadataJsonWithTtl)
      _ <- log("Metadata written to dynamo db")
      typeToIds: Map[Type, List[UUID]] = metadataJson
        .groupBy(jsonObj => typeFromString(jsonObj("type").str))
        .view
        .mapValues(_.map(jsonObj => UUID.fromString(jsonObj("id").str)))
        .toMap
      uploadFileToS3 = uploadIdsToS3(dependencies.s3, config.ingestStateBucket, input.executionName)
      archiveFolderIds = typeToIds.getOrElse(ArchiveFolder, Nil)
      contentFolderIds = typeToIds.getOrElse(ContentFolder, Nil)
      folderIds = contentFolderIds ::: archiveFolderIds
      assetIds = typeToIds.getOrElse(Asset, Nil)
      jsonFileNamesAndIds = List(("folders", folderIds), ("assets", assetIds))
      bucketInfo <- jsonFileNamesAndIds.traverse((fileName, ids) => uploadFileToS3(fileName, ids))
      _ <- log("ids written to json files and uploaded to S3")
    } yield StateOutput(
      input.groupId,
      input.batchId,
      input.metadataPackage,
      bucketInfo(1),
      bucketInfo.head,
      archiveFolderIds
    )

  override def dependencies(config: Config): IO[Dependencies] = {
    val randomUuidGenerator: () => UUID = () => Generators().generateRandomUuid
    DiscoveryService[IO](config.discoveryApiUrl, randomUuidGenerator).map { discoveryService =>
      val metadataService: MetadataService = MetadataService(discoveryService)
      val dynamo: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
      val s3: DAS3Client[IO] = DAS3Client[IO]()
      Dependencies(metadataService, dynamo, s3)
    }
  }
  private def uploadIdsToS3(s3Client: DAS3Client[IO], bucketName: String, executionName: String)(jsonFileName: String, ids: List[UUID]) = {
    val key = s"$executionName/$jsonFileName.json"
    Stream
      .eval(IO.pure(ids.asJson.noSpaces))
      .map(s => ByteBuffer.wrap(s.getBytes()))
      .toPublisherResource
      .use(pub => s3Client.upload(bucketName, key, FlowAdapters.toPublisher(pub)))
      .map(_ => BucketInfo(bucketName, key))
  }
}
object Lambda {
  case class BucketInfo(bucket: String, key: String)
  case class StateOutput(
      groupId: String,
      batchId: String,
      metadataPackage: URI,
      assets: BucketInfo,
      folders: BucketInfo,
      archiveHierarchyFolders: List[UUID]
  )
  case class Input(groupId: String, batchId: String, metadataPackage: URI, executionName: String)
  case class Config(dynamoTableName: String, discoveryApiUrl: String, ingestStateBucket: String) derives ConfigReader
  case class Dependencies(metadataService: MetadataService, dynamo: DADynamoDBClient[IO], s3: DAS3Client[IO], time: () => Instant = () => Generators().generateInstant)
}
