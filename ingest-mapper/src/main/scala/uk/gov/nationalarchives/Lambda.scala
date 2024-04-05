package uk.gov.nationalarchives

import cats.effect.IO
import io.circe.generic.auto._
import org.scanamo._
import org.scanamo.generic.semiauto._
import pureconfig.generic.derivation.default._
import pureconfig.ConfigReader
import ujson._
import upickle.core._
import uk.gov.nationalarchives.Lambda.{Config, Dependencies, Input, StateOutput}
import uk.gov.nationalarchives.MetadataService._
import java.util.UUID
import io.circe.*

class Lambda extends LambdaRunner[Input, StateOutput, Config, Dependencies] {

  implicit val dynamoTableFormat: Typeclass[Obj] = new Typeclass[Obj] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, Obj] = {
      dynamoValue.asObject
        .map(_.toMap[String].map { valuesMap =>
          val jsonValuesMap = valuesMap.view.mapValues(Str.apply).toList
          Obj(LinkedHashMap(jsonValuesMap))
        })
        .getOrElse(Left(TypeCoercionError(new Exception("Dynamo object not found"))))
    }

    override def write(jsonObject: Obj): DynamoValue = {
      val dynamoValuesMap: Map[String, DynamoValue] = jsonObject.value.toMap.view
        .filterNot { case (_, value) => value.isNull }
        .mapValues(processDynamoValue)
        .toMap
      DynamoValue.fromDynamoObject(DynamoObject(dynamoValuesMap))
    }
  }

  private def processDynamoValue(dynamoValue: Value): DynamoValue = {
    dynamoValue match {
      case Num(value) =>
        DynamoValue.fromNumber[Long](value.toLong)
      case Arr(arr) => DynamoValue.fromDynamoArray(DynamoArray(arr.map(processDynamoValue).toList))
      case s =>
        DynamoValue.fromString(s.str)

    }
  }

  override def handler: (
      Input,
      Config,
      Dependencies
  ) => IO[StateOutput] = (input, config, dependencies) =>
    for {

      log <- IO(log(Map("batchRef" -> input.batchId)))
      _ <- log(s"Processing batchRef ${input.batchId}")

      discoveryService <- DiscoveryService(config.discoveryApiUrl, dependencies.uuidGenerator)
      departmentAndSeries <- discoveryService.getDepartmentAndSeriesRows(input)
      _ <- log(s"Retrieved department and series ${departmentAndSeries.show}")

      bagManifests <- dependencies.metadataService.parseBagManifest(input)
      bagInfoJson <- dependencies.metadataService.parseBagInfoJson(input)
      metadataJson <- dependencies.metadataService.parseMetadataJson(input, departmentAndSeries, bagManifests, bagInfoJson.headOption.getOrElse(Obj()))
      _ <- dependencies.dynamo.writeItems(config.dynamoTableName, metadataJson)
      _ <- log("Metadata written to dynamo db")
    } yield {

      val typeToId: Map[Type, List[UUID]] = metadataJson
        .groupBy(jsonObj => typeFromString(jsonObj("type").str))
        .view
        .mapValues(_.map(jsonObj => UUID.fromString(jsonObj("id").str)))
        .toMap

      StateOutput(
        input.batchId,
        input.s3Bucket,
        input.s3Prefix,
        typeToId.getOrElse(ArchiveFolder, Nil),
        typeToId.getOrElse(ContentFolder, Nil),
        typeToId.getOrElse(Asset, Nil)
      )
    }

  override def dependencies(config: Config): IO[Dependencies] = {
    val metadataService: MetadataService = MetadataService()
    val dynamo: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
    val randomUuidGenerator: () => UUID = () => UUID.randomUUID()
    IO(Dependencies(metadataService, dynamo, randomUuidGenerator))
  }
}
object Lambda {
  case class StateOutput(batchId: String, s3Bucket: String, s3Prefix: String, archiveHierarchyFolders: List[UUID], contentFolders: List[UUID], contentAssets: List[UUID])
  case class Input(batchId: String, s3Bucket: String, s3Prefix: String, department: Option[String], series: Option[String])
  case class Config(dynamoTableName: String, discoveryApiUrl: String) derives ConfigReader
  case class Dependencies(metadataService: MetadataService, dynamo: DADynamoDBClient[IO], uuidGenerator: () => UUID)
}
