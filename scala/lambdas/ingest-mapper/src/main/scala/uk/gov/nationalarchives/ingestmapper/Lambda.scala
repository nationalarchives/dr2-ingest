package uk.gov.nationalarchives.ingestmapper

import cats.effect.IO
import io.circe.generic.auto.*
import org.scanamo.*
import org.scanamo.generic.semiauto.*
import pureconfig.generic.derivation.default.*
import pureconfig.ConfigReader
import ujson.*
import upickle.core.*
import uk.gov.nationalarchives.ingestmapper.Lambda.{Config, Dependencies, Input, StateOutput}
import uk.gov.nationalarchives.ingestmapper.MetadataService.*
import uk.gov.nationalarchives.ingestmapper.MetadataService.Type.*

import java.util.UUID
import io.circe.*
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.DADynamoDBClient
import uk.gov.nationalarchives.utils.Generators

import java.net.URI
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
      case s =>
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
      a = metadataJsonWithTtl.find(json => json("id").str == "de4e10bf-33f8-4089-8451-9fc5ed2b5822")
      _ <- dependencies.dynamo.writeItems(config.dynamoTableName, metadataJsonWithTtl)
      _ <- log("Metadata written to dynamo db")
    } yield {
      val typeToId: Map[Type, List[UUID]] = metadataJson
        .groupBy(jsonObj => typeFromString(jsonObj("type").str))
        .view
        .mapValues(_.map(jsonObj => UUID.fromString(jsonObj("id").str)))
        .toMap

      StateOutput(
        input.batchId,
        input.metadataPackage,
        typeToId.getOrElse(ArchiveFolder, Nil),
        typeToId.getOrElse(ContentFolder, Nil),
        typeToId.getOrElse(Asset, Nil)
      )
    }

  override def dependencies(config: Config): IO[Dependencies] = {
    val randomUuidGenerator: () => UUID = () => Generators().generateRandomUuid
    DiscoveryService(config.discoveryApiUrl, randomUuidGenerator).map { discoveryService =>
      val metadataService: MetadataService = MetadataService(discoveryService)
      val dynamo: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
      Dependencies(metadataService, dynamo)
    }
  }
}
object Lambda {
  case class StateOutput(batchId: String, metadataPackage: URI, archiveHierarchyFolders: List[UUID], contentFolders: List[UUID], contentAssets: List[UUID])
  case class Input(batchId: String, metadataPackage: URI)
  case class Config(dynamoTableName: String, discoveryApiUrl: String) derives ConfigReader
  case class Dependencies(metadataService: MetadataService, dynamo: DADynamoDBClient[IO], time: () => Instant = () => Generators().generateInstant)
}
