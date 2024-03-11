package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import org.scanamo.generic.semiauto._
import org.scanamo._
import org.typelevel.log4cats.{LoggerName, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jFactory
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import ujson.{Arr, Null, Num, Obj, Str, Value}
import uk.gov.nationalarchives.Lambda.{Config, Input, StateOutput}
import uk.gov.nationalarchives.MetadataService._
import upickle.default
import upickle.default._

import java.io.{InputStream, OutputStream}
import java.util.UUID
import scala.collection.mutable

class Lambda extends RequestStreamHandler {
  val metadataService: MetadataService = MetadataService()
  val dynamo: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  val randomUuidGenerator: () => UUID = () => UUID.randomUUID()

  implicit val loggerName: LoggerName = LoggerName("Ingest Mapper")
  private val logger: SelfAwareStructuredLogger[IO] = Slf4jFactory.create[IO].getLogger
  implicit val inputReader: Reader[Input] = macroR[Input]

  implicit def OptionReader[T: Reader]: Reader[Option[T]] = reader[Value].map[Option[T]] {
    case Null    => None
    case jsValue => Some(read[T](jsValue))
  }

  implicit def OptionWriter[T: Writer]: Writer[Option[T]] = writer[Value].comap {
    case Some(value) => write(value)
    case None        => Null
  }

  implicit val dynamoTableFormat: Typeclass[Obj] = new Typeclass[Obj] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, Obj] = {
      dynamoValue.asObject
        .map(_.toMap[String].map { valuesMap =>
          val jsonValuesMap = valuesMap.view.mapValues(Str)
          Obj(mutable.LinkedHashMap.newBuilder[String, Value].addAll(jsonValuesMap).result())
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

  private def parseInput(inputStream: InputStream): IO[Input] = IO {
    val inputString = inputStream.readAllBytes().map(_.toChar).mkString
    read[Input](inputString)
  }

  override def handleRequest(inputStream: InputStream, outputStream: OutputStream, context: Context): Unit = {
    for {
      input <- parseInput(inputStream)
      config <- ConfigSource.default.loadF[IO, Config]()
      logCtx = Map("batchRef" -> input.batchId)
      log = logger.info(logCtx)(_)
      _ <- log(s"Processing batchRef ${input.batchId}")

      discoveryService <- DiscoveryService(config.discoveryApiUrl, randomUuidGenerator)
      departmentAndSeries <- discoveryService.getDepartmentAndSeriesRows(input)
      _ <- log(s"Retrieved department and series ${departmentAndSeries.show}")

      bagManifests <- metadataService.parseBagManifest(input)
      bagInfoJson <- metadataService.parseBagInfoJson(input)
      metadataJson <- metadataService.parseMetadataJson(input, departmentAndSeries, bagManifests, bagInfoJson.headOption.getOrElse(Obj()))
      _ <- dynamo.writeItems(config.dynamoTableName, metadataJson)
      _ <- log("Metadata written to dynamo db")
    } yield {

      val typeToId: Map[Type, List[UUID]] = metadataJson
        .groupBy(jsonObj => typeFromString(jsonObj("type").str))
        .view
        .mapValues(_.map(jsonObj => UUID.fromString(jsonObj("id").str)))
        .toMap

      val stateData = StateOutput(
        input.batchId,
        input.s3Bucket,
        input.s3Prefix,
        typeToId.getOrElse(ArchiveFolder, Nil),
        typeToId.getOrElse(ContentFolder, Nil),
        typeToId.getOrElse(Asset, Nil)
      )
      outputStream.write(write(stateData).getBytes())
    }
  }.onError(logLambdaError).unsafeRunSync()

  private def logLambdaError(error: Throwable): IO[Unit] = logger.error(error)("Error running ingest mapper")

}
object Lambda {
  implicit val stateDataWriter: default.Writer[StateOutput] = macroW[StateOutput]
  case class StateOutput(batchId: String, s3Bucket: String, s3Prefix: String, archiveHierarchyFolders: List[UUID], contentFolders: List[UUID], contentAssets: List[UUID])
  case class Input(batchId: String, s3Bucket: String, s3Prefix: String, department: Option[String], series: Option[String])
  case class Config(dynamoTableName: String, discoveryApiUrl: String)
}
