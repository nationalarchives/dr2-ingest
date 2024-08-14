package uk.gov.nationalarchives.eventaggregator

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*
import pureconfig.{ConfigReader, ConfigSource}
import uk.gov.nationalarchives.eventaggregator.Lambda.*
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient}
import io.circe.generic.auto.*
import java.net.URI
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*
class Lambda extends RequestHandler[SQSEvent, Unit]:

  private val ref: Ref[IO, Map[String, Batch]] = Ref.unsafe[IO, Map[String, Batch]](Map[String, Batch]())

  given DASFNClient[IO] = DASFNClient[IO]()
  given DADynamoDBClient[IO] = DADynamoDBClient[IO]()

  override def handleRequest(input: SQSEvent, context: Context): Unit =
    ConfigSource.default
      .loadF[IO, Config]()
      .flatMap { config =>
        Aggregator[IO].aggregate(config, ref, input.getRecords.asScala.toList, context.getRemainingTimeInMillis)
      }
      .unsafeRunSync()

object Lambda:

  case class Batch(batchId: String, expires: Instant, items: Int)

  case class Config(lockTable: String, outputQueueUrl: String, sourceSystem: String, sfnArn: String, maxSecondaryBatchingWindow: Int, maxBatchSize: Int) derives ConfigReader
