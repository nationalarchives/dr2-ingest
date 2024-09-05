package uk.gov.nationalarchives.preingesttdraggregator

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource}
import uk.gov.nationalarchives.preingesttdraggregator.Lambda.*
import uk.gov.nationalarchives.preingesttdraggregator.Aggregator.*
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient}
import io.circe.generic.auto.*
import pureconfig.ConfigReader.Result
import uk.gov.nationalarchives.preingesttdraggregator.Duration.Seconds
import uk.gov.nationalarchives.preingesttdraggregator.Ids.GroupId

import java.net.URI
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*
class Lambda extends RequestHandler[SQSEvent, Unit]:

  private val groupCacheRef: Ref[IO, Map[String, Group]] = Ref.unsafe[IO, Map[String, Group]](Map[String, Group]())

  given DASFNClient[IO] = DASFNClient[IO]()
  given DADynamoDBClient[IO] = DADynamoDBClient[IO]()

  override def handleRequest(input: SQSEvent, context: Context): Unit =
    ConfigSource.default
      .loadF[IO, Config]()
      .flatMap { config =>
        Aggregator[IO].aggregate(config, groupCacheRef, input.getRecords.asScala.toList, context.getRemainingTimeInMillis)
      }
      .unsafeRunSync()

object Lambda:

  case class Group(groupId: GroupId, expires: Instant, itemCount: Int)

  given ConfigReader[Seconds] = (cur: ConfigCursor) => cur.asInt.map(i => i.seconds)

  case class Config(lockTable: String, sourceSystem: String, sfnArn: String, maxSecondaryBatchingWindow: Seconds, maxBatchSize: Int) derives ConfigReader
