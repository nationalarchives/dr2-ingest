package uk.gov.nationalarchives.preingesttdraggregator

import cats.Monoid
import cats.effect.IO
import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import io.circe.generic.auto.*
import pureconfig.ConfigReader.Result
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource}
import uk.gov.nationalarchives.preingesttdraggregator.Aggregator.*
import uk.gov.nationalarchives.preingesttdraggregator.Ids.GroupId
import uk.gov.nationalarchives.preingesttdraggregator.Lambda.*
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient, DASQSClient}
import uk.gov.nationalarchives.preingesttdraggregator.Duration.*

import java.net.URI
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*
class Lambda extends RequestHandler[SQSEvent, Unit]:

  given Monoid[Map[String, Group]] = Monoid.instance(Map.empty, _ ++ _)

  private val groupCacheAtomicCell: AtomicCell[IO, Map[String, Group]] =
    AtomicCell[IO].empty[Map[String, Group]].unsafeRunSync() // Not ideal but it needs initialising outside the handler function

  given DASFNClient[IO] = DASFNClient[IO]()
  given DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  given DASQSClient[IO] = DASQSClient[IO]()

  override def handleRequest(input: SQSEvent, context: Context): Unit =
    ConfigSource.default
      .loadF[IO, Config]()
      .flatMap { config =>
        Aggregator[IO].aggregate(config, groupCacheAtomicCell, input.getRecords.asScala.toList, context.getRemainingTimeInMillis)
      }
      .unsafeRunSync()

object Lambda:

  case class Group(groupId: GroupId, expires: Instant, itemCount: Int)

  given ConfigReader[Seconds] = (cur: ConfigCursor) => cur.asInt.map(i => i.seconds)

  case class Config(lockTable: String, sourceSystem: String, sfnArn: Option[String], outputQueue: Option[String], maxSecondaryBatchingWindow: Seconds, maxBatchSize: Int) derives ConfigReader
