package uk.gov.nationalarchives.preingesttdraggregator

import cats.Monoid
import cats.effect.IO
import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.{SQSBatchResponse, SQSEvent}
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import io.circe.generic.auto.*
import pureconfig.ConfigReader.Result
import pureconfig.module.catseffect.syntax.*
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource}
import uk.gov.nationalarchives.preingesttdraggregator.Duration.*
import uk.gov.nationalarchives.preingesttdraggregator.Ids.GroupId
import uk.gov.nationalarchives.preingesttdraggregator.Lambda.*
import uk.gov.nationalarchives.utils.ExternalUtils.given
import uk.gov.nationalarchives.utils.Generators.given
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient}

import java.time.Instant
import scala.jdk.CollectionConverters.*
class Lambda extends RequestHandler[SQSEvent, SQSBatchResponse]:
  given Monoid[Map[String, Group]] = Monoid.instance(Map.empty, _ ++ _)

  private val groupCacheAtomicCell: AtomicCell[IO, Map[String, Group]] =
    AtomicCell[IO].empty[Map[String, Group]].unsafeRunSync() // Not ideal but it needs initialising outside the handler function

  override def handleRequest(input: SQSEvent, context: Context): SQSBatchResponse = {
    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      batchResponse <- run(Aggregator[IO](DASFNClient[IO](), DADynamoDBClient[IO]()), input, context, config)
    } yield batchResponse
  }.unsafeRunSync()

  def run(aggregator: Aggregator[IO], input: SQSEvent, context: Context, config: Config): IO[SQSBatchResponse] = {
    for {
      potentialFailures <- aggregator.aggregate(config, groupCacheAtomicCell, input.getRecords.asScala.toList, context.getRemainingTimeInMillis)
    } yield SQSBatchResponse.builder().withBatchItemFailures(potentialFailures.asJava).build
  }

object Lambda:

  case class Group(groupId: GroupId, expires: Instant, itemCount: Int)

  given ConfigReader[Seconds] = (cur: ConfigCursor) => cur.asInt.map(i => i.seconds)

  case class Config(lockTable: String, sourceSystem: String, sfnArn: String, maxSecondaryBatchingWindow: Seconds, maxBatchSize: Int) derives ConfigReader
