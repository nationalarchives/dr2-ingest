package uk.gov.nationalarchives.preingesttdraggregator

import cats.Monoid
import cats.effect.IO
import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.{SQSBatchResponse, SQSEvent}
import com.amazonaws.services.lambda.runtime.Context
import io.circe.generic.auto.*
import org.encalmo.lambda.{LambdaContext, LambdaEnvironment, LambdaRuntime}
import pureconfig.ConfigReader.Result
import pureconfig.module.catseffect.syntax.*
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource}
import uk.gov.nationalarchives.preingesttdraggregator.Duration.*
import uk.gov.nationalarchives.preingesttdraggregator.Ids.GroupId
import uk.gov.nationalarchives.preingesttdraggregator.Lambda.*
import uk.gov.nationalarchives.utils.Generators.given
import uk.gov.nationalarchives.utils.EventCodecs.given
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient}
import io.circe.parser.decode
import io.circe.syntax.*
import java.net.URI
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.annotation.static

class Lambda extends LambdaRuntime:

  given Monoid[Map[String, Group]] = Monoid.instance(Map.empty, _ ++ _)

  private val groupCacheAtomicCell: AtomicCell[IO, Map[String, Group]] =
    AtomicCell[IO].empty[Map[String, Group]].unsafeRunSync() // Not ideal but it needs initialising outside the handler function


  def run(aggregator: Aggregator[IO], input: SQSEvent, context: Context, config: Config): IO[SQSBatchResponse] = {
    for {
      potentialFailures <- aggregator.aggregate(config, groupCacheAtomicCell, input.getRecords.asScala.toList, context.getRemainingTimeInMillis)
    } yield SQSBatchResponse.builder().withBatchItemFailures(potentialFailures.asJava).build
  }

  override type ApplicationContext = Unit

  override def initialize(using LambdaEnvironment): Unit = ()

  override def handleRequest(input: String)(using ctx: LambdaContext, ignored: Unit): String = {
    for {
      input <- IO.fromEither(decode[SQSEvent](input))
      config <- ConfigSource.default.loadF[IO, Config]()
      batchResponse <- run(Aggregator[IO](DASFNClient[IO](), DADynamoDBClient[IO]()), input, ctx, config)
    } yield batchResponse.asJson.noSpaces
  }.unsafeRunSync()


object Lambda:
  @static def main(args: Array[String]): Unit = new Lambda().run()

  case class Group(groupId: GroupId, expires: Instant, itemCount: Int)

  given ConfigReader[Seconds] = (cur: ConfigCursor) => cur.asInt.map(i => i.seconds)

  case class Config(lockTable: String, sourceSystem: String, sfnArn: String, maxSecondaryBatchingWindow: Seconds, maxBatchSize: Int) derives ConfigReader
  case class Dependencies()
