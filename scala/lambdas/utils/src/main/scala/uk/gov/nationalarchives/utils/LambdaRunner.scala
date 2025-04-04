package uk.gov.nationalarchives.utils

import cats.effect.*
import cats.effect.unsafe.implicits.global
import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import org.encalmo.lambda.{LambdaContext, LambdaEnvironment, LambdaRuntime}
import org.slf4j
import org.slf4j.{LoggerFactory, MDC}
import pureconfig.*
import pureconfig.error.ConfigReaderFailures
import uk.gov.nationalarchives.utils.LambdaRunner.AppContext

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

trait LambdaRunner[Event, Result, Config, Dependencies](using
                                                        val decoder: Decoder[Event],
                                                        val configReader: ConfigReader[Config],
                                                        val classTag: ClassTag[Config],
                                                        val encoder: Encoder[Result]
                                                       ) extends LambdaRuntime {

  val lambdaName: String = sys.env("AWS_LAMBDA_FUNCTION_NAME")
  val logger: slf4j.Logger = LoggerFactory.getLogger(lambdaName)

  def log(logCtx: Map[String, String]): String => IO[Unit] = message => IO {
    MDC.setContextMap(logCtx.asJava)
    logger.info(message)
    MDC.clear()
  }

  def handler: (Event, Config, Dependencies) => IO[Result]

  def dependencies(config: Config): IO[Dependencies]

  override type ApplicationContext = AppContext[Config, Dependencies]

  override def initialize(using env: LambdaEnvironment): ApplicationContext = {
    ConfigSource.default.load[Config]() match
      case Left(errors) => throw RuntimeException(errors.prettyPrint())
      case Right(config) => AppContext(config, dependencies(config).unsafeRunSync())
  }

  override def handleRequest(input: String)(using context: LambdaContext, appContext: AppContext[Config, Dependencies]): String =
    val inputStream = new ByteArrayInputStream(input.getBytes)
    val outputStream = new ByteArrayOutputStream()
    runLambda(input, appContext).asJson.noSpaces

  def runLambda(input: String, appContext: AppContext[Config, Dependencies]): Result = (for {
    event <- IO.fromEither(decode[Event](input))
    response <- handler(event, appContext.config, appContext.dependencies)
  } yield response).unsafeRunSync()

  private def logLambdaError(throwable: Throwable)(using context: LambdaContext): IO[Unit] =
    IO(logger.error(s"Error running $lambdaName", throwable))
}
object LambdaRunner {
  case class AppContext[Config, Dependencies](config: Config, dependencies: Dependencies)
}