package uk.gov.nationalarchives.utils

import cats.effect._
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import io.circe.{Decoder, Encoder, Printer}
import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import org.typelevel.log4cats.{LoggerName, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jFactory
import pureconfig._
import pureconfig.module.catseffect.syntax._
import java.io.{InputStream, OutputStream}
import scala.reflect.ClassTag

abstract class LambdaRunner[Event, Result, Config, Dependencies](using
    val decoder: Decoder[Event],
    val configReader: ConfigReader[Config],
    val classTag: ClassTag[Config],
    val encoder: Encoder[Result]
) extends RequestStreamHandler {

  val lambdaName: String = sys.env("AWS_LAMBDA_FUNCTION_NAME")
  given LoggerName = LoggerName(lambdaName)
  val logger: SelfAwareStructuredLogger[IO] = Slf4jFactory.create[IO].getLogger
  def log(logCtx: Map[String, String]): String => IO[Unit] = {
    logger.info(logCtx)(_)
  }
  def handler: (Event, Config, Dependencies) => IO[Result]
  def dependencies(config: Config): IO[Dependencies]

  override def handleRequest(input: InputStream, output: OutputStream, context: Context): Unit = (for {
    inputString <- Resource
      .fromAutoCloseable(IO(input))
      .use { is =>
        IO(is.readAllBytes().map(_.toChar).mkString)
      }
    config <- ConfigSource.default.loadF[IO, Config]()
    event <- IO.fromEither(decode[Event](inputString))
    deps <- dependencies(config)
    response <- handler(event, config, deps)
    _ <- response match {
      case _: Unit => IO.unit
      case _       =>
        Resource
          .fromAutoCloseable(IO(output))
          .use { os =>
            IO(os.write(response.asJson.printWith(Printer.noSpaces).getBytes))
          }
    }
  } yield ()).onError(logLambdaError).unsafeRunSync()

  private def logLambdaError: PartialFunction[Throwable, IO[Unit]] =
    case error => logger.error(error)(s"Error running $lambdaName")
}
