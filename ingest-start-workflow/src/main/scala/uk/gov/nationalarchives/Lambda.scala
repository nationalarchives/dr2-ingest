package uk.gov.nationalarchives

import cats.effect._
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import org.typelevel.log4cats.{LoggerName, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jFactory
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import uk.gov.nationalarchives.Lambda._
import uk.gov.nationalarchives.dp.client.WorkflowClient
import uk.gov.nationalarchives.dp.client.WorkflowClient.{Parameter, StartWorkflowRequest}
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import upickle.default
import upickle.default._

import java.io.{InputStream, OutputStream}

class Lambda extends RequestStreamHandler {
  private val configIo: IO[Config] = ConfigSource.default.loadF[IO, Config]()
  implicit val loggerName: LoggerName = LoggerName(sys.env("AWS_LAMBDA_FUNCTION_NAME"))
  private val logger: SelfAwareStructuredLogger[IO] = Slf4jFactory.create[IO].getLogger

  lazy val workflowClientIO: IO[WorkflowClient[IO]] = configIo.flatMap { config =>
    Fs2Client.workflowClient(config.apiUrl, config.secretName)
  }

  implicit val inputReader: Reader[Input] = macroR[Input]

  override def handleRequest(inputStream: InputStream, output: OutputStream, context: Context): Unit = {
    val inputString = inputStream.readAllBytes().map(_.toChar).mkString
    val input = read[Input](inputString)
    val batchRef = input.executionId.split("-").take(3).mkString("-")
    val log = logger.info(Map("batchRef" -> batchRef))(_)
    for {
      _ <- log(s"Starting workflow ${input.workflowContextName} for $batchRef")
      workflowClient <- workflowClientIO
      id <- workflowClient.startWorkflow(
        StartWorkflowRequest(
          Some(input.workflowContextName),
          parameters = List(Parameter("OpexContainerDirectory", s"opex/${input.executionId}"))
        )
      )
      _ <- log(s"Workflow ${input.workflowContextName} for $batchRef started")
    } yield output.write(write(StateOutput(id)).getBytes())
  }.onError(logLambdaError).unsafeRunSync()

  private def logLambdaError(error: Throwable): IO[Unit] = logger.error(error)("Error running start workflow")
}

object Lambda {
  implicit val stateDataWriter: default.Writer[StateOutput] = macroW[StateOutput]
  case class Input(workflowContextName: String, executionId: String)
  private case class Config(apiUrl: String, secretName: String)
  case class StateOutput(id: Int)
}
