package uk.gov.nationalarchives

import cats.effect._
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import uk.gov.nationalarchives.Lambda._
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient.{GetMonitorsRequest, Ingest}
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import upickle.default
import upickle.default._

import java.io.{InputStream, OutputStream}

class Lambda extends RequestStreamHandler {
  private val configIo: IO[Config] = ConfigSource.default.loadF[IO, Config]()

  lazy val processMonitorClientIO: IO[ProcessMonitorClient[IO]] = configIo.flatMap { config =>
    Fs2Client.processMonitorClient(config.apiUrl, config.secretName)
  }

  implicit val inputReader: Reader[Input] = macroR[Input]

  override def handleRequest(inputStream: InputStream, output: OutputStream, context: Context): Unit = {
    val inputString = inputStream.readAllBytes().map(_.toChar).mkString
    val input = read[Input](inputString)
    for {
      processMonitorClient <- processMonitorClientIO
      monitors <- processMonitorClient.getMonitors(
        GetMonitorsRequest(name = Some(s"opex/${input.executionId}"), category = List(Ingest))
      )
      monitor = monitors.head
    } yield output.write(write(StateOutput(monitor.status, monitor.mappedId)).getBytes())
  }.unsafeRunSync()
}

object Lambda {
  implicit val stateDataWriter: default.Writer[StateOutput] = macroW[StateOutput]
  case class Input(executionId: String)
  private case class Config(apiUrl: String, secretName: String)
  case class StateOutput(status: String, mappedId: String)
}
