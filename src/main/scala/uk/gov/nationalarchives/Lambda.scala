package uk.gov.nationalarchives

import cats.effect._
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import uk.gov.nationalarchives.Lambda._
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient
import uk.gov.nationalarchives.dp.client.ProcessMonitorClient.{
  Error,
  GetMessagesRequest,
  GetMonitorsRequest,
  Info,
  Ingest,
  Pending,
  Running,
  Warning
}
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import upickle.default
import upickle.default._

import java.io.{InputStream, OutputStream}
import java.util.UUID

class Lambda extends RequestStreamHandler {
  lazy val processMonitorClientIO: IO[ProcessMonitorClient[IO]] = configIo.flatMap { config =>
    Fs2Client.processMonitorClient(config.apiUrl, config.secretName)
  }
  private val opexSuccessMessage = "monitor.info.successful.ingest.with.opex"
  private val inProgressStatuses = List(Pending.toString(), Running.toString())
  private val mappedStatuses = Map(
    "Running" -> "Running",
    "Pending" -> "Running",
    "Suspended" -> "Succeeded",
    "Recoverable" -> "Succeeded",
    "Succeeded" -> "Succeeded",
    "Failed" -> "Failed"
  )
  private val configIo: IO[Config] = ConfigSource.default.loadF[IO, Config]()

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
      monitorStatus <- IO.fromOption(mappedStatuses.get(monitor.status))(
        new Exception(s"'${monitor.status}' is an unexpected status!")
      )
      assetsIds <-
        if (inProgressStatuses.contains(monitorStatus)) IO.pure(AssetIds(Nil, Nil, Nil))
        else
          for {
            monitorMessages <- processMonitorClient.getMessages(
              GetMessagesRequest(List(monitor.mappedId), List(Info, Warning, Error))
            )

            pathsOfAssetsIngestedWithOpex = monitorMessages.collect {
              case monitorMessage if monitorMessage.message == opexSuccessMessage => monitorMessage.path
            }

            _ <- pathsOfAssetsIngestedWithOpex
              .map(path =>
                IO.raiseWhen(!path.endsWith(".pax"))(new Exception("There is no pax file at the end of this path!"))
              )
              .sequence

            idsOfIngestedAssets = pathsOfAssetsIngestedWithOpex.map { path =>
              val splitPath = path.split("/")
              val paxFileNameForAsset = splitPath.last
              UUID.fromString(paxFileNameForAsset.stripSuffix(".pax"))
            }

            allAssetIds = input.contentAssets.map(UUID.fromString).toList

            (succeededAssetIds, failedAssetIds) = allAssetIds.partition(idsOfIngestedAssets.contains)
            duplicatedAssetIds = Nil // once we know how to find/extract duplicate assets, we'll add the implementation
          } yield AssetIds(succeededAssetIds, failedAssetIds, duplicatedAssetIds)
    } yield output.write(
      write(
        StateOutput(
          monitorStatus,
          monitor.mappedId,
          assetsIds.succeededAssets,
          assetsIds.failedAssets,
          assetsIds.duplicatedAssets
        )
      ).getBytes()
    )
  }.unsafeRunSync()
}

object Lambda {
  implicit val stateDataWriter: default.Writer[StateOutput] = macroW[StateOutput]
  case class Input(executionId: String, contentAssets: Seq[String])
  case class AssetIds(succeededAssets: List[UUID], failedAssets: List[UUID], duplicatedAssets: List[UUID])

  case class StateOutput(
      status: String,
      mappedId: String,
      succeededAssets: List[UUID],
      failedAssets: List[UUID],
      duplicatedAssets: List[UUID]
  )

  private case class Config(apiUrl: String, secretName: String)
}
