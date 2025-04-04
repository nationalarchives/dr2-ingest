package uk.gov.nationalarchives.utils

import java.util.logging.{ConsoleHandler, Formatter, LogRecord, Logger, Level}
import io.circe.syntax.*
object LogUtils {

  def createLogger(name: String): Logger = {
    val jsonFormatter: Formatter =
      new Formatter {
        override def format(record: LogRecord): String = {
          val parameters = Option(record.getParameters).map(_.toList).getOrElse(Nil).map {
            case (a: String, b: String) => a -> b
            case unknown => "unknown" -> unknown.toString
          }.toMap
          val jsonMap = Map(
            "timestamp" -> record.getInstant.toString,
            "level" -> record.getLevel.getName,
            "logger" -> record.getLoggerName,
            "message" -> record.getMessage
          ) ++ parameters
          jsonMap.asJson.noSpaces
        }
      }

    val logger: Logger = Logger.getLogger(name)
    logger.setUseParentHandlers(false)
    logger.getHandlers.toList.foreach(logger.removeHandler)
    val logHandler = new ConsoleHandler()
    logHandler.setLevel(Level.ALL)
    logHandler.setFormatter(jsonFormatter)
    logger.addHandler(logHandler)
    logger
  }

}
