package uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler

import cats.effect._
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.SeriesMapper._

class SeriesMapper(validCourts: Set[Court]) {
  def createOutput(
      uploadBucket: String,
      batchId: String,
      potentialCourt: Option[String],
      skipSeriesLookup: Boolean
  ): IO[Output] = {
    potentialCourt
      .map { court =>
        val potentiallyFoundCourt: Option[Court] = validCourts.find(_.code == court.toUpperCase)
        potentiallyFoundCourt match {
          case None if skipSeriesLookup => IO.pure(Output(batchId, uploadBucket, s"$batchId/", None, None))
          case None                     => IO.raiseError(new Exception(s"Cannot find series and department for court $court for batchId $batchId"))
          case _ =>
            IO.pure(
              Output(batchId, uploadBucket, s"$batchId/", potentiallyFoundCourt.map(_.dept), potentiallyFoundCourt.map(_.series))
            )
        }
      }
      .getOrElse(IO.pure(Output(batchId, uploadBucket, s"$batchId/", None, None)))

  }
}

object SeriesMapper {
  case class Output(
      batchId: String,
      s3Bucket: String,
      s3Prefix: String,
      department: Option[String],
      series: Option[String]
  )

  case class Court(code: String, dept: String, series: String)

  def apply(): SeriesMapper = new SeriesMapper(seriesMap)

  val seriesMap: Set[Court] = Set(
    Court("EAT", "LE", "LE 10"),
    Court("EWCA", "J", "J 347"),
    Court("EWHC", "J", "J 348"),
    Court("EWCOP", "J", "J 349"),
    Court("EWFC", "J", "J 350"),
    Court("UKPC", "PCAP", "PCAP 16"),
    Court("UKSC", "UKSC", "UKSC 2"),
    Court("UKUT", "LE", "LE 9"),
    Court("UKEAT", "LE", "LE 10"),
    Court("UKFTT", "LE", "LE 11"),
    Court("UKET", "LE", "LE 12")
  )
}
