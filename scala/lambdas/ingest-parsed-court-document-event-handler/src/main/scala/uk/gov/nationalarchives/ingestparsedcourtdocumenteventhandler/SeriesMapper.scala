package uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler

import cats.effect.*
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.SeriesMapper.*

import java.net.URI

class SeriesMapper(validCourts: Set[Court]) {
  def createDepartmentAndSeries(
      potentialCourt: Option[String],
      skipSeriesLookup: Boolean
  ): IO[DepartmentAndSeries] = {
    potentialCourt
      .map { court =>
        val potentiallyFoundCourt: Option[Court] = validCourts.find(_.code == court.toUpperCase)
        potentiallyFoundCourt match {
          case None if skipSeriesLookup => IO.pure(DepartmentAndSeries(None, None))
          case None                     => IO.raiseError(new Exception(s"Cannot find series and department for court $court"))
          case _ =>
            IO.pure(
              DepartmentAndSeries(potentiallyFoundCourt.map(_.dept), potentiallyFoundCourt.map(_.series))
            )
        }
      }
      .getOrElse(IO.pure(DepartmentAndSeries(None, None)))

  }
}

object SeriesMapper {
  case class Output(
      batchId: String,
      metadataPackage: URI
  )

  case class DepartmentAndSeries(potentialDepartment: Option[String], potentialSeries: Option[String])
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
    Court("UKET", "LE", "LE 12"),
    Court("UKIPTRIB", "HO", "HO 654"),
    Court("EWCC", "J", "J 354"),
    Court("EWCRC", "J", "J 355"),
    Court("EWCR", "J", "J 355")
  )
}
