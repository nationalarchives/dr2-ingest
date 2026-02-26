package uk.gov.nationalarchives.preingestcourtdocpackagebuilder

import cats.effect.*
import SeriesMapper.*
import io.circe.parser.decode
import io.circe.generic.auto.*

import scala.io.Source

trait SeriesMapper:
  def createDepartmentAndSeries(potentialCourt: Option[String], skipSeriesLookup: Boolean): IO[DepartmentSeries]

object SeriesMapper:
  case class DepartmentSeries(potentialDepartment: Option[String], potentialSeries: Option[String])
  case class Court(code: String, dept: String, series: String)

  val seriesMap: IO[Set[Court]] = IO
    .fromEither {
      decode[List[Court]](Source.fromResource("series.json").getLines().mkString)
    }
    .map(_.toSet)

  def apply(): SeriesMapper = (potentialCourt: Option[String], skipSeriesLookup: Boolean) =>
    potentialCourt
      .map { court =>
        seriesMap.flatMap { map =>
          val potentiallyFoundCourt: Option[Court] = map.find(_.code == court.toUpperCase)
          potentiallyFoundCourt match {
            case None if skipSeriesLookup => IO.pure(DepartmentSeries(None, None))
            case None                     => IO.raiseError(new Exception(s"Cannot find series and department for court $court"))
            case _                        => IO.pure(DepartmentSeries(potentiallyFoundCourt.map(_.dept), potentiallyFoundCourt.map(_.series)))
          }
        }
      }
      .getOrElse(IO.pure(DepartmentSeries(None, None)))
