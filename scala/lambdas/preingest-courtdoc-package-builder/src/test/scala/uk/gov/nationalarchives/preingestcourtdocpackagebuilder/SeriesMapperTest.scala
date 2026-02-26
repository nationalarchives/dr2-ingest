package uk.gov.nationalarchives.preingestcourtdocpackagebuilder

import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import SeriesMapper.seriesMap

import java.net.URI

class SeriesMapperTest extends AnyFlatSpec with TableDrivenPropertyChecks {

  "seriesMap" should "have 15 entries" in {
    seriesMap.unsafeRunSync().size should equal(15)
  }

  val courtToSeries: TableFor2[String, String] = Table(
    ("court", "series"),
    ("EAT", "LE 10"),
    ("EWCA", "J 347"),
    ("EWHC", "J 348"),
    ("EWCOP", "J 349"),
    ("EWFC", "J 350"),
    ("UKPC", "PCAP 16"),
    ("UKSC", "UKSC 2"),
    ("UKUT", "LE 9"),
    ("UKEAT", "LE 10"),
    ("UKFTT", "LE 11"),
    ("UKET", "LE 12"),
    ("UKIPTRIB", "HO 654"),
    ("EWCC", "J 354"),
    ("EWCRC", "J 355"),
    ("EWCR", "J 355")
  )

  private val s3Uri: URI = URI.create("s3://upload/key")

  assert(courtToSeries.length == seriesMap.unsafeRunSync().size)

  forAll(courtToSeries) { (court, series) =>
    "createDepartmentAndSeries" should s"return $series for court $court" in {
      val seriesMapper = SeriesMapper()
      val output =
        seriesMapper.createDepartmentAndSeries(Option(court), skipSeriesLookup = false).unsafeRunSync()
      output.potentialDepartment.get should equal(series.split(' ').head)
      output.potentialSeries.get should equal(series)
    }
  }

  "createDepartmentAndSeries" should "return an error if a court does not yield a series and 'skipSeriesLookup' is set to false" in {
    val seriesMapper = SeriesMapper()
    val ex = intercept[Exception] {
      seriesMapper.createDepartmentAndSeries(Option("invalidCourt"), skipSeriesLookup = false).unsafeRunSync()
    }
    val expectedMessage = s"Cannot find series and department for court invalidCourt"
    ex.getMessage should equal(expectedMessage)
  }

  "createDepartmentAndSeries" should "return an empty department and series if a court does not yield a series but 'skipSeriesLookup' is set to true" in {
    val seriesMapper = SeriesMapper()
    val output =
      seriesMapper.createDepartmentAndSeries(Option("invalidCourt"), skipSeriesLookup = true).unsafeRunSync()

    output.potentialSeries should equal(None)
    output.potentialDepartment should equal(None)
  }

  "createDepartmentAndSeries" should "return an empty department and series if the court is missing" in {
    val seriesMapper = SeriesMapper()
    val output = seriesMapper.createDepartmentAndSeries(None, skipSeriesLookup = false).unsafeRunSync()

    output.potentialSeries should equal(None)
    output.potentialDepartment should equal(None)
  }
}
