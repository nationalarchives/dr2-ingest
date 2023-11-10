package uk.gov.nationalarchives

import cats.effect.IO
import cats.implicits._
import uk.gov.nationalarchives.UriProcessor.ParsedUri

class UriProcessor(potentialUri: Option[String]) {
  def verifyFileNameStartsWithPressSummaryOfIfInUri(potentialFileName: Option[String]): IO[Unit] = {
    val uriContainsPressSummary = potentialUri.exists(_.contains("/press-summary"))
    val fileNameDoesNotStartWithPressSummaryOf = !potentialFileName.exists(_.startsWith("Press Summary of "))
    if (uriContainsPressSummary && fileNameDoesNotStartWithPressSummaryOf)
      IO.raiseError(new Exception("URI contains '/press-summary' but file does not start with 'Press Summary of '"))
    else IO.unit
  }

  def getCiteAndUriWithoutDocType: IO[Option[ParsedUri]] = {
    potentialUri.map { uri =>
      val citeRegex = "^.*/id/([a-z]*)/".r
      val uriWithoutDocTypeRegex = """(^.*/\d{4}/\d*)""".r
      val potentialCite = citeRegex.findFirstMatchIn(uri).map(_.group(1))
      val potentialUriWithoutDocType = uriWithoutDocTypeRegex.findFirstMatchIn(uri).map(_.group(1))
      IO.fromOption(potentialUriWithoutDocType)(
        new RuntimeException(s"Failure trying to trim off the doc type for $uri. Is the year missing?")
      ).map { uriWithoutDocType =>
        ParsedUri(potentialCite, uriWithoutDocType)
      }
    }.sequence
  }
}

object UriProcessor {
  case class ParsedUri(potentialCite: Option[String], uriWithoutDocType: String)

}
