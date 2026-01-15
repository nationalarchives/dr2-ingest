package uk.gov.nationalarchives.preingestcourtdocpackagebuilder

import cats.effect.IO
import uk.gov.nationalarchives.preingestcourtdocpackagebuilder.UriProcessor.ParsedUri
import uk.gov.nationalarchives.utils.ExternalUtils.TREMetadata

trait UriProcessor:

  def verifyJudgmentName(treMetadata: TREMetadata): IO[Unit]

  def parseUri(treMetadata: TREMetadata): IO[Option[ParsedUri]]

object UriProcessor:

  case class ParsedUri(potentialCourt: Option[String], uriWithoutDocType: String)

  def apply(): UriProcessor = new UriProcessor {
    override def verifyJudgmentName(treMetadata: TREMetadata): IO[Unit] = {
      val potentialUri = treMetadata.parameters.PARSER.uri
      val potentialName = treMetadata.parameters.PARSER.name
      val uriContainsPressSummary = potentialUri.exists(_.contains("/press-summary"))
      val fileNameDoesNotStartWithPressSummaryOf = !potentialName.exists(_.startsWith("Press Summary of "))
      IO.whenA(uriContainsPressSummary && fileNameDoesNotStartWithPressSummaryOf)(
        IO.raiseError(new Exception("URI contains '/press-summary' but file does not start with 'Press Summary of '"))
      )
    }

    override def parseUri(treMetadata: TREMetadata): IO[Option[ParsedUri]] =
      treMetadata.parameters.PARSER.uri.map { uri =>
        val courtRegex = "^.*/id/([a-z]*)/".r
        val uriWithoutDocTypeRegex = """(^.*/\d{4}/\d*)""".r
        val potentialCourt = courtRegex.findFirstMatchIn(uri).map(_.group(1))
        val potentialUriWithoutDocType = uriWithoutDocTypeRegex.findFirstMatchIn(uri).map(_.group(1))
        IO.fromOption(potentialUriWithoutDocType)(
          new RuntimeException(s"Failure trying to trim off the doc type for $uri. Is the year missing?")
        ).map { uriWithoutDocType =>
          ParsedUri(potentialCourt, uriWithoutDocType)
        }
      }.sequence
  }
