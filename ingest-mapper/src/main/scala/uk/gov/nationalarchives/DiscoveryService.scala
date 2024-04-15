package uk.gov.nationalarchives

import cats.effect.{IO, Resource}
import cats.implicits._
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.httpclient.fs2.HttpClientFs2Backend
import sttp.client3.{SttpBackend, UriContext, basicRequest}
import sttp.client3.circe.*
import sttp.client3.*
import uk.gov.nationalarchives.DiscoveryService._
import uk.gov.nationalarchives.Lambda.Input
import uk.gov.nationalarchives.MetadataService._
import uk.gov.nationalarchives.MetadataService.Type._
import io.circe.generic.auto._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.UUID
import javax.xml.transform.TransformerFactory
import javax.xml.transform.stream.{StreamResult, StreamSource}
import scala.xml.XML
import ujson._

class DiscoveryService(discoveryBaseUrl: String, backend: SttpBackend[IO, Fs2Streams[IO]], randomUuidGenerator: () => UUID) {

  private def replaceHtmlCodesWithUnicodeChars(input: String) =
    "&#[0-9]+".r.replaceAllIn(input, _.matched.drop(2).toInt.toChar.toString)

  private def stripHtmlFromDiscoveryResponse(discoveryAsset: DiscoveryCollectionAsset) = {
    val resources = for {
      xsltStream <- Resource.make(IO(getClass.getResourceAsStream("/transform.xsl")))(is => IO(is.close()))
      inputStream <- Resource.make {
        val descriptionWithHtmlCodesReplaced = replaceHtmlCodesWithUnicodeChars(discoveryAsset.scopeContent.description)
        IO(new ByteArrayInputStream(descriptionWithHtmlCodesReplaced.getBytes()))
      }(is => IO(is.close()))
      outputStream <- Resource.make(IO(new ByteArrayOutputStream()))(bos => IO(bos.close()))
    } yield (xsltStream, inputStream, outputStream)
    resources.use { case (xsltStream, inputStream, outputStream) =>
      val factory = TransformerFactory.newInstance()
      val xslt = new StreamSource(xsltStream)
      val input = new StreamSource(inputStream)
      val result = new StreamResult(outputStream)
      val transformer = factory.newTransformer(xslt)
      transformer.transform(input, result)
      val newDescription = outputStream.toByteArray.map(_.toChar).mkString.trim
      val scopeContentWithNewDescription = discoveryAsset.scopeContent.copy(description = newDescription)
      val titleWithoutHtmlCodes = replaceHtmlCodesWithUnicodeChars(discoveryAsset.title)
      val titleWithoutBackslashes = XML.loadString(titleWithoutHtmlCodes.replaceAll("\\\\", "")).text
      IO(discoveryAsset.copy(scopeContent = scopeContentWithNewDescription, title = titleWithoutBackslashes)).handleError(_ => discoveryAsset)
    }
  }

  private def getAssetFromDiscoveryApi(citableReference: String): IO[DiscoveryCollectionAsset] = {
    val uri = uri"$discoveryBaseUrl/API/records/v1/collection/$citableReference"
    val request = basicRequest.get(uri).response(asJson[DiscoveryCollectionAssetResponse])
    for {
      response <- backend.send(request)
      body <- IO.fromEither(response.body)
      potentialAsset = body.assets.find(_.citableReference == citableReference)
      formattedAsset <- potentialAsset.map(stripHtmlFromDiscoveryResponse).getOrElse {
        IO(DiscoveryCollectionAsset(citableReference, DiscoveryScopeContent(""), citableReference))
      }
    } yield formattedAsset
  }

  def getDepartmentAndSeriesRows(input: Input): IO[DepartmentAndSeriesTableData] = {
    def generateTableEntry(asset: DiscoveryCollectionAsset): Map[String, Value] =
      Map(
        "batchId" -> Str(input.batchId),
        "id" -> Str(randomUuidGenerator().toString),
        "name" -> Str(asset.citableReference),
        "type" -> Str(ArchiveFolder.toString),
        "title" -> Str(asset.title),
        "description" -> Str(asset.scopeContent.description)
      )

    for {
      potentialDepartmentDiscoveryAsset <- input.department.map(getAssetFromDiscoveryApi).sequence
      potentialSeriesDiscoveryAsset <- input.series.map(getAssetFromDiscoveryApi).sequence
    } yield {
      val departmentTableEntryMap = potentialDepartmentDiscoveryAsset
        .map(generateTableEntry)
        .map(jsonMap => jsonMap ++ Map("id_Code" -> jsonMap("name")))
        .getOrElse(Map("batchId" -> Str(input.batchId), "id" -> Str(randomUuidGenerator().toString), "name" -> Str("Unknown"), "type" -> Str(ArchiveFolder.toString)))

      val seriesTableEntryOpt = potentialSeriesDiscoveryAsset
        .map(generateTableEntry)
        .map(jsonMap => jsonMap ++ Map("parentPath" -> departmentTableEntryMap("id"), "id_Code" -> jsonMap("name")))
        .map(Obj.from)

      DepartmentAndSeriesTableData(Obj.from(departmentTableEntryMap), seriesTableEntryOpt)
    }
  }
}
object DiscoveryService {
  case class DiscoveryScopeContent(description: String)
  case class DiscoveryCollectionAsset(citableReference: String, scopeContent: DiscoveryScopeContent, title: String)
  case class DiscoveryCollectionAssetResponse(assets: List[DiscoveryCollectionAsset])

  def apply(discoveryUrl: String, randomUuidGenerator: () => UUID): IO[DiscoveryService] = HttpClientFs2Backend.resource[IO]().use { backend =>
    IO(new DiscoveryService(discoveryUrl, backend, randomUuidGenerator))
  }
}
