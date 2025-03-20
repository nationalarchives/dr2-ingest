package uk.gov.nationalarchives.ingestmapper

import cats.effect.{Async, Resource}
import cats.implicits.*
import io.circe.generic.auto.*
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jFactory
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.circe.*
import sttp.client3.httpclient.fs2.HttpClientFs2Backend
import sttp.client3.*
import ujson.*
import uk.gov.nationalarchives.ingestmapper.DiscoveryService.*
import uk.gov.nationalarchives.ingestmapper.MetadataService.*
import uk.gov.nationalarchives.ingestmapper.MetadataService.Type.*

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.UUID
import javax.xml.transform.TransformerFactory
import javax.xml.transform.stream.{StreamResult, StreamSource}
import scala.xml.XML

trait DiscoveryService[F[_]] {

  def getDepartmentAndSeriesItems(batchId: String, departmentAndSeriesAssets: DepartmentAndSeriesCollectionAssets): DepartmentAndSeriesTableItems

  def getDiscoveryCollectionAssets(potentialSeries: Option[String]): F[DepartmentAndSeriesCollectionAssets]

}
object DiscoveryService {
  case class DiscoveryScopeContent(description: String)
  case class DiscoveryCollectionAsset(citableReference: String, scopeContent: DiscoveryScopeContent, title: String)
  private case class DiscoveryCollectionAssetResponse(assets: List[DiscoveryCollectionAsset])
  case class DepartmentAndSeriesCollectionAssets(
      potentialDepartmentCollectionAsset: Option[DiscoveryCollectionAsset],
      potentialSeriesCollectionAsset: Option[DiscoveryCollectionAsset]
  )

  def apply[F[_]: Async](discoveryBaseUrl: String, backend: SttpBackend[F, Fs2Streams[F]], randomUuidGenerator: () => UUID): DiscoveryService[F] =
    new DiscoveryService[F] {

      private val logger: SelfAwareStructuredLogger[F] = Slf4jFactory.create[F].getLogger

      private def replaceHtmlCodesWithUnicodeChars(input: String) =
        "&#[0-9]+".r.replaceAllIn(input, _.matched.drop(2).toInt.toChar.toString)

      private def stripHtmlFromDiscoveryResponse(discoveryAsset: DiscoveryCollectionAsset) = {
        val resources = for {
          xsltStream <- Resource.make(Async[F].blocking(getClass.getResourceAsStream("/transform.xsl")))(is => Async[F].blocking(is.close()))
          inputStream <- Resource.make {
            val descriptionWithHtmlCodesReplaced = replaceHtmlCodesWithUnicodeChars(discoveryAsset.scopeContent.description)
            Async[F].blocking(new ByteArrayInputStream(descriptionWithHtmlCodesReplaced.getBytes()))
          }(is => Async[F].blocking(is.close()))
          outputStream <- Resource.make(Async[F].pure(new ByteArrayOutputStream()))(bos => Async[F].blocking(bos.close()))
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
          Async[F].pure(discoveryAsset.copy(scopeContent = scopeContentWithNewDescription, title = titleWithoutBackslashes)).handleError(_ => discoveryAsset)
        }
      }

      private def getAssetFromDiscoveryApi(citableReference: String): F[DiscoveryCollectionAsset] = {
        val uri = uri"$discoveryBaseUrl/API/records/v1/collection/$citableReference"
        val request = basicRequest.get(uri).response(asJson[DiscoveryCollectionAssetResponse])
        for {
          response <- backend.send(request)
          body <- Async[F].fromEither(response.body)
          potentialAsset = body.assets.find(_.citableReference == citableReference)
          formattedAsset <- potentialAsset.map(stripHtmlFromDiscoveryResponse).getOrElse {
            Async[F].pure(DiscoveryCollectionAsset(citableReference, DiscoveryScopeContent(""), citableReference))
          }
        } yield formattedAsset
      }.handleErrorWith { e =>
        logger.warn(e)("Error from Discovery") >> Async[F].pure(DiscoveryCollectionAsset(citableReference, DiscoveryScopeContent(""), citableReference))
      }

      override def getDiscoveryCollectionAssets(potentialSeries: Option[String]): F[DepartmentAndSeriesCollectionAssets] = {
        val potentialDepartment = potentialSeries.flatMap(_.split(" ").headOption)
        for {
          potentialDepartmentDiscoveryAsset <- potentialDepartment.traverse(getAssetFromDiscoveryApi)
          potentialSeriesDiscoveryAsset <- potentialSeries.traverse(getAssetFromDiscoveryApi)
        } yield DepartmentAndSeriesCollectionAssets(potentialDepartmentDiscoveryAsset, potentialSeriesDiscoveryAsset)
      }

      override def getDepartmentAndSeriesItems(batchId: String, departmentAndSeriesAssets: DepartmentAndSeriesCollectionAssets): DepartmentAndSeriesTableItems = {
        def generateTableItem(asset: DiscoveryCollectionAsset): Map[String, Value] =
          Map(
            "batchId" -> Str(batchId),
            "id" -> Str(randomUuidGenerator().toString),
            "name" -> Str(asset.citableReference),
            "type" -> Str(ArchiveFolder.toString),
            "title" -> Str(asset.title),
            "description" -> Str(asset.scopeContent.description)
          )

        val departmentTableEntryMap = departmentAndSeriesAssets.potentialDepartmentCollectionAsset
          .map(generateTableItem)
          .map(jsonMap => jsonMap ++ Map("id_Code" -> jsonMap("name")))
          .getOrElse(
            Map(
              "batchId" -> Str(batchId),
              "id" -> Str(randomUuidGenerator().toString),
              "name" -> Str("Unknown"),
              "type" -> Str(ArchiveFolder.toString)
            )
          )

        val seriesTableEntryOpt = departmentAndSeriesAssets.potentialSeriesCollectionAsset
          .map(generateTableItem)
          .map(jsonMap => jsonMap ++ Map("parentPath" -> departmentTableEntryMap("id"), "id_Code" -> jsonMap("name")))
          .map(Obj.from)

        DepartmentAndSeriesTableItems(Obj.from(departmentTableEntryMap), seriesTableEntryOpt)

      }
    }

  def apply[F[_]: Async](discoveryBaseUrl: String, randomUuidGenerator: () => UUID): F[DiscoveryService[F]] = HttpClientFs2Backend.resource[F]().use { backend =>
    Async[F].pure(DiscoveryService(discoveryBaseUrl, backend, randomUuidGenerator))
  }
}
