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
import uk.gov.nationalarchives.ingestmapper.MetadataService.Type.*

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.UUID
import javax.xml.transform.TransformerFactory
import javax.xml.transform.stream.{StreamResult, StreamSource}
import scala.xml.XML

trait DiscoveryService[F[_]] {

  def departmentItem(batchId: String, collectionAsset: Option[DiscoveryCollectionAsset]): Obj
  def seriesItem(batchId: String, department: Obj, collectionAsset: DiscoveryCollectionAsset): Obj
  def getAssetFromDiscoveryApi(citableReference: String): F[DiscoveryCollectionAsset]

}
object DiscoveryService {
  case class DiscoveryScopeContent(description: Option[String])
  case class DiscoveryCollectionAsset(citableReference: String, scopeContent: DiscoveryScopeContent, title: Option[String])
  private case class DiscoveryCollectionAssetResponse(assets: List[DiscoveryCollectionAsset])

  def apply[F[_]: Async](discoveryBaseUrl: String, backend: SttpBackend[F, Fs2Streams[F]], randomUuidGenerator: () => UUID): DiscoveryService[F] =
    new DiscoveryService[F] {

      private val logger: SelfAwareStructuredLogger[F] = Slf4jFactory.create[F].getLogger

      private def replaceHtmlCodesWithUnicodeChars(input: String) =
        "&#[0-9]+".r.replaceAllIn(input, _.matched.drop(2).toInt.toChar.toString)

      private def transformWithXslt(description: String): F[String] = {
        val resources = for {
          xsltStream <- Resource.make(Async[F].blocking(getClass.getResourceAsStream("/transform.xsl")))(is => Async[F].blocking(is.close()))
          inputStream <- Resource.make {
            val descriptionWithHtmlCodesReplaced = replaceHtmlCodesWithUnicodeChars(description)
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
          Async[F].pure(outputStream.toByteArray.map(_.toChar).mkString.trim)
        }
      }

      private def stripHtmlFromDiscoveryResponse(discoveryAsset: DiscoveryCollectionAsset) = {
        discoveryAsset.scopeContent.description.traverse(transformWithXslt).map { potentialDescription =>
          val potentialTitleWithoutBackslashes = discoveryAsset.title.map { discoveryAssetTitle =>
            val titleWithoutHtmlCodes = replaceHtmlCodesWithUnicodeChars(discoveryAssetTitle)
            XML.loadString(titleWithoutHtmlCodes.replaceAll("\\\\", "")).text
          }
          val newScopeContent = DiscoveryScopeContent(potentialDescription)
          discoveryAsset.copy(title = potentialTitleWithoutBackslashes, scopeContent = newScopeContent)
        }
      }

      private def defaultCollectionAsset(citableReference: String): F[DiscoveryCollectionAsset] =
        Async[F].pure(DiscoveryCollectionAsset(citableReference, DiscoveryScopeContent(None), None))

      def getAssetFromDiscoveryApi(citableReference: String): F[DiscoveryCollectionAsset] = {
        val uri = uri"$discoveryBaseUrl/API/records/v1/collection/$citableReference"
        val request = basicRequest.get(uri).response(asJson[DiscoveryCollectionAssetResponse])
        for {
          response <- backend.send(request)
          body <- Async[F].fromEither(response.body)
          potentialAsset = body.assets.find(_.citableReference == citableReference)
          formattedAsset <- potentialAsset.map(stripHtmlFromDiscoveryResponse).getOrElse(defaultCollectionAsset(citableReference))
        } yield formattedAsset
      }.handleErrorWith { e =>
        logger.warn(e)("Error from Discovery") >> defaultCollectionAsset(citableReference)
      }

      def generateTableItem(batchId: String, asset: DiscoveryCollectionAsset): Map[String, Value] =
        Map(
          "batchId" -> Str(batchId),
          "id" -> Str(randomUuidGenerator().toString),
          "name" -> Str(asset.citableReference),
          "type" -> Str(ArchiveFolder.toString)
        ) ++ asset.title.map(title => Map("title" -> Str(title))).getOrElse(Map())
          ++ asset.scopeContent.description.map(description => Map("description" -> Str(description))).getOrElse(Map())

      def seriesItem(batchId: String, department: Obj, collectionAsset: DiscoveryCollectionAsset): Obj = {
        val jsonMap = generateTableItem(batchId, collectionAsset)
        Obj.from {
          jsonMap ++ Map("parentPath" -> department("id"), "id_Code" -> jsonMap("name"))

        }
      }

      def departmentItem(batchId: String, collectionAsset: Option[DiscoveryCollectionAsset]): Obj = {
        collectionAsset
          .map(a => generateTableItem(batchId, a))
          .map(jsonMap => jsonMap ++ Map("id_Code" -> jsonMap("name")))
          .getOrElse(
            Map(
              "batchId" -> Str(batchId),
              "id" -> Str(randomUuidGenerator().toString),
              "name" -> Str("Unknown"),
              "type" -> Str(ArchiveFolder.toString)
            )
          )
      }
    }

  def apply[F[_]: Async](discoveryBaseUrl: String, randomUuidGenerator: () => UUID): F[DiscoveryService[F]] = HttpClientFs2Backend.resource[F]().use { backend =>
    Async[F].pure(DiscoveryService(discoveryBaseUrl, backend, randomUuidGenerator))
  }
}
