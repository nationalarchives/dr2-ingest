package uk.gov.nationalarchives.ingestmapper

import cats.effect.IO
import cats.effect.IO.asyncForIO
import cats.effect.unsafe.implicits.global
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.UriContext
import sttp.client3.impl.cats.CatsMonadError
import sttp.client3.testing.SttpBackendStub
import ujson.Obj
import uk.gov.nationalarchives.ingestmapper.DiscoveryService.{DepartmentAndSeriesCollectionAssets, DiscoveryCollectionAsset, DiscoveryScopeContent}

import java.net.URI
import java.util.UUID

class DiscoveryServiceTest extends AnyFlatSpec {

  val uuids: List[String] = List(
    "c7e6b27f-5778-4da8-9b83-1b64bbccbd03",
    "61ac0166-ccdf-48c4-800f-29e5fba2efda",
    "457cc27d-5b74-4e81-80e3-d808e0b3e425",
    "2b9e5c87-2342-4006-b1aa-5308a5ce2544",
    "a504d58d-2f7d-4f29-b5b8-173b558970db",
    "41c5604d-70b3-44d1-aa1f-d9ffe18b33cb"
  )

  lazy val s3Uri: URI = URI.create("s3://bucket/key")

  val uuidIterator: () => UUID = () => {
    val uuidsIterator: Iterator[String] = uuids.iterator
    UUID.fromString(uuidsIterator.next())
  }

  val baseUrl = "http://localhost"
  val assetMap: Map[String, Option[DiscoveryCollectionAsset]] = List("T", "T TEST").map { col =>
    col -> Option(DiscoveryCollectionAsset(col, DiscoveryScopeContent(Option(s"TestDescription $col 1          \nTestDescription $col 2")), Option(s"Test Title $col")))
  }.toMap
  val bodyMap: Map[String, String] = List("T", "T TEST").map { col =>
    val description = <scopecontent>
        <head>Head</head>
        <p><list>
          <item>TestDescription {col} &#49;</item>
          <item>TestDescription {col} &#50;</item></list>
        </p>
      </scopecontent>.toString().replaceAll("\n", "")

    val body =
      s"""{
         |  "assets": [
         |    {
         |      "citableReference": "$col",
         |      "scopeContent": {
         |        "description": "$description"
         |      },
         |      "title": "<unittitle type=&#34Title\\">Test \\\\Title $col</unittitle>"
         |    }
         |  ]
         |}
         |""".stripMargin
    col -> body
  }.toMap

  private def checkDynamoItem(item: Obj, collection: String, expectedId: String, parentPath: Option[String], citableRefFound: Boolean = true): Assertion = {
    val expectedTitle = if citableRefFound then s"Test Title $collection" else collection
    val expectedDescription = if citableRefFound then s"TestDescription $collection 1          \nTestDescription $collection 2" else ""

    item("id").str should equal(expectedId)
    item("name").str should equal(collection)
    item("batchId").str should equal("testBatch")
    item("type").str should equal("ArchiveFolder")
    !item.value.contains("fileSize") should be(true)
    item.value.get("parentPath").map(_.str) should equal(parentPath)
    if (collection != "Unknown") {
      item("title").str should equal(expectedTitle)
      item("id_Code").str should equal(collection)
      item("description").str should equal(expectedDescription)
    } else {
      item.value.contains("title") should equal(false)
      item.value.contains("id_Code") should equal(false)
      item.value.contains("description") should equal(false)
    }
  }

  "getDiscoveryCollectionAssets" should "return the correct values for series and department" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T"))
      .thenRespond(bodyMap("T"))
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T TEST"))
      .thenRespond(bodyMap("T TEST"))

    val result = DiscoveryService(baseUrl, backend, uuidIterator)
      .getDiscoveryCollectionAssets(Option("T TEST"))
      .unsafeRunSync()

    val departmentCollectionAsset = result.potentialDepartmentCollectionAsset.get
    val seriesCollectionAsset = result.potentialSeriesCollectionAsset.get
    def checkAsset(asset: DiscoveryCollectionAsset, suffix: String) = {
      asset.potentialTitle.get should equal(s"Test Title $suffix")
      asset.scopeContent.potentialDescription.get should equal(s"TestDescription $suffix 1          \nTestDescription $suffix 2")
      asset.citableReference should equal(suffix)
    }
    checkAsset(departmentCollectionAsset, "T")
    checkAsset(seriesCollectionAsset, "T TEST")
  }

  "getDiscoveryCollectionAssets" should "return an empty title and description if the discovery API returns an error" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError()).whenAnyRequest
      .thenRespondServerError()

    val assets = DiscoveryService(baseUrl, backend, uuidIterator)
      .getDiscoveryCollectionAssets(Option("A TEST"))
      .unsafeRunSync()

    val discoveryAsset = assets.potentialDepartmentCollectionAsset.get
    val seriesAsset = assets.potentialSeriesCollectionAsset.get

    discoveryAsset.potentialTitle should equal(None)
    discoveryAsset.citableReference should equal("A")
    discoveryAsset.scopeContent.potentialDescription should equal(None)

    seriesAsset.potentialTitle should equal(None)
    seriesAsset.citableReference should equal("A TEST")
    seriesAsset.scopeContent.potentialDescription should equal(None)
  }

  "getDiscoveryCollectionAssets" should "set the citable ref as the title and description as None, if the series reference doesn't match the response" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/A"))
      .thenRespond(bodyMap("T"))
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/A TEST"))
      .thenRespond(bodyMap("T TEST"))

    val result = DiscoveryService(baseUrl, backend, uuidIterator)
      .getDiscoveryCollectionAssets(Option("A TEST"))
      .unsafeRunSync()

    val departmentItem = result.potentialDepartmentCollectionAsset.get
    val seriesItem = result.potentialSeriesCollectionAsset.get

    def checkAsset(asset: DiscoveryCollectionAsset, suffix: String) = {
      asset.potentialTitle should equal(suffix)
      asset.scopeContent.potentialDescription should equal("")
      asset.citableReference should equal(suffix)
    }
  }

  "getDiscoveryCollectionAssets" should "set the citable ref as the title and description as '', if the department call returns an empty asset" in {
    val emptyResponse: String = """{"assets": []}"""

    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T"))
      .thenRespond(emptyResponse)
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T TEST"))
      .thenRespond(bodyMap("T TEST"))

    val result = DiscoveryService(baseUrl, backend, uuidIterator)
      .getDiscoveryCollectionAssets(Option("T TEST"))
      .unsafeRunSync()

    result.potentialSeriesCollectionAsset.isDefined should equal(true)
    val departmentItem = result.potentialDepartmentCollectionAsset.get
    val seriesItem = result.potentialSeriesCollectionAsset.get

    departmentItem.potentialTitle should equal(None)
    departmentItem.scopeContent.potentialDescription should equal(None)
    departmentItem.citableReference should equal("T")
    seriesItem.potentialTitle.get should equal("Test Title T TEST")
    seriesItem.scopeContent.potentialDescription.get should equal("TestDescription T TEST 1          \nTestDescription T TEST 2")
    seriesItem.citableReference should equal("T TEST")
  }

  "getDiscoveryCollectionAssets" should "set the citable ref as the title and description as '', if the series call returns an empty asset" in {
    val emptyResponse: String = """{"assets": []}"""

    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T"))
      .thenRespond(bodyMap("T"))
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T TEST"))
      .thenRespond(emptyResponse)

    val result = DiscoveryService(baseUrl, backend, uuidIterator)
      .getDiscoveryCollectionAssets(Option("T TEST"))
      .unsafeRunSync()

    result.potentialSeriesCollectionAsset.isDefined should equal(true)
    val departmentItem = result.potentialDepartmentCollectionAsset.get
    val seriesItem = result.potentialSeriesCollectionAsset.get

    departmentItem.potentialTitle.get should equal("Test Title T")
    departmentItem.scopeContent.potentialDescription.get should equal("TestDescription T 1          \nTestDescription T 2")
    departmentItem.citableReference should equal("T")
    seriesItem.potentialTitle should equal(None)
    seriesItem.scopeContent.potentialDescription should equal(None)
    seriesItem.citableReference should equal("T TEST")
  }

  "getDepartmentAndSeriesItems" should "return the correct values for series and department" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())

    val result = DiscoveryService(baseUrl, backend, uuidIterator)
      .getDepartmentAndSeriesItems("testBatch", DepartmentAndSeriesCollectionAssets(assetMap("T"), assetMap("T TEST")))

    val departmentItem = result.departmentItem
    val seriesItem = result.potentialSeriesItem.head

    checkDynamoItem(departmentItem, "T", uuids.head, None)
    checkDynamoItem(seriesItem, "T TEST", uuids.head, Option(uuids.head))
  }

  "getDepartmentAndSeriesItems" should "not add a title attribute if the title is missing" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
    val departmentCollectionAsset = assetMap("T").map(_.copy(potentialTitle = None))
    val seriesCollectionAsset = assetMap("T TEST").map(_.copy(potentialTitle = None))

    val result = DiscoveryService(baseUrl, backend, uuidIterator)
      .getDepartmentAndSeriesItems("testBatch", DepartmentAndSeriesCollectionAssets(departmentCollectionAsset, seriesCollectionAsset))

    val departmentItem = result.departmentItem
    val seriesItem = result.potentialSeriesItem.head

    departmentItem.value.contains("title") should equal(false)
    seriesItem.value.contains("title") should equal(false)
  }

  "getDepartmentAndSeriesItems" should "not add a description attribute if the description is missing" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
    val departmentCollectionAsset = assetMap("T").map(departmentAsset => departmentAsset.copy(scopeContent = departmentAsset.scopeContent.copy(potentialDescription = None)))
    val seriesCollectionAsset = assetMap("T TEST").map(seriesAsset => seriesAsset.copy(scopeContent = seriesAsset.scopeContent.copy(potentialDescription = None)))

    val result = DiscoveryService(baseUrl, backend, uuidIterator)
      .getDepartmentAndSeriesItems("testBatch", DepartmentAndSeriesCollectionAssets(departmentCollectionAsset, seriesCollectionAsset))

    val departmentItem = result.departmentItem
    val seriesItem = result.potentialSeriesItem.head

    departmentItem.value.contains("description") should equal(false)
    seriesItem.value.contains("description") should equal(false)
  }

  "getDepartmentAndSeriesItems" should "return unknown for the department if the department is missing" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())

    val result = DiscoveryService(baseUrl, backend, uuidIterator)
      .getDepartmentAndSeriesItems("testBatch", DepartmentAndSeriesCollectionAssets(None, assetMap("T TEST")))

    val departmentItem = result.departmentItem
    val seriesItem = result.potentialSeriesItem.head

    checkDynamoItem(departmentItem, "Unknown", uuids.head, None)
    checkDynamoItem(seriesItem, "T TEST", uuids.head, Option(uuids.head))
  }

  "getDepartmentAndSeriesItems" should "return an empty series if the series is missing" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())

    val result = DiscoveryService(baseUrl, backend, uuidIterator)
      .getDepartmentAndSeriesItems("testBatch", DepartmentAndSeriesCollectionAssets(assetMap("T"), None))

    val departmentItem = result.departmentItem
    val seriesItem = result.potentialSeriesItem

    seriesItem.isEmpty should be(true)
    checkDynamoItem(departmentItem, "T", uuids.head, None)

  }

  "getDepartmentAndSeriesItems" should "return an unknown department if the series and department are missing" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())

    val result = DiscoveryService(baseUrl, backend, uuidIterator)
      .getDepartmentAndSeriesItems("testBatch", DepartmentAndSeriesCollectionAssets(None, None))

    result.potentialSeriesItem.isDefined should equal(false)
    val departmentItem = result.departmentItem
    checkDynamoItem(departmentItem, "Unknown", uuids.head, None)
  }

}
