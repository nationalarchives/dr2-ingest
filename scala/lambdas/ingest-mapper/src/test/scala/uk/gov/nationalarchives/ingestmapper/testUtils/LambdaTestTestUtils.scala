package uk.gov.nationalarchives.ingestmapper.testUtils

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import fs2.interop.reactivestreams.*
import org.reactivestreams.Publisher
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import reactor.core.publisher.Flux
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.s3.model.{DeleteObjectsResponse, HeadObjectResponse, ListObjectsV2Response, PutObjectResponse}
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CompletedUpload}
import sttp.client3.impl.cats.CatsMonadAsyncError
import sttp.client3.testing.SttpBackendStub
import ujson.Obj
import uk.gov.nationalarchives.ingestmapper.DiscoveryService.{DepartmentAndSeriesCollectionAssets, DiscoveryCollectionAsset, DiscoveryScopeContent}
import uk.gov.nationalarchives.ingestmapper.Lambda.{Config, Dependencies, Input}
import uk.gov.nationalarchives.ingestmapper.MetadataService.DepartmentAndSeriesTableItems
import uk.gov.nationalarchives.ingestmapper.testUtils.TestUtils.*
import uk.gov.nationalarchives.ingestmapper.{DiscoveryService, MetadataService}
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import upickle.default.write

import java.net.URI
import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID

object LambdaTestTestUtils extends TableDrivenPropertyChecks {
  val inputBucket = "input"
  val uuids: List[String] = List(
    "c7e6b27f-5778-4da8-9b83-1b64bbccbd03",
    "61ac0166-ccdf-48c4-800f-29e5fba2efda",
    "5364b309-aa11-4660-b518-f47b5b96a588",
    "0dcc4151-b1d0-44ac-a4a1-5415d7d50d65"
  )
  val config: Config = Config("test", "http://localhost:9015", "testInputStateBucket")
  def input(s3Prefix: String = "TEST/"): Input = Input("TEST", "TEST_0", URI.create(s"s3://$inputBucket/${s3Prefix}metadata.json"), "executionName")

  def notImplemented[T]: IO[T] = IO.raiseError(new Exception("Not implemented"))

  case class Identifiers(
      folderIdentifier: UUID,
      assetIdentifier: UUID,
      docxIdentifier: UUID,
      metadataFileIdentifier: UUID,
      originalMetadataFilesOne: List[String]
  )
  case class MetadataResponse(metadata: String, identifiersOne: Identifiers, identifiersTwo: Identifiers)

  def getMetadata: MetadataResponse = {
    val folderIdentifierOne = UUID.randomUUID()
    val folderIdentifierTwo = UUID.randomUUID()
    val assetIdentifierOne = UUID.randomUUID()
    val assetIdentifierTwo = UUID.randomUUID()
    val docxIdentifierOne = UUID.randomUUID()
    val docxIdentifierTwo = UUID.randomUUID()
    val metadataFileIdentifierOne = UUID.randomUUID()
    val metadataFileIdentifierTwo = UUID.randomUUID()
    val originalMetadataFilesOne = List(UUID.randomUUID(), UUID.randomUUID()).map(_.toString)
    val originalMetadataFilesTwo = List(UUID.randomUUID(), UUID.randomUUID()).map(_.toString)

    val metadata =
      s"""[
         |{"id":"$folderIdentifierOne","parentId":null,"title":"TestTitle","type":"ArchiveFolder","name":"TestName","fileSize":null, "customMetadataAttribute2": "customMetadataValue2", "series": "A 1"},
         |{"id":"$folderIdentifierTwo","parentId":null,"title":"TestTitle","type":"ArchiveFolder","name":"TestName","fileSize":null, "customMetadataAttribute2": "customMetadataValue2", "series": null},
         |{
         | "id":"$assetIdentifierOne",
         | "parentId":"$folderIdentifierOne",
         | "title":"TestAssetTitle",
         | "type":"Asset",
         | "name":"TestAssetName",
         | "fileSize":null,
         | "originalMetadataFiles": ${write(originalMetadataFilesOne)}
         |},
         |{
         | "id":"$assetIdentifierTwo",
         | "parentId":"$folderIdentifierTwo",
         | "title":"TestAssetTitle",
         | "type":"Asset",
         | "name":"TestAssetName",
         | "fileSize":null,
         | "originalMetadataFiles": ${write(originalMetadataFilesTwo)}
         |},
         |{"id":"$docxIdentifierOne","parentId":"$assetIdentifierOne","title":"Test","type":"File","name":"Test.docx","fileSize":1, "customMetadataAttribute1": "customMetadataValue1"},
         |{"id":"$metadataFileIdentifierOne","parentId":"$assetIdentifierOne","title":"","type":"File","name":"TEST-metadata.json","fileSize":2},
         |{"id":"$docxIdentifierTwo","parentId":"$assetIdentifierTwo","title":"Test","type":"File","name":"Test.docx","fileSize":1, "customMetadataAttribute1": "customMetadataValue1"},
         |{"id":"$metadataFileIdentifierTwo","parentId":"$assetIdentifierTwo","title":"","type":"File","name":"TEST-metadata.json","fileSize":2}]
         |""".stripMargin.replaceAll("\n", "")

    MetadataResponse(
      metadata,
      Identifiers(folderIdentifierOne, assetIdentifierOne, docxIdentifierOne, metadataFileIdentifierOne, originalMetadataFilesOne),
      Identifiers(folderIdentifierTwo, assetIdentifierTwo, docxIdentifierTwo, metadataFileIdentifierTwo, originalMetadataFilesTwo)
    )
  }

  def checkDynamoItems(tableRequestItems: List[Obj], expectedTable: DynamoFilesTableItem): Unit = {
    val items = tableRequestItems
      .filter(item => item("id").str == expectedTable.id.toString)

    items.size should equal(1)
    val item = items.head
    def list(name: String): List[String] = item.value.get(name).map(_.arr.toList.map(_.str)).getOrElse(Nil)
    def num(name: String) = item.value.get(name).flatMap(_.numOpt).map(_.toLong)
    def strOpt(name: String) = item.value.get(name).map(_.str)
    def str(name: String) = strOpt(name).getOrElse("")
    str("id") should equal(expectedTable.id.toString)
    str("name") should equal(expectedTable.name)
    str("title") should equal(expectedTable.title)
    expectedTable.id_Code.map(id_Code => str("id_Code") should equal(id_Code))
    str("parentPath") should equal(expectedTable.parentPath)
    str("batchId") should equal(expectedTable.batchId)
    str("description") should equal(expectedTable.description)
    num("childCount").get should equal(expectedTable.childCount)
    num("fileSize") should equal(expectedTable.fileSize)
    num("ttl").get should equal(expectedTable.ttl)
    str("type") should equal(expectedTable.`type`.toString)
    strOpt("customMetadataAttribute1") should equal(expectedTable.customMetadataAttribute1)
    list("originalMetadataFiles") should equal(expectedTable.originalMetadataFiles)
  }

  def dependencies(s3Ref: Ref[IO, List[S3Object]], dynamoRef: Ref[IO, List[Obj]], discoveryServiceException: Boolean = false): Dependencies = {
    val creds: StaticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
    val uuidsIterator: Iterator[String] = uuids.iterator
    val randomUuidGenerator: () => UUID = () => UUID.fromString(uuidsIterator.next())
    val dynamo = mockDynamoClient(dynamoRef)
    val discoveryService = createDiscoveryService(discoveryServiceException, randomUuidGenerator)
    val s3Client = mocks3Client(s3Ref)
    val metadataService: MetadataService = new MetadataService(s3Client, discoveryService)
    val fixedTime = () => Instant.parse("2024-01-01T00:00:00.00Z")
    Dependencies(metadataService, dynamo, mocks3Client(s3Ref), fixedTime)
  }

  def createDiscoveryService(discoveryServiceException: Boolean, randomUuidGenerator: () => UUID): DiscoveryService[IO] =
    if !discoveryServiceException then
      new DiscoveryService[IO]:
        def generateDiscoveryCollectionAsset(col: String): DiscoveryCollectionAsset =
          DiscoveryCollectionAsset(col, DiscoveryScopeContent(Option(s"TestDescription$col with 0")), Option(s"Test Title $col"))

        def generateJson: Obj = Obj("id" -> randomUuidGenerator().toString, "type" -> "ArchiveFolder", "name" -> "Test name")

        override def getDepartmentAndSeriesItems(batchId: String, departmentAndSeriesAssets: DepartmentAndSeriesCollectionAssets): DepartmentAndSeriesTableItems =
          DiscoveryService[IO]("baseUrl", randomUuidGenerator).unsafeRunSync().getDepartmentAndSeriesItems(batchId, departmentAndSeriesAssets)

        override def getDiscoveryCollectionAssets(series: Option[String]): IO[DepartmentAndSeriesCollectionAssets] =
          if discoveryServiceException then IO.raiseError(new Exception("Exception when sending request: GET http://localhost:9015/API/records/v1/collection/A"))
          else if series.isEmpty then IO.pure(DepartmentAndSeriesCollectionAssets(None, None))
          else
            val department = series.get.split(" ").head
            IO.pure(DepartmentAndSeriesCollectionAssets(Option(generateDiscoveryCollectionAsset(department)), Option(generateDiscoveryCollectionAsset(series.get))))
    else
      val backendStub = SttpBackendStub(CatsMonadAsyncError[IO]()).whenAnyRequest.thenRespondServerError()
      DiscoveryService[IO]("https://example.com", backendStub, randomUuidGenerator)

  case class S3Object(bucket: String, key: String, fileContent: String)

  def mockDynamoClient(ref: Ref[IO, List[Obj]]): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = notImplemented

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] =
      IO.raiseWhen(tableName == "invalid")(new Exception("Table invalid does not exist")) >>
        ref
          .update { requests =>
            requests ++ items.map(_.asInstanceOf[Obj])
          }
          .map(_ => List(BatchWriteItemResponse.builder.build))

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      notImplemented

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = notImplemented

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = notImplemented

  def mocks3Client(ref: Ref[IO, List[S3Object]]): DAS3Client[IO] =
    new DAS3Client[IO]():
      override def download(bucket: String, key: String): IO[Publisher[ByteBuffer]] = ref.get.flatMap { objects =>
        IO.fromOption {
          objects
            .find(obj => obj.bucket == bucket && obj.key == key)
            .map(obj => Flux.just(ByteBuffer.wrap(obj.fileContent.getBytes)))
        }(new Exception(s"Key $key not found in bucket $bucket"))
      }

      override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] = notImplemented

      override def upload(bucket: String, key: String, publisher: Publisher[ByteBuffer]): IO[CompletedUpload] = for {
        _ <- IO(assert(bucket == "testInputStateBucket"))
        actualUploadedJson <- publisher
          .toStreamBuffered[IO](1024)
          .map(_.array().map(_.toChar).mkString)
          .compile
          .string
        _ <- ref.update(s3Objects => S3Object(bucket, key, actualUploadedJson) :: s3Objects)

      } yield CompletedUpload.builder.response(PutObjectResponse.builder.build).build

      override def headObject(bucket: String, key: String): IO[HeadObjectResponse] = notImplemented

      override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = notImplemented

      override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[SdkPublisher[String]] = notImplemented

      override def listObjects(bucket: String, prefix: Option[String]): IO[ListObjectsV2Response] = notImplemented
}
