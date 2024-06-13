package uk.gov.nationalarchives.ingestassetopexcreator

import cats.effect.unsafe.implicits.global
import com.github.tomakehurst.wiremock.WireMockServer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.ingestassetopexcreator.Lambda.Config
import uk.gov.nationalarchives.ingestassetopexcreator.testUtils.ExternalServicesTestUtils

import scala.jdk.CollectionConverters.*
import scala.xml.XML

class LambdaTest extends AnyFlatSpec with BeforeAndAfterEach {
  val dynamoServer = new WireMockServer(9003)
  val s3Server = new WireMockServer(9004)
  private val testUtils = new ExternalServicesTestUtils(dynamoServer, s3Server)
  private val config: Config = Config("test-table", "test-gsi", "test-destination-bucket")
  import testUtils._

  override def beforeEach(): Unit = {
    dynamoServer.start()
    s3Server.start()
  }

  override def afterEach(): Unit = {
    dynamoServer.resetAll()
    s3Server.resetAll()
    dynamoServer.stop()
    s3Server.stop()
  }

  "handler" should "return an error if the asset is not found in dynamo" in {
    stubGetRequest(emptyDynamoGetResponse)
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"No asset found for $assetId and $batchId")
  }

  "handler" should "return an error if no children are found for the asset" in {
    stubGetRequest(dynamoGetResponse(0))
    stubPostRequest(emptyDynamoPostResponse)

    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"No children found for $assetId and $batchId")
  }

  "handler" should "return an error if childCount is higher than the number of rows returned" in {
    stubGetRequest(dynamoGetResponse(3))
    stubPostRequest(dynamoPostResponse)

    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"Asset id $assetId: has 3 children in the files table but found 2 children in the Preservation system")
  }

  "handler" should "return an error if the dynamo entry does not have a type of 'Asset'" in {
    stubGetRequest(dynamoGetResponse().replace(""""S": "Asset"""", """"S": "ArchiveFolder""""))
    stubPostRequest(emptyDynamoPostResponse)

    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(s"Object $assetId is of type ArchiveFolder and not 'Asset'")
  }

  "handler" should "pass the correct id to dynamo getItem" in {
    stubGetRequest(emptyDynamoGetResponse)
    intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    val serveEvents = dynamoServer.getAllServeEvents.asScala
    serveEvents.size should equal(1)
    serveEvents.head.getRequest.getBodyAsString should equal(s"""{"RequestItems":{"test-table":{"Keys":[{"id":{"S":"$assetId"}}]}}}""")
  }

  "handler" should "pass the correct parameters to dynamo for the query request" in {
    stubGetRequest(dynamoGetResponse())
    stubPostRequest(emptyDynamoPostResponse)
    intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    val serveEvents = dynamoServer.getAllServeEvents.asScala
    val queryEvent = serveEvents.head
    val requestBody = queryEvent.getRequest.getBodyAsString
    val expectedRequestBody =
      """{"TableName":"test-table","IndexName":"test-gsi","KeyConditionExpression":"#A = :batchId AND #B = :parentPath",""" +
        s""""ExpressionAttributeNames":{"#A":"batchId","#B":"parentPath"},"ExpressionAttributeValues":{":batchId":{"S":"TEST-ID"},":parentPath":{"S":"$assetParentPath/$assetId"}}}"""
    requestBody should equal(expectedRequestBody)
  }

  "handler" should "copy the correct child assets from source to destination" in {
    stubGetRequest(dynamoGetResponse())
    stubPostRequest(dynamoPostResponse)
    val (sourceJson, destinationJson) = stubJsonCopyRequest()
    val (sourceDocx, destinationDocx) = stubDocxCopyRequest()
    stubPutRequest()

    new Lambda().handler(input, config, dependencies).unsafeRunSync()

    def checkCopyRequest(source: String, destination: String) = {
      val s3CopyRequest = s3Server.getAllServeEvents.asScala.filter(_.getRequest.getUrl == destination).head.getRequest
      s3CopyRequest.getUrl should equal(destination)
      s3CopyRequest.getHost should equal("test-destination-bucket.localhost")
      s3CopyRequest.getHeader("x-amz-copy-source") should equal(s"test-source-bucket$source")
    }
    checkCopyRequest(sourceJson, destinationJson)
    checkCopyRequest(sourceDocx, destinationDocx)
  }

  "handler" should "upload the xip and opex files" in {
    stubGetRequest(dynamoGetResponse())
    stubPostRequest(dynamoPostResponse)
    val (xipPath, opexPath) = stubPutRequest()
    stubJsonCopyRequest()
    stubDocxCopyRequest()

    new Lambda().handler(input, config, dependencies).unsafeRunSync()

    val s3CopyRequests = s3Server.getAllServeEvents.asScala
    s3CopyRequests.count(_.getRequest.getUrl == xipPath) should equal(1)
    s3CopyRequests.count(_.getRequest.getUrl == opexPath) should equal(1)
  }

  "handler" should "write the xip content objects in the correct order" in {
    stubGetRequest(dynamoGetResponse())
    stubPostRequest(dynamoPostResponse)
    val (xipPath, _) = stubPutRequest()
    stubJsonCopyRequest()
    stubDocxCopyRequest()

    new Lambda().handler(input, config, dependencies).unsafeRunSync()

    val s3CopyRequests = s3Server.getAllServeEvents.asScala
    val xipString = s3CopyRequests.filter(_.getRequest.getUrl == xipPath).head.getRequest.getBodyAsString.split('\n').tail.dropRight(4).mkString("\n")
    val contentObjects = XML.loadString(xipString) \ "Representation" \ "ContentObjects" \ "ContentObject"
    contentObjects.head.text should equal(childIdDocx.toString)
    contentObjects.last.text should equal(childIdJson.toString)
  }

  "handler" should "upload the correct opex file to s3" in {
    stubGetRequest(dynamoGetResponse())
    stubPostRequest(dynamoPostResponse)

    val (_, opexPath) = stubPutRequest()
    stubJsonCopyRequest()
    stubDocxCopyRequest()

    new Lambda().handler(input, config, dependencies).unsafeRunSync()

    val s3UploadRequests = s3Server.getAllServeEvents.asScala
    val opexString = s3UploadRequests.filter(_.getRequest.getUrl == opexPath).head.getRequest.getBodyAsString.split('\n').tail.dropRight(3).mkString("\n")
    val opexXml = XML.loadString(opexString)
    opexXml should equal(expectedOpex)
  }

  "handler" should "return an error if the Dynamo API is unavailable" in {
    dynamoServer.stop()
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal("Unable to execute HTTP request: Connection refused: localhost/127.0.0.1:9003")
  }

  "handler" should "return an error if the S3 API is unavailable" in {
    s3Server.stop()
    stubGetRequest(dynamoGetResponse())
    stubPostRequest(dynamoPostResponse)
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal("Failed to send the request: socket connection refused.")
  }
}
