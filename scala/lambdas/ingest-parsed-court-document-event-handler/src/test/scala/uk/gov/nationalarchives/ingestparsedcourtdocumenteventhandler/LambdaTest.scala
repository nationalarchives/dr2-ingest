package uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.http.RequestMethod
import io.circe.{Decoder, DecodingFailure, Printer}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.sfn.SfnAsyncClient
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.FileProcessor.*
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.SeriesMapper.{Court, Output}
import io.circe.parser.decode
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2, TableFor4}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client, DASFNClient}
import uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.Lambda.Dependencies

import java.net.URI
import java.time.OffsetDateTime
import java.util.{Base64, HexFormat, UUID}
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.*

class LambdaTest extends AnyFlatSpec with BeforeAndAfterEach with TableDrivenPropertyChecks {
  case class SFNRequest(stateMachineArn: String, name: String, input: String)

  case class DDBRequest(TableName: String, Item: DDBItem, ConditionExpression: String)
  case class DDBItem(ioId: DDBValue, batchId: DDBValue, message: DDBValue)
  case class DDBValue(S: String)

  val config: Config = Config("", "", "")

  val reference = "TEST-REFERENCE"
  val uuidsAndChecksum: List[(String, String)] = List(
    ("c7e6b27f-5778-4da8-9b83-1b64bbccbd03", "71"),
    ("61ac0166-ccdf-48c4-800f-29e5fba2efda", "81"),
    ("4e6bac50-d80a-4c68-bd92-772ac9701f14", "91"),
    ("c2e7866e-5e94-4b4e-a49f-043ad937c18a", "A1"),
    ("27a9a6bb-a023-4cab-8592-39b44761a30a", "B1")
  )

  val metadataFilesAndChecksum: (String, String) = ("metadata.json", "01")

  override def beforeEach(): Unit = {
    sfnServer.resetAll()
    s3Server.resetAll()
    dynamoServer.resetAll()
    sfnServer.start()
    s3Server.start()
    dynamoServer.start()
  }

  val s3Server = new WireMockServer(9011)
  val sfnServer = new WireMockServer(9012)
  val dynamoServer = new WireMockServer(9013)
  val testOutputBucket = "outputBucket"
  val inputBucket = "inputBucket"
  private def packageAvailable(s3Key: String): TREInput = TREInput(
    TREInputParameters("status", "TEST-REFERENCE", skipSeriesLookup = false, inputBucket, s3Key)
  )
  private def event(s3Key: String = "test.tar.gz"): SQSEvent = createEvent(
    packageAvailable(s3Key).asJson.printWith(Printer.noSpaces)
  )
  val expectedDeleteRequestXml: String =
    """<?xml version="1.0" encoding="UTF-8"?><Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      |<Object><Key>c7e6b27f-5778-4da8-9b83-1b64bbccbd03</Key></Object></Delete>""".stripMargin.replaceAll("\\n", "")

  private def read[T](jsonString: String)(using enc: Decoder[T]): T =
    decode[T](jsonString).toOption.get

  private def runLambdaAndReturnStepFunctionRequest(metadataJsonOpt: Option[String] = None) = {
    stubAWSRequests(inputBucket, metadataJsonOpt = metadataJsonOpt)
    new Lambda().handler(event(), config, dependencies).unsafeRunSync()

    val sfnEvent = sfnServer.getAllServeEvents.asScala.head
    read[SFNRequest](sfnEvent.getRequest.getBodyAsString)
  }

  private def runLambdaAndReturnDynamoDbRequest(metadataJsonOpt: Option[String] = None) = {
    stubAWSRequests(inputBucket, metadataJsonOpt = metadataJsonOpt)
    new Lambda().handler(event(), config, dependencies).unsafeRunSync()

    val ddbEvent = dynamoServer.getAllServeEvents.asScala.head
    read[DDBRequest](ddbEvent.getRequest.getBodyAsString)
  }

  def dependencies: Dependencies = {
    val credentials: StaticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
    val s3AsyncClient: S3AsyncClient = S3AsyncClient
      .crtBuilder()
      .endpointOverride(URI.create("http://localhost:9011"))
      .region(Region.EU_WEST_2)
      .credentialsProvider(credentials)
      .build()

    val sfnAsyncClient: SfnAsyncClient = SfnAsyncClient
      .builder()
      .endpointOverride(URI.create("http://localhost:9012"))
      .region(Region.EU_WEST_2)
      .credentialsProvider(credentials)
      .build()

    val dynamoAsyncClient: DynamoDbAsyncClient = DynamoDbAsyncClient
      .builder()
      .endpointOverride(URI.create("http://localhost:9013"))
      .region(Region.EU_WEST_2)
      .credentialsProvider(credentials)
      .build()

    val s3: DAS3Client[IO] = DAS3Client[IO](s3AsyncClient)
    val sfn: DASFNClient[IO] = new DASFNClient(sfnAsyncClient)
    val dynamo: DADynamoDBClient[IO] = DADynamoDBClient[IO](dynamoAsyncClient)
    val seriesMapper: SeriesMapper = new SeriesMapper(Set(Court("COURT", "TEST", "TEST SERIES")))
    val uuidsIterator: Iterator[String] = uuidsAndChecksum.map(_._1).iterator
    Dependencies(s3, sfn, dynamo, () => UUID.fromString(uuidsIterator.next()), seriesMapper)
  }

  def createEvent(body: String): SQSEvent = {
    val sqsEvent = new SQSEvent()
    val record = new SQSMessage()
    record.setBody(body)
    sqsEvent.setRecords(List(record).asJava)
    sqsEvent
  }

  def convertChecksumToS3Format(cs: String): String =
    Base64.getEncoder
      .encode(HexFormat.of().parseHex(cs))
      .map(_.toChar)
      .mkString

  def stubAWSRequests(
      inputBucket: String,
      tarFileName: String = "test.tar.gz",
      metadataJsonOpt: Option[String] = None
  ): Unit = {
    val bytes = getClass.getResourceAsStream(s"/files/$tarFileName").readAllBytes()
    sfnServer.stubFor(post(urlEqualTo("/")).willReturn(ok()))
    s3Server.stubFor(
      head(urlEqualTo(s"/$inputBucket/$tarFileName"))
        .willReturn(
          ok()
            .withHeader("Content-Length", bytes.length.toString)
            .withHeader("ETag", "abcde")
        )
    )
    s3Server.stubFor(
      get(urlEqualTo(s"/$inputBucket/$tarFileName"))
        .willReturn(ok.withBody(bytes))
    )

    val metadataJson: String = metadataJsonOpt.getOrElse(
      s"""{"parameters":{"TDR": {"Document-Checksum-sha256": "abcde", "Source-Organization": "test-organisation",
         | "Internal-Sender-Identifier": "test-identifier","Consignment-Export-Datetime": "2023-10-31T13:40:54Z", "UUID": "24190792-a2e5-43a0-a9e9-6a0580905d90"},
         |"TRE":{"reference":"$reference","payload":{"filename":"Test.docx"}},
         |"PARSER":{"cite":"cite","uri":"https://example.com/id/court/2023/","court":"test","date":"2023-07-26","name":"test"}}}""".stripMargin
    )

    val (file, checksum) = metadataFilesAndChecksum
    s3Server.stubFor(
      put(urlEqualTo(s"/$testOutputBucket/$reference/$file"))
        .willReturn(ok().withHeader("x-amz-checksum-sha256", convertChecksumToS3Format(checksum)))
    )

    uuidsAndChecksum.foreach { case (uuid, checksum) =>
      s3Server.stubFor(
        put(urlEqualTo(s"/$testOutputBucket/$uuid"))
          .willReturn(ok().withHeader("x-amz-checksum-sha256", convertChecksumToS3Format(checksum)))
      )
      s3Server.stubFor(
        put(urlEqualTo(s"/$testOutputBucket/$reference/$uuid"))
          .willReturn(ok())
      )
      s3Server.stubFor(
        get(urlEqualTo(s"/$testOutputBucket/$uuid"))
          .willReturn(okJson(metadataJson))
      )

      s3Server.stubFor(
        post(urlEqualTo(s"/$testOutputBucket?delete"))
          .withRequestBody(equalToXml(expectedDeleteRequestXml))
          .willReturn(ok())
      )
      s3Server.stubFor(
        head(urlEqualTo(s"/$testOutputBucket/$uuid"))
          .willReturn(
            ok()
              .withHeader("Content-Length", bytes.length.toString)
              .withHeader("ETag", "abcde")
          )
      )
    }

    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.TableName", equalTo("test-table")))
        .willReturn(ok())
    )
  }

  "the lambda" should "download the .tar.gz file from the input bucket" in {
    stubAWSRequests(inputBucket)
    new Lambda().handler(event(), config, dependencies).unsafeRunSync()
    val serveEvents = s3Server.getAllServeEvents.asScala
    serveEvents.count(e => e.getRequest.getUrl == s"/$inputBucket/test.tar.gz" && e.getRequest.getMethod == RequestMethod.GET) should equal(1)
  }

  "the lambda" should "write the metadata and files to the output bucket" in {
    stubAWSRequests(inputBucket)
    new Lambda().handler(event(), config, dependencies).unsafeRunSync()
    val serveEvents = s3Server.getAllServeEvents.asScala

    def countPutEvents(name: String) = serveEvents.count(e => e.getRequest.getUrl == s"/$testOutputBucket/$reference/$name" && e.getRequest.getMethod == RequestMethod.PUT)

    countPutEvents(metadataFilesAndChecksum._1) should equal(1)
  }

  val citeTable: TableFor2[Option[String], List[IdField]] = Table(
    ("potentialCite", "idFields"),
    (None, List(IdField("URI", "https://example.com/id/court/2023/"))),
    (Option("\"cite\""), List(IdField("Code", "cite"), IdField("Cite", "cite")))
  )

  forAll(citeTable) { (potentialCite, idFields) =>
    "the lambda" should s"write the correct metadata files to S3 with a cite ${potentialCite.orNull}" in {
      val tdrUuid = UUID.fromString("24190792-a2e5-43a0-a9e9-6a0580905d90")
      val metadataJson: String =
        s"""{"parameters":{"TDR": {"Document-Checksum-sha256": "abcde", "Source-Organization": "test-organisation",
           | "Internal-Sender-Identifier": "test-identifier","Consignment-Export-Datetime": "2023-10-31T13:40:54Z", "UUID": "$tdrUuid"},
           |"TRE":{"reference":"$reference","payload":{"filename":"Test.docx"}},
           |"PARSER":{"cite":${potentialCite.orNull},"uri":"https://example.com/id/court/2023/","court":"test","date":"2023-07-26","name":"test"}}}""".stripMargin
      stubAWSRequests(inputBucket, metadataJsonOpt = Option(metadataJson))
      new Lambda().handler(event(), config, dependencies).unsafeRunSync()
      val serveEvents = s3Server.getAllServeEvents.asScala

      def getContentOfAllMetadataFilePutEvents: List[String] = serveEvents
        .map(_.getRequest)
        .filter { ev =>
          val outputBucket = s"/$testOutputBucket/$reference/"
          ev.getUrl
            .contains(outputBucket) && ev.getUrl.stripPrefix(outputBucket).contains(".") && ev.getMethod == RequestMethod.PUT
        }
        .map(_.getBodyAsString.split("\r\n")(1).trim)
        .toList

      val folderId = UUID.fromString("c2e7866e-5e94-4b4e-a49f-043ad937c18a")
      val fileId = UUID.fromString("61ac0166-ccdf-48c4-800f-29e5fba2efda")
      val metadataFileId = UUID.fromString("4e6bac50-d80a-4c68-bd92-772ac9701f14")
      val expectedAssetMetadata = AssetMetadataObject(
        tdrUuid,
        Option(folderId),
        "Test.docx",
        "24190792-a2e5-43a0-a9e9-6a0580905d90",
        List(fileId),
        List(metadataFileId),
        Some("test"),
        "test-organisation",
        OffsetDateTime.parse("2023-10-31T13:40:54Z"),
        "TRE: FCL Parser workflow",
        "Born Digital",
        "FCL",
        List(
          Option(IdField("UpstreamSystemReference", reference)),
          Option(IdField("URI", "https://example.com/id/court/2023/")),
          potentialCite.map(_ => IdField("NeutralCitation", "cite")),
          Option(IdField("ConsignmentReference", "test-identifier")),
          Option(IdField("UpstreamSystemReference", "TEST-REFERENCE")),
          Option(IdField("RecordID", "24190792-a2e5-43a0-a9e9-6a0580905d90"))
        ).flatten
      )
      val expectedFileMetadata = List(
        FileMetadataObject(fileId, Option(tdrUuid), "Test", 1, "Test.docx", 15684, RepresentationType.Preservation, 1, URI.create(s"s3://$testOutputBucket/$fileId"), "abcde"),
        FileMetadataObject(
          metadataFileId,
          Option(tdrUuid),
          "",
          2,
          "TRE-TEST-REFERENCE-metadata.json",
          215,
          RepresentationType.Preservation,
          1,
          URI.create(s"s3://$testOutputBucket/$metadataFileId"),
          "91"
        )
      )

      val expectedFolderMetadata =
        FolderMetadataObject(
          folderId,
          None,
          Option("test"),
          "https://example.com/id/court/2023/",
          "TEST SERIES",
          if potentialCite.isDefined then idFields :+ IdField("URI", "https://example.com/id/court/2023/") else idFields
        )
      val metadataList: List[MetadataObject] =
        List(expectedFolderMetadata, expectedAssetMetadata) ++ expectedFileMetadata
      val expectedMetadata = metadataList.asJson.printWith(Printer.noSpaces)

      val metadataFilePutEvents: List[String] = getContentOfAllMetadataFilePutEvents
      val expectedMetadataFileContents = ListMap(
        "metadata.json" -> expectedMetadata
      )
      val expectedContentAndActual = metadataFilePutEvents.zip(expectedMetadataFileContents.values)

      expectedContentAndActual.length should equal(metadataFilePutEvents.length)
      expectedContentAndActual.foreach { case (actualMetadataContent, expectedMetadataContent) =>
        actualMetadataContent should equal(expectedMetadataContent)
      }
    }
  }

  "the lambda" should "write the correct values to the lock table" in {
    val ddbRequest = runLambdaAndReturnDynamoDbRequest()

    ddbRequest.TableName should equal("test-table")
    ddbRequest.Item.ioId.S should equal("24190792-a2e5-43a0-a9e9-6a0580905d90")
    ddbRequest.Item.batchId.S should equal("TEST-REFERENCE")
    ddbRequest.Item.message.S should equal("{\"messageId\":\"27a9a6bb-a023-4cab-8592-39b44761a30a\"}")
    ddbRequest.ConditionExpression should equal("attribute_not_exists(ioId)")
  }

  "handler" should "return an error if the Dynamo API is unavailable" in {
    stubAWSRequests(inputBucket)
    dynamoServer.stop()
    val ex = intercept[Exception] {
      new Lambda().handler(event(), config, dependencies).unsafeRunSync()
    }

    ex.getMessage should equal("Unable to execute HTTP request: Connection refused: localhost/127.0.0.1:9013")
  }

  "the lambda" should "start the state machine execution with the correct parameters" in {
    val sfnRequest = runLambdaAndReturnStepFunctionRequest()
    val input = read[Output](sfnRequest.input)

    sfnRequest.stateMachineArn should equal("arn:aws:states:eu-west-2:123456789:stateMachine:StateMachineName")
    sfnRequest.name should equal("TEST-REFERENCE")

    input.batchId should equal("TEST-REFERENCE")
    input.metadataPackage.toString should equal("s3://outputBucket/TEST-REFERENCE/metadata.json")
  }

  val citeAndUri: TableFor4[Option[String], Option[String], Option[String], Option[String]] = Table(
    ("cite", "uri", "expectedSeries", "expectedDepartment"),
    (None, None, None, None),
    (None, Option(""""https://example.com/id/court/2023/""""), Option("TEST SERIES"), Option("TEST")),
    (Option(""""cite""""), None, None, None),
    (Option(""""cite""""), Option(""""https://example.com/id/court/2023/""""), Option("TEST SERIES"), Option("TEST"))
  )

  forAll(citeAndUri) { (cite, uri, expectedSeries, expectedDepartment) =>
    "the lambda" should s"start the state machine execution with a ${expectedSeries.orNull} series and ${expectedDepartment.orNull} department if the uri is ${uri.orNull} and the cite is ${cite.orNull}" in {
      val inputJson =
        s"""{"parameters":{
           |"TDR": {"Document-Checksum-sha256": "abcde", "Source-Organization": "test-organisation",
           | "Internal-Sender-Identifier": "test-identifier","Consignment-Export-Datetime": "2023-10-31T13:40:54Z", "UUID": "24190792-a2e5-43a0-a9e9-6a0580905d90"},
           |"TRE":{"reference":"$reference","payload":{"filename":"Test.docx"}},
           |"PARSER":{"cite": ${cite.orNull}, "uri":${uri.orNull},"name":"test"}}}""".stripMargin

      val sfnRequest = runLambdaAndReturnStepFunctionRequest(Option(inputJson))
      val input = read[Output](sfnRequest.input)

      sfnRequest.stateMachineArn should equal("arn:aws:states:eu-west-2:123456789:stateMachine:StateMachineName")
      sfnRequest.name should equal("TEST-REFERENCE")

      input.batchId should equal("TEST-REFERENCE")
      input.metadataPackage.toString should equal("s3://outputBucket/TEST-REFERENCE/metadata.json")
    }
  }

  "the lambda" should "send a request to delete the unused tre file" in {
    stubAWSRequests(inputBucket)
    new Lambda().handler(event(), config, dependencies).unsafeRunSync()
    val serveEvents = s3Server.getAllServeEvents.asScala
    val deleteObjectsEvents =
      serveEvents.filter(e => e.getRequest.getUrl == s"/$testOutputBucket?delete" && e.getRequest.getMethod == RequestMethod.POST)
    deleteObjectsEvents.size should equal(1)
    deleteObjectsEvents.head.getRequest.getBodyAsString should equal(expectedDeleteRequestXml)
  }

  "the lambda" should "error if the uri contains '/press-summary' but file name does not contain 'Press Summary of'" in {
    val metadataJson: String =
      s"""{"parameters":{"TDR": {"Document-Checksum-sha256": "abcde", "Source-Organization": "test-organisation",
         | "Internal-Sender-Identifier": "test-identifier","Consignment-Export-Datetime": "2023-10-31T13:40:54Z", "UUID": "24190792-a2e5-43a0-a9e9-6a0580905d90"},
         |"TRE":{"reference":"$reference","payload":{"filename":"Test.docx"}},
         |"PARSER":{"cite":"cite","uri":"https://example.com/id/court/press-summary/3/","court":"test","date":"2023-07-26","name":"test"}}}""".stripMargin

    stubAWSRequests(inputBucket, metadataJsonOpt = Option(metadataJson))
    val ex = intercept[Exception] {
      new Lambda().handler(event(), config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal("URI contains '/press-summary' but file does not start with 'Press Summary of '")
  }

  "the lambda" should "error if the input json is invalid" in {
    val eventWithInvalidJson = createEvent("{}")
    val ex = intercept[Exception] {
      new Lambda().handler(eventWithInvalidJson, config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal("DecodingFailure at .parameters: Missing required field")
  }

  "the lambda" should "error if the json in the metadata file is invalid" in {
    stubAWSRequests(inputBucket, metadataJsonOpt = Option("invalidJson"))
    val ex = intercept[Exception] {
      new Lambda().handler(event(), config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal("""expected json value got 'invali...' (line 1, column 1)""".stripMargin)
  }

  "the lambda" should "error if the json in the metadata file is missing required fields" in {
    stubAWSRequests(inputBucket, metadataJsonOpt = Option("{}"))
    val ex = intercept[DecodingFailure] {
      new Lambda().handler(event(), config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal("""DecodingFailure at .parameters: Missing required field""".stripMargin)
  }

  "the lambda" should "error if the json in the metadata file has a field with a non-optional value that is null" in {
    stubAWSRequests(
      inputBucket,
      metadataJsonOpt = Option(
        s"""{"parameters":{"TDR": {"Document-Checksum-sha256": null, "Source-Organization": "test-organisation", "UUID": "24190792-a2e5-43a0-a9e9-6a0580905d90",
         |"Internal-Sender-Identifier": "test-identifier", "Consignment-Export-Datetime": "2023-10-31T13:40:54Z"},
         |"TRE":{"reference":"$reference","payload":{"filename":"Test.docx"}},
         |"PARSER":{"cite":"cite","uri":"https://example.com","court":"test","date":"2023-07-26","name":"test"}}}""".stripMargin
      )
    )
    val ex = intercept[Exception] {
      new Lambda().handler(event(), config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal(
      """DecodingFailure at .parameters.TDR.Document-Checksum-sha256: Got value 'null' with wrong type, expecting string""".stripMargin
    )
  }

  "the lambda" should "error if the tar file contains a zero-byte file" in {
    val zeroBytesTarFileName = "zero-byte-test.tar.gz"
    stubAWSRequests(inputBucket, tarFileName = zeroBytesTarFileName)
    val ex = intercept[Exception] {
      new Lambda().handler(event(zeroBytesTarFileName), config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal("File id 'c7e6b27f-5778-4da8-9b83-1b64bbccbd03' size is 0")
  }

  "the lambda" should "error if S3 is unavailable" in {
    s3Server.stop()
    val ex = intercept[Exception] {
      new Lambda().handler(event(), config, dependencies).unsafeRunSync()
    }
    ex.getMessage should equal("Failed to send the request: socket connection refused.")
  }

  "the lambda" should "succeed even if the`skipSeriesLookup` parameter is missing from the 'parameters' json " in {
    val eventWithoutSkipParameter =
      """{"parameters":{"status":"status","reference":"TEST-REFERENCE","s3Bucket":"inputBucket","s3Key":"test.tar.gz"}}"""
    val event = createEvent(eventWithoutSkipParameter)
    stubAWSRequests(inputBucket)

    new Lambda().handler(event, config, dependencies)
    // All good, no "DecodingFailure at .skipSeriesLookup: Missing required field" thrown
  }
}
