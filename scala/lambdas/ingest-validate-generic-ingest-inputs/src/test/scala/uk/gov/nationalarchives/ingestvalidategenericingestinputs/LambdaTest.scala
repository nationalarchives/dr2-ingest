package uk.gov.nationalarchives.ingestvalidategenericingestinputs

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.interop.reactivestreams.*
import org.mockito.Mockito.{spy, times, verify}
import org.mockito.{ArgumentCaptor, Mockito}
import org.reactivestreams.Publisher
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.TableDrivenPropertyChecks
import reactor.core.publisher.Flux
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.http.SdkHttpResponse
import software.amazon.awssdk.services.s3.model.{DeleteObjectsResponse, HeadObjectResponse, PutObjectResponse}
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CompletedUpload}
import ujson.{Arr, Obj, Str, write}
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda.*
import uk.gov.nationalarchives.ingestvalidategenericingestinputs.testUtils.ExternalServicesTestUtils.{entryType, testValidMetadataJson}

import java.net.URI
import java.nio.ByteBuffer

class LambdaTest extends AnyFlatSpec with BeforeAndAfterEach with TableDrivenPropertyChecks {
  val config: Config = Config()
  val batchId = "TEST-ID"
  val metadataPackage: URI = URI.create("s3://outputBucket/TEST-REFERENCE/metadata.json")
  val input: Input = Input(batchId, metadataPackage)
  private val s3Client = getS3Client(metadataJsonAsObj = testValidMetadataJson())
  private val stringArgToCauseTestToFail = "This string will deliberately cause the test in the mock 'upload' method to fail if " +
    "'upload' method (called in the lambda) was invoked unintentionally OR if the method is expected to be invoked but a dev " +
    "forgot to pass a List[String] in to the 'stringsExpectedInJson' in the 'getS3Client' method"
  private val logMessageCallback = new (String => IO[Unit]) {
    def apply(message: String): IO[Unit] = IO.unit
  }

  def dependencies(s3: DAS3Client[IO]): Dependencies = Dependencies(s3)

  private def getS3Client(
      downloadResponseError: Boolean = false,
      metadataJsonAsObj: List[Obj],
      headObjectResponseError: Boolean = false,
      stringsExpectedInJson: List[String] = List(stringArgToCauseTestToFail),
      stringsNotExpectedInJson: List[String] = Nil
  ) = {
    new DAS3Client[IO]():
      override def download(bucket: String, key: String): IO[Publisher[ByteBuffer]] = {
        assert(bucket == "outputBucket")
        assert(key == "TEST-REFERENCE/metadata.json")
        if downloadResponseError then IO.raiseError(new Exception("Key could not be found")) else IO(getJsonAsByteBuffer(metadataJsonAsObj))
      }

      override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] = ???

      override def upload(bucket: String, key: String, publisher: Publisher[ByteBuffer]): IO[CompletedUpload] = for {
        _ <- IO(assert(bucket == "outputBucket"))
        _ <- IO(assert(key == "TEST-REFERENCE/metadata-entries-with-errors.json"))
        actualUploadedJson <- publisher
          .toStreamBuffered[IO](1024)
          .map(_.array().map(_.toChar).mkString)
          .compile
          .string
        _ <- IO(stringsExpectedInJson.foreach(expectedSubString => assert(actualUploadedJson.contains(expectedSubString))))
        _ <- IO(stringsNotExpectedInJson.foreach(unexpectedSubString => assert(!actualUploadedJson.contains(unexpectedSubString))))

      } yield CompletedUpload.builder.response(PutObjectResponse.builder.build).build

      override def headObject(bucket: String, key: String): IO[HeadObjectResponse] =
        if headObjectResponseError then IO.raiseError(new Exception("Key could not be found"))
        else
          val response = SdkHttpResponse.builder().statusCode(200).build()
          val headObjectResponse = HeadObjectResponse.builder()
          headObjectResponse.sdkHttpResponse(response).build
          IO(headObjectResponse.build)

      override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = ???

      override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[SdkPublisher[String]] = ???
  }

  private def getJsonAsByteBuffer(jsonAsObj: List[Obj]): Flux[ByteBuffer] = {
    val jsonAsString = write(jsonAsObj)
    Flux.just(ByteBuffer.wrap(jsonAsString.getBytes()))
  }

  class LambdaWithLogSpy(logMessageCallbackSpy: String => IO[Unit]) extends Lambda() {
    override def log(logCtx: Map[String, String]): String => IO[Unit] = logMessageCallbackSpy
  }

  "the lambda" should "not throw an exception nor log any entries if there were no error encountered" in {
    val logMessageCallbackSpy = spy(logMessageCallback)

    new LambdaWithLogSpy(logMessageCallbackSpy).handler(input, config, dependencies(s3Client)).unsafeRunSync()

    val logCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    verify(logMessageCallbackSpy, times(5)).apply(logCaptor.capture())
  }

  "the lambda" should "error if S3 returns an error when trying to download the json file" in {
    val s3Client = getS3Client(downloadResponseError = true, metadataJsonAsObj = testValidMetadataJson())
    val ex = intercept[Exception] {
      new Lambda().handler(input, config, dependencies(s3Client)).unsafeRunSync()
    }
    ex.getMessage should equal("Key could not be found")
  }

  "the lambda" should "call the 'checkJsonForMinimumObjects' method" in {
    val jsonWithoutAsset = testValidMetadataJson().filterNot(entry => entry(entryType).str == "Asset")

    val errorThatJsonShouldContain =
      """{"MinimumAssetsAndFilesError 1:":{"Invalid":[{"errorType":"MinimumAssetsAndFilesError","errorMessage":"$: """ +
        """must contain at least 1 element(s) that passes these validations: {\"title\":\"Asset\",\"description\":\"JSON must contain at least one object with all and only these properties, """ +
        """one of which is 'type': 'Asset'\",\"properties\":{\"type\":{\"type\":\"string\",\"const\":\"Asset\"}}}"}]}}"""

    val s3Client = getS3Client(metadataJsonAsObj = jsonWithoutAsset, stringsExpectedInJson = List(errorThatJsonShouldContain))
    val logMessageCallbackSpy = spy(logMessageCallback)

    val ex = intercept[JsonValidationException] {
      new LambdaWithLogSpy(logMessageCallbackSpy).handler(input, config, dependencies(s3Client)).unsafeRunSync()
    }

    verify(logMessageCallbackSpy).apply(
      "1 thing wrong with the structure of the metadata.json for batchId 'TEST-ID'; the results can be found here: outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json\n\n"
    )
    ex.getMessage.contains("1 thing wrong with the structure of the metadata.json for batchId 'TEST-ID'") should equal(true)
  }

  "the lambda" should "call the 'atLeastOneEntryWithSeriesAndNullParentErrors' method" in {
    val jsonWithoutAsset = testValidMetadataJson().filterNot(entry => entry(entryType).str == "ArchiveFolder")
    val errorThatJsonShouldContain =
      """{"AtLeastOneEntryWithSeriesAndNullParentError 1:":{"Invalid":[{"errorType":"AtLeastOneEntryWithSeriesAndNullParentError","errorMessage":"$: """ +
        """must contain at least 1 element(s) that passes these validations: {\"title\":\"At least one top-level entry in the JSON\",""" +
        """\"description\":\"There must be at least one object in the JSON that has both a series and a parent that is null\",""" +
        """\"properties\":{\"parentId\":{\"type\":\"null\",\"const\":null},\"series\":true},\"required\":[\"series\",\"parentId\"]}"}]}}"""
    val s3Client = getS3Client(metadataJsonAsObj = jsonWithoutAsset, stringsExpectedInJson = List(errorThatJsonShouldContain))
    val logMessageCallbackSpy = spy(logMessageCallback)

    val ex = intercept[Exception] {
      new LambdaWithLogSpy(logMessageCallbackSpy).handler(input, config, dependencies(s3Client)).unsafeRunSync()
    }

    verify(logMessageCallbackSpy).apply(
      "1 thing wrong with the structure of the metadata.json for batchId 'TEST-ID'; the results can be found here: outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json\n\n"
    )
    assert(
      ex.getMessage.contains(
        "1 thing wrong with the structure of the metadata.json for batchId 'TEST-ID'; the results can be found here: outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"
      )
    )
  }

  "the lambda" should "continue if S3 returns an error when trying to get the headObject at the location it has been given" +
    " it should instead add this error to the 'location' field of the entry" in {
      val expectedLocationJsonError1 =
        """"location":{"Invalid":[{"errorType":"NoFileAtS3LocationError","valueThatCausedError":"s3://test-source-bucket/d4f8613d-2d2a-420d-a729-700c841244f3","errorMessage":"Key could not be found"}]}"""
      val expectedLocationJsonError2 =
        """"location":{"Invalid":[{"errorType":"NoFileAtS3LocationError","valueThatCausedError":"s3://test-source-bucket/b0147dea-878b-4a25-891f-66eba66194ca","errorMessage":"Key could not be found"}]}"""
      val s3Client = getS3Client(
        headObjectResponseError = true,
        metadataJsonAsObj = testValidMetadataJson(),
        stringsExpectedInJson = List(expectedLocationJsonError1, expectedLocationJsonError2)
      )
      val logMessageCallbackSpy = spy(logMessageCallback)

      val ex = intercept[JsonValidationException] {
        new LambdaWithLogSpy(logMessageCallbackSpy).handler(input, config, dependencies(s3Client)).unsafeRunSync()
      }

      verify(logMessageCallbackSpy).apply(
        "2 entries (objects) in the metadata.json for batchId 'TEST-ID' have failed validation; the results can be found here: outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"
      )
      verify(logMessageCallbackSpy).apply(
        """{"id":"\"d4f8613d-2d2a-420d-a729-700c841244f3\"","fieldNamesWithErrors":"location","locationOfFile":"outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"}"""
      )
      verify(logMessageCallbackSpy).apply(
        """{"id":"\"b0147dea-878b-4a25-891f-66eba66194ca\"","fieldNamesWithErrors":"location","locationOfFile":"outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"}"""
      )
      ex.getMessage should equal(
        "2 entries (objects) in the metadata.json for batchId 'TEST-ID' have failed validation; the results can be found here: outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"
      )
    }

  "the lambda" should "call the 'checkFileNamesHaveExtensions' method with only metadata files" in {
    val entriesWithFileNamesWithoutExtensions = testValidMetadataJson().map { entry =>
      val typeOfEntry = entry(entryType).str
      if typeOfEntry == "File" then
        val entryName = entry("name").str
        val nameWithoutExtension = entryName.split('.').dropRight(1).mkString(".")
        Obj.from(entry.value ++ Map("name" -> Str(nameWithoutExtension)))
      else entry
    }

    val expectedExtensionMethodString = List(
      """"name":{"Invalid":[{"errorType":"MissingFileExtensionError","valueThatCausedError":"TDD-2023-ABC-metadata","errorMessage":"The file name does not have an extension at the end of it"}]}"""
    )
    val unexpectedExtensionMethodString = List(
      """"name":{"Invalid":[{"errorType":"MissingFileExtensionError","valueThatCausedError":"test name","errorMessage":"The file name does not have an extension at the end of it"}]}"""
    )

    val s3Client = getS3Client(
      metadataJsonAsObj = entriesWithFileNamesWithoutExtensions,
      stringsExpectedInJson = expectedExtensionMethodString,
      stringsNotExpectedInJson = unexpectedExtensionMethodString
    )
    val logMessageCallbackSpy = spy(logMessageCallback)

    val ex = intercept[JsonValidationException] {
      new LambdaWithLogSpy(logMessageCallbackSpy).handler(input, config, dependencies(s3Client)).unsafeRunSync()
    }

    verify(logMessageCallbackSpy).apply(
      "2 entries (objects) in the metadata.json for batchId 'TEST-ID' have failed validation; the results can be found here: outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"
    )
    verify(logMessageCallbackSpy).apply(
      """{"id":"\"d4f8613d-2d2a-420d-a729-700c841244f3\"","fieldNamesWithErrors":"name, title","locationOfFile":"outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"}"""
    )

    ex.getMessage should equal(
      "2 entries (objects) in the metadata.json for batchId 'TEST-ID' have failed validation; the results can be found here: outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"
    )
  }

  "the lambda" should "call the 'checkIfAllIdsAreUnique' and 'checkIfAllIdsAreUuids' methods" in {
    val unknownTypeEntry = testValidMetadataJson().collect {
      case entry if entry(entryType).str == "ContentFolder" => Obj.from(entry.value ++ Map("type" -> Str("UnknownType")))
    }

    val entriesWithoutUuidsAndUniqueIds = testValidMetadataJson(unknownTypeEntry).map { entry =>
      val typeOfEntry = entry(entryType).str
      if typeOfEntry == "ContentFolder" then Obj.from(entry.value ++ Map("id" -> Str("non-uuid id")))
      else if typeOfEntry == "File" then Obj.from(entry.value ++ Map("id" -> Str("29dc3d9b-e451-4a92-bbcd-88ba3a8d1935")))
      else entry
    }

    val expectedErrorsPerMethod = Map(
      "checkIfAllIdsAreUnique" ->
        List(
          """"id":{"Invalid":[{"errorType":"IdIsNotUniqueError","valueThatCausedError":"29dc3d9b-e451-4a92-bbcd-88ba3a8d1935","errorMessage":"This id occurs 2 times"}]}""",
          """"id":{"Invalid":[{"errorType":"IdIsNotUniqueError","valueThatCausedError":"29dc3d9b-e451-4a92-bbcd-88ba3a8d1935","errorMessage":"This id occurs 2 times"}]}"""
        ),
      "checkIfAllIdsAreUuids" ->
        List(
          """"id":{"Invalid":[{"errorType":"IdIsNotAUuidError","valueThatCausedError":"non-uuid id","errorMessage":"The id is not a valid UUID"}]}"""
        )
    )

    val expectedJsonErrors = expectedErrorsPerMethod.values.toList.flatten

    val s3Client = getS3Client(metadataJsonAsObj = entriesWithoutUuidsAndUniqueIds, stringsExpectedInJson = expectedJsonErrors)
    val logMessageCallbackSpy = spy(logMessageCallback)

    val ex = intercept[JsonValidationException] {
      new LambdaWithLogSpy(logMessageCallbackSpy).handler(input, config, dependencies(s3Client)).unsafeRunSync()
    }

    verify(logMessageCallbackSpy).apply(
      "5 entries (objects) in the metadata.json for batchId 'TEST-ID' have failed validation; the results can be found here: outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"
    )
    verify(logMessageCallbackSpy).apply(
      """{"id":"\"27354aa8-975f-48d1-af79-121b9a349cbe\"","fieldNamesWithErrors":"type","locationOfFile":"outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"}"""
    )
    verify(logMessageCallbackSpy, times(2)).apply(
      """{"id":"29dc3d9b-e451-4a92-bbcd-88ba3a8d1935","fieldNamesWithErrors":"id","locationOfFile":"outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"}"""
    )
    verify(logMessageCallbackSpy).apply(
      """{"id":"\"b3bcfd9b-3fe6-41eb-8620-0cb3c40655d6\"","fieldNamesWithErrors":"originalMetadataFiles, originalFiles","locationOfFile":"outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"}"""
    )
    verify(logMessageCallbackSpy).apply(
      """{"id":"non-uuid id","fieldNamesWithErrors":"id","locationOfFile":"outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"}"""
    )

    ex.getMessage should equal(
      "5 entries (objects) in the metadata.json for batchId 'TEST-ID' have failed validation; the results can be found here: outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"
    )
  }

  "the lambda" should "call the 'checkIfEntriesHaveCorrectParentIds' and 'checkForCircularDependenciesInFolders' methods" in {
    val additionalArchiveFolders = testValidMetadataJson().collect {
      case entry if entry(entryType).str == "ArchiveFolder" =>
        List(
          Obj.from((entry.value ++ Map("id" -> Str("3ede334c-b1ea-4642-ba31-d4574b0ddf5b"), "parentId" -> Str("1c751e2f-3c9e-4c12-b06e-c0917b0ba3ec"))).toMap - "series"),
          Obj.from((entry.value ++ Map("id" -> Str("1c751e2f-3c9e-4c12-b06e-c0917b0ba3ec"), "parentId" -> Str("3ede334c-b1ea-4642-ba31-d4574b0ddf5b"))).toMap - "series")
        )
    }.flatten

    val entriesWithAdditionalFoldersAndAssetWithNoFiles = testValidMetadataJson(additionalArchiveFolders).map { entry =>
      if entry(entryType).str == "Asset" then Obj.from(entry.value ++ Map("originalFiles" -> Arr())) else entry
    }

    val expectedErrorsPerMethod = Map(
      "checkIfEntriesHaveCorrectParentIds" ->
        List(
          """"originalFiles":{"Invalid":[{"errorType":"HierarchyLinkingError","valueThatCausedError":"b0147dea-878b-4a25-891f-66eba66194ca","errorMessage":"There are files in the JSON that have the parentId of this Asset (b3bcfd9b-3fe6-41eb-8620-0cb3c40655d6) but do not appear in 'originalFiles'"}]}"""
        ),
      "checkForCircularDependenciesInFolders" ->
        List(
          """"parentId":{"Invalid":[{"errorType":"HierarchyLinkingError","valueThatCausedError":"3ede334c-b1ea-4642-ba31-d4574b0ddf5b","errorMessage":"Circular dependency! A parent entry (id '3ede334c-b1ea-4642-ba31-d4574b0ddf5b') references this entry as its parentId.\n\nThe breadcrumb trail looks like this 1c751e2f-3c9e-4c12-b06e-c0917b0ba3ec > 3ede334c-b1ea-4642-ba31-d4574b0ddf5b > 1c751e2f-3c9e-4c12-b06e-c0917b0ba3ec"}]}""",
          """"parentId":{"Invalid":[{"errorType":"HierarchyLinkingError","valueThatCausedError":"1c751e2f-3c9e-4c12-b06e-c0917b0ba3ec","errorMessage":"Circular dependency! A parent entry (id '1c751e2f-3c9e-4c12-b06e-c0917b0ba3ec') references this entry as its parentId.\n\nThe breadcrumb trail looks like this 3ede334c-b1ea-4642-ba31-d4574b0ddf5b > 1c751e2f-3c9e-4c12-b06e-c0917b0ba3ec > 3ede334c-b1ea-4642-ba31-d4574b0ddf5b"}]}"""
        )
    )

    val expectedJsonErrors = expectedErrorsPerMethod.values.toList.flatten

    val s3Client = getS3Client(metadataJsonAsObj = entriesWithAdditionalFoldersAndAssetWithNoFiles, stringsExpectedInJson = expectedJsonErrors)
    val logMessageCallbackSpy = spy(logMessageCallback)

    val ex = intercept[JsonValidationException] {
      new LambdaWithLogSpy(logMessageCallbackSpy).handler(input, config, dependencies(s3Client)).unsafeRunSync()
    }

    verify(logMessageCallbackSpy).apply(
      "3 entries (objects) in the metadata.json for batchId 'TEST-ID' have failed validation; the results can be found here: outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"
    )
    verify(logMessageCallbackSpy).apply(
      """{"id":"\"b3bcfd9b-3fe6-41eb-8620-0cb3c40655d6\"","fieldNamesWithErrors":"originalFiles","locationOfFile":"outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"}"""
    )
    verify(logMessageCallbackSpy).apply(
      """{"id":"\"1c751e2f-3c9e-4c12-b06e-c0917b0ba3ec\"","fieldNamesWithErrors":"parentId","locationOfFile":"outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"}"""
    )
    verify(logMessageCallbackSpy).apply(
      """{"id":"\"3ede334c-b1ea-4642-ba31-d4574b0ddf5b\"","fieldNamesWithErrors":"parentId","locationOfFile":"outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"}"""
    )

    ex.getMessage should equal(
      "3 entries (objects) in the metadata.json for batchId 'TEST-ID' have failed validation; the results can be found here: outputBucket/TEST-REFERENCE/metadata-entries-with-errors.json"
    )
  }
}
