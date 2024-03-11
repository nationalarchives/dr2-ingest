package uk.gov.nationalarchives.testUtils

import cats.effect.IO
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.reactivestreams.Publisher
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{CommonPrefix, ListObjectsV2Request, ListObjectsV2Response}
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher
import software.amazon.awssdk.transfer.s3.model.CompletedUpload
import uk.gov.nationalarchives.{DAS3Client, Lambda}

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters.SeqHasAsJava

class ExternalServicesTestUtils extends AnyFlatSpec {
  def generateMockSdkPublisherWithPrefixes(commonPrefixStrings: List[String]): IO[SdkPublisher[String]] = {
    val commonPrefixes = commonPrefixStrings.map(prefix => CommonPrefix.builder.prefix(prefix).build).asJava
    val asyncClientMock = mock[S3AsyncClient]

    val listObjectsV2Request = ListObjectsV2Request.builder
      .bucket("testBucket")
      .delimiter("/")
      .prefix("testPrefix/")
      .build

    val listObjectsV2Response =
      ListObjectsV2Response.builder.commonPrefixes(commonPrefixes).build()
    val listObjectsV2Publisher = new ListObjectsV2Publisher(asyncClientMock, listObjectsV2Request)
    when(asyncClientMock.listObjectsV2(any[ListObjectsV2Request]))
      .thenReturn(CompletableFuture.completedFuture(listObjectsV2Response))

    IO(listObjectsV2Publisher.commonPrefixes().map(_.prefix()))
  }

  case class MockLambda(sdkPublisher: IO[SdkPublisher[String]], s3UploadResult: IO[CompletedUpload]) extends Lambda() {
    private val mockS3Client: DAS3Client[IO] = mock[DAS3Client[IO]]
    override val dAS3Client: DAS3Client[IO] = {
      when(
        mockS3Client.listCommonPrefixes(
          any[String],
          any[String]
        )
      ).thenReturn(sdkPublisher)
      when(
        mockS3Client.upload(
          any[String],
          any[String],
          any[Long],
          any[Publisher[ByteBuffer]]
        )
      ).thenReturn(s3UploadResult)
      mockS3Client
    }

    def verifyInvocationsAndArgumentsPassed(
        numberOfUploads: Int
    ): Unit = {
      val stagingCacheBucketCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
      val keysPrefixedWithCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

      val uploadBucketCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
      val keyToUploadCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
      val opexFileContentLengthCaptor: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

      verify(mockS3Client, times(1)).listCommonPrefixes(
        stagingCacheBucketCaptor.capture(),
        keysPrefixedWithCaptor.capture()
      )

      stagingCacheBucketCaptor.getValue should be("stagingCacheBucketName")
      keysPrefixedWithCaptor.getValue should be("opex/9e32383f-52a7-4591-83dc-e3e598a6f1a7/")

      verify(mockS3Client, times(numberOfUploads)).upload(
        uploadBucketCaptor.capture(),
        keyToUploadCaptor.capture(),
        opexFileContentLengthCaptor.capture(),
        any[Publisher[ByteBuffer]]
      )

      if (numberOfUploads > 0) {
        val keysToUpload: String = keyToUploadCaptor.getValue

        uploadBucketCaptor.getValue should be("stagingCacheBucketName")
        keysToUpload should equal("opex/9e32383f-52a7-4591-83dc-e3e598a6f1a7/9e32383f-52a7-4591-83dc-e3e598a6f1a7.opex")
        opexFileContentLengthCaptor.getValue should be(346)

      }
    }
  }
}
