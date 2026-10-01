package uk.gov.nationalarchives.preingestpaimporter

import cats.syntax.all.*
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.{Decoder, Encoder}
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.preingestpaimporter.Lambda.{*, given}
import uk.gov.nationalarchives.preingestpaimporter.TestUtils.*

import java.util.UUID

class LambdaTest extends AnyFlatSpec with EitherValues {

  "lambda" should "copy the files to s3 and upload the modified json" in {
    val uuid = UUID.randomUUID
    val fileId = UUID.randomUUID
    val input = List(
      Data(
        "HLAB/1",
        uuid,
        fileId,
        None,
        "fileName",
        "ABCD/E/F",
        "/a/file/path",
        "iaid",
        "digitalAssetSource",
        "checksum",
      )
    )

    val output = runLambda(input.asJson.noSpaces)

    val uploadedMetadata = decode[List[Data]](output.metadataMap(s"$uuid.metadata")).value.head
    uploadedMetadata.series should equal("YHLA/1")
    uploadedMetadata.fileReference should equal("YABC/E/F")
    uploadedMetadata.uuid should equal(uuid)
    uploadedMetadata.fileId should equal(fileId)
    uploadedMetadata.description should equal(None)
    uploadedMetadata.fileName should equal("fileName")
    uploadedMetadata.digitalAssetSource should equal("digitalAssetSource")
    uploadedMetadata.clientSideOriginalFilepath should equal("clientSideOriginalFilepath")
    uploadedMetadata.checksum should equal("checksum")
    uploadedMetadata.iaid should equal("iaid")

    val copyResponse = output.copy.head
    copyResponse.sourceBucket should equal("filesBucket")
    copyResponse.sourceKey should equal(s"$uuid/$fileId")
    copyResponse.destinationBucket should equal("outputBucketName")
    copyResponse.destinationKey should equal(s"$uuid/$fileId")

    val message = output.messages.head
    message.id should equal(uuid)
    message.location should equal(s"s3://outputBucketName/$uuid.metadata")
  }

  "lambda" should "error for invalid input json" in {
    val output = runLambda("{}")
    output.handlerResponse.left.value.getMessage should equal("DecodingFailure at : Got value '{}' with wrong type, expecting array")
  }

  "lambda" should "error if there are AWS failures" in {
    val input = List(
      Data(
        "HLAB/1",
        UUID.randomUUID,
        UUID.randomUUID,
        None,
        "fileName",
        "ABCD/E/F",
        "clientSideOriginalFilepath",
        "iaid",
        "digitalAssetSource",
        "checksum",
      )
    )

    val downloadError = runLambda(input.asJson.noSpaces, Errors(download = true).some)
    val uploadError = runLambda(input.asJson.noSpaces, Errors(upload = true).some)
    val copyError = runLambda(input.asJson.noSpaces, Errors(copy = true).some)
    val sendMessageErrorError = runLambda(input.asJson.noSpaces, Errors(sendMessage = true).some)

    def errorMessage(output: Output) = output.handlerResponse.left.value.getMessage

    errorMessage(downloadError) should equal("Download failed")
    errorMessage(uploadError) should equal("Upload failed")
    errorMessage(copyError) should equal("Copy failed")
    errorMessage(sendMessageErrorError) should equal("Send message failed")
  }
}
