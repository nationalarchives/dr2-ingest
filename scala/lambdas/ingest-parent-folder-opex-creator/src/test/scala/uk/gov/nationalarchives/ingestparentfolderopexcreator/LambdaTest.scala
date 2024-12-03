package uk.gov.nationalarchives.ingestparentfolderopexcreator

import cats.syntax.all.*
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.ingestparentfolderopexcreator.Lambda.{Config, Input}
import uk.gov.nationalarchives.ingestparentfolderopexcreator.testUtils.ExternalServicesTestUtils.*

import scala.xml.Utility.trim
import scala.xml.XML.loadString

class LambdaTest extends AnyFlatSpec with EitherValues {
  val input: Input = Input("9e32383f-52a7-4591-83dc-e3e598a6f1a7")
  val config: Config = Config("stagingCacheBucketName", "role-arn")

  "handler" should "upload opex files to S3" in {
    val initialS3State: List[S3Object] = List(1, 2, 3).map(suffix => S3Object("bucket", s"opex/executionId/key$suffix", "content"))
    val (_, finalS3State) = runLambda(initialS3State)
    val expectedOpex = trim(<opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.2">
      <opex:Transfer>
        <opex:Manifest>
          <opex:Folders>
            <opex:Folder>key3</opex:Folder>
            <opex:Folder>key2</opex:Folder>
            <opex:Folder>key1</opex:Folder>
          </opex:Folders>
        </opex:Manifest>
      </opex:Transfer>
    </opex:OPEXMetadata>)

    val resultOpex = finalS3State
      .find(objects => objects.bucket == "bucketName" && objects.key == "opex/executionId/executionId.opex")
      .map(obj => trim(loadString(obj.content)))
      .get

    resultOpex should equal(expectedOpex)
  }

  "handler" should "not send an s3 'upload' request to upload Opex files if no prefixes were returned" in {
    val (_, finalS3State) = runLambda(Nil)

    finalS3State.size should equal(0)
  }

  "handler" should "not return an error if no prefixes were returned" in {
    val (result, _) = runLambda(Nil)

    result.left.value.getMessage should equal("No uploads were attempted for 'opex/executionId/'")
  }

  "handler" should "return an exception and not send an s3 'upload' request if 'listCommonPrefixes' returns an exception" in {
    val (result, finalS3State) = runLambda(Nil, Errors(listPrefix = true).some)
    result.left.value.getMessage should equal("List prefixes failed")
  }

  "handler" should "return an exception if an s3 'upload' attempt returns an exception" in {
    val initialS3State = List(S3Object("bucket", s"opex/executionId/key", "content"))
    val (result, finalS3State) = runLambda(initialS3State, Errors(upload = true).some)
    result.left.value.getMessage should equal("Upload has failed")
  }
}
