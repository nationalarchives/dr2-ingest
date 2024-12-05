package uk.gov.nationalarchives.ingestassetopexcreator

import cats.syntax.all.*
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.ArchiveFolder
import uk.gov.nationalarchives.ingestassetopexcreator.testUtils.ExternalServicesTestUtils.*

import java.util.UUID
import scala.xml.{Utility, XML}

class LambdaTest extends AnyFlatSpec with EitherValues {

  "handler" should "return an error if the asset is not found in dynamo" in {
    val assetId = UUID.randomUUID
    val (result, _) = runLambda(generateInput(assetId), Nil, Nil)
    result.left.value.getMessage should equal(s"No asset found for $assetId and $batchId")
  }

  "handler" should "return an error if no children are found for the asset" in {
    val asset = generateAsset.copy(childCount = 0)
    val assets = List(AssetWithChildren(asset, Nil))
    val (result, _) = runLambda(generateInput(asset.id), assets, Nil)
    result.left.value.getMessage should equal(s"No children found for ${asset.id} and $batchId")
  }

  "handler" should "return an error if childCount is higher than the number of rows returned" in {
    val asset = generateAsset.copy(childCount = 2)
    val assets = List(AssetWithChildren(asset, List(generateFile)))
    val (result, _) = runLambda(generateInput(asset.id), assets, Nil)
    result.left.value.getMessage should equal(s"Asset id ${asset.id}: has 2 children in the files table but found 1 children in the Preservation system")
  }

  "handler" should "return an error if the dynamo entry does not have a type of 'Asset'" in {
    val asset = generateAsset.copy(`type` = ArchiveFolder)
    val assets = List(AssetWithChildren(asset, Nil))
    val (result, _) = runLambda(generateInput(asset.id), assets, Nil)
    result.left.value.getMessage should equal(s"Object ${asset.id} is of type ArchiveFolder and not 'Asset'")
  }

  "handler" should "copy the correct child assets from source to destination" in {
    val asset = generateAsset
    val file = generateFile
    val initialS3Objects = List(S3Object(file.location.getHost, file.location.getPath.drop(1), ""))
    val assets = List(AssetWithChildren(asset, List(file)))
    val (result, s3Objects) = runLambda(generateInput(asset.id), assets, initialS3Objects)

    s3Objects.size should equal(3)
    s3Objects.last.bucket should equal("destinationBucket")
    s3Objects.last.key should equal(s"opex/$batchId/${asset.id}.pax/Representation_Preservation/${file.id}/Generation_1/${file.id}.${file.potentialFileExtension.get}")
  }

  "handler" should "upload the xip and opex files" in {
    val asset = generateAsset
    val file = generateFile
    val initialS3Objects = List(S3Object(file.location.getHost, file.location.getPath.drop(1), ""))
    val assets = List(AssetWithChildren(asset, List(file)))
    val (result, s3Objects) = runLambda(generateInput(asset.id), assets, initialS3Objects)

    s3Objects.count(_.key == s"opex/$batchId/${asset.id}.pax.opex") should equal(1)
    s3Objects.count(_.key == s"opex/$batchId/${asset.id}.pax/${asset.id}.xip") should equal(1)
  }

  "handler" should "write the xip content objects in the correct order" in {
    val asset = generateAsset.copy(childCount = 2)
    val files = List(generateFile, generateFile.copy(sortOrder = 2))
    val initialS3Objects = files.map(file => S3Object(file.location.getHost, file.location.getPath.drop(1), ""))
    val assets = List(AssetWithChildren(asset, files))
    val (result, s3Objects) = runLambda(generateInput(asset.id), assets, initialS3Objects)

    val xipString = s3Objects.find(_.key == s"opex/$batchId/${asset.id}.pax/${asset.id}.xip").map(_.content).getOrElse("")
    val contentObjects = XML.loadString(xipString) \ "Representation" \ "ContentObjects" \ "ContentObject"
    contentObjects.head.text should equal(files.head.id.toString)
    contentObjects.last.text should equal(files.last.id.toString)
  }

  "handler" should "upload the correct opex file to s3" in {
    val asset = generateAsset.copy(childCount = 2)
    val files = List(generateFile, generateFile.copy(sortOrder = 2))
    val initialS3Objects = files.map(file => S3Object(file.location.getHost, file.location.getPath.drop(1), ""))
    val assets = List(AssetWithChildren(asset, files))
    val (result, s3Objects) = runLambda(generateInput(asset.id), assets, initialS3Objects)

    val opexString = s3Objects.find(_.key == s"opex/$batchId/${asset.id}.pax.opex").map(_.content).getOrElse("")
    val opexXml = XML.loadString(opexString)
    val expectedOpex = generateExpectedOpex(asset, files)
    Utility.trim(opexXml).toString should equal(Utility.trim(expectedOpex).toString)
  }

  "handler" should "return an error if the Dynamo get call fails" in {
    val (result, _) = runLambda(generateInput(UUID.randomUUID), Nil, Nil, Errors(dynamoGetError = true).some)
    result.left.value.getMessage should equal("Error getting items from Dynamo")
  }

  "handler" should "return an error if the Dynamo query call fails" in {
    val asset = generateAsset
    val (result, _) = runLambda(generateInput(asset.id), List(AssetWithChildren(asset, Nil)), Nil, Errors(dynamoQueryError = true).some)
    result.left.value.getMessage should equal("Error querying Dynamo")
  }

  "handler" should "return an error if the S3 copy call fails" in {
    val asset = generateAsset
    val file = generateFile
    val assets = List(AssetWithChildren(asset, List(file)))
    val (result, _) = runLambda(generateInput(asset.id), assets, Nil, Errors(s3CopyError = true).some)

    result.left.value.getMessage should equal("Error copying s3 objects")
  }

  "handler" should "return an error if the S3 upload call fails" in {
    val asset = generateAsset
    val file = generateFile
    val assets = List(AssetWithChildren(asset, List(file)))
    val s3Objects = S3Object(file.location.getHost, file.location.getPath.drop(1), "")
    val (result, _) = runLambda(generateInput(asset.id), assets, Nil, Errors(s3UploadError = true).some)

    result.left.value.getMessage should equal("Error uploading s3 objects")
  }
}
