package uk.gov.nationalarchives.ingestfolderopexcreator

import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.ingestfolderopexcreator.ExternalServicesTestUtils.*
import scala.xml.Utility.trim
import scala.xml.XML.loadString
import java.util.UUID

class LambdaTest extends AnyFlatSpec with EitherValues {

  "handler" should "return an error if the folder is not found in dynamo" in {
    val folderId = UUID.randomUUID
    val (result, _) = runLambda(generateInput(folderId), Nil, Nil)
    result.left.value.getMessage should equal(s"No folder found for $folderId and $batchId")
  }

  "handler" should "return an error if no children are found for the folder" in {
    val folder = generateFolder.copy(childCount = 0)
    val folders = List(FolderWithChildren(folder, Nil))
    val (result, _) = runLambda(generateInput(folder.id), folders, Nil)
    result.left.value.getMessage should equal(s"No children found for ${folder.id} and $batchId")
  }

  "handler" should "return an error if the dynamo entry does not have a type of 'folder'" in {
    val folder = generateFolder.copy(`type` = Asset)
    val folders = List(FolderWithChildren(folder, Nil))
    val (result, _) = runLambda(generateInput(folder.id), folders, Nil)
    result.left.value.getMessage should equal(s"Object ${folder.id} is of type Asset and not 'ContentFolder' or 'ArchiveFolder'")
  }

  "handler" should "return an error if the expected child count does not match" in {
    val folder = generateFolder.copy(childCount = 2)
    val folders = List(FolderWithChildren(folder, List(generateAsset)))
    val (result, _) = runLambda(generateInput(folder.id), folders, Nil)
    result.left.value.getMessage should equal(s"Folder id ${folder.id}: has 2 children in the files table but found 1 children in the Preservation system")
  }

  "handler" should "upload the opex file to the correct path" in {
    val folder = generateFolder
    val asset = generateAsset
    val folders = List(FolderWithChildren(folder, List(asset)))
    val initialS3Objects = List(S3Object("sourceBucket", s"opex/$batchId/${asset.potentialParentPath.get}/${asset.id}.pax.opex", "opex"))

    val (result, s3Objects) = runLambda(generateInput(folder.id), folders, initialS3Objects)

    val opexPath = s"opex/$batchId/${folder.id}/${folder.id}.opex"
    s3Objects.head.key should equal(opexPath)
  }

  "handler" should "return an error if the object is not in S3" in {
    val folder = generateFolder
    val folders = List(FolderWithChildren(folder, List(generateAsset)))

    val (result, _) = runLambda(generateInput(folder.id), folders, Nil)

    result.left.value.getMessage should equal("Object not found")
  }

  "handler" should "upload the correct body to S3 if skipIngest is false on the asset" in {
    val folder = generateFolder.copy(childCount = 2)
    val assets = List(generateAsset, generateAsset)
    val folders = List(FolderWithChildren(folder, assets))
    val initialS3Objects = assets.map(asset => S3Object("sourceBucket", s"opex/$batchId/${asset.potentialParentPath.get}/${asset.id}.pax.opex", "opex"))

    val (result, s3Objects) = runLambda(generateInput(folder.id), folders, initialS3Objects)

    val expectedXml = generateExpectedOpex(assets)
    trim(loadString(s3Objects.head.content)).toString should equal(trim(expectedXml).toString)
  }

  "handler" should "upload the correct body to S3 if skipIngest is true on the asset" in {
    val folder = generateFolder
    val assets = List(generateAsset.copy(skipIngest = true))
    val folders = List(FolderWithChildren(folder, assets))
    val initialS3Objects = assets.map(asset => S3Object("sourceBucket", s"opex/$batchId/${asset.potentialParentPath.get}/${asset.id}.pax.opex", "opex"))

    val (result, s3Objects) = runLambda(generateInput(folder.id), folders, initialS3Objects)

    val expectedXml = generateExpectedOpex(Nil)
    trim(loadString(s3Objects.head.content)).toString should equal(trim(expectedXml).toString)
  }

  "handler" should "return an error if the get from Dynamo fails" in {
    val (result, _) = runLambda(generateInput(UUID.randomUUID), Nil, Nil, Option(Errors(dynamoGetError = true)))

    result.left.value.getMessage should equal("Error getting items from Dynamo")
  }

  "handler" should "return an error if the query from Dynamo fails" in {
    val folder = generateFolder
    val folders = List(FolderWithChildren(folder, Nil))
    val (result, _) = runLambda(generateInput(folder.id), folders, Nil, Option(Errors(dynamoQueryError = true)))

    result.left.value.getMessage should equal("Error querying Dynamo")
  }

  "handler" should "return an error if the S3 headObject fails" in {
    val folder = generateFolder
    val assets = List(generateAsset)
    val folders = List(FolderWithChildren(folder, assets))
    val initialS3Objects = assets.map(asset => S3Object("sourceBucket", s"opex/$batchId/${asset.potentialParentPath.get}/${asset.id}.pax.opex", "opex"))

    val (result, s3Objects) = runLambda(generateInput(folder.id), folders, initialS3Objects, Option(Errors(s3HeadObjectError = true)))

    result.left.value.getMessage should equal("Error calling head object")
  }

  "handler" should "return an error if the S3 upload fails" in {
    val folder = generateFolder
    val assets = List(generateAsset)
    val folders = List(FolderWithChildren(folder, assets))
    val initialS3Objects = assets.map(asset => S3Object("sourceBucket", s"opex/$batchId/${asset.potentialParentPath.get}/${asset.id}.pax.opex", "opex"))

    val (result, s3Objects) = runLambda(generateInput(folder.id), folders, initialS3Objects, Option(Errors(s3UploadError = true)))

    result.left.value.getMessage should equal("Error uploading s3 objects")
  }
}
