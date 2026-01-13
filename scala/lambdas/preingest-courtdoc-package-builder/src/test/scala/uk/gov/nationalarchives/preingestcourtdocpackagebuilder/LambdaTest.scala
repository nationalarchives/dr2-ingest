package uk.gov.nationalarchives.preingestcourtdocpackagebuilder

import io.circe.parser.decode
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.IngestLockTableItem
import uk.gov.nationalarchives.preingestcourtdocpackagebuilder.TestUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.SourceSystem.`TRE: FCL Parser workflow`
import uk.gov.nationalarchives.utils.ExternalUtils.{ArchiveFolderMetadataObject, AssetMetadataObject, MetadataObject, given}

import java.net.URI
import java.util.UUID

class LambdaTest extends AnyFlatSpec with EitherValues:

  def item(id: UUID) = IngestLockTableItem(id, "", "", "")

  "lambda handler" should "upload the metadata to s3" in {
    val metadata = List(ArchiveFolderMetadataObject(UUID.randomUUID, None, Option("title"), "name", None, Nil))
    val items = List(item(UUID.randomUUID))
    val output = runLambda(Map(items.head -> metadata), items)

    output.res.isRight should equal(true)

    val uploadedMetadata = decode[List[ArchiveFolderMetadataObject]](output.uploads.head.content.array().map(_.toChar).mkString).value

    uploadedMetadata should equal(metadata)
  }

  "lambda handler" should "return the correct metadata location" in {
    val metadata = List(ArchiveFolderMetadataObject(UUID.randomUUID, None, Option("title"), "name", None, Nil))
    val items = List(item(UUID.randomUUID))
    val output = runLambda(Map(items.head -> metadata), items)

    output.res.isRight should equal(true)

    output.res.value.metadataPackage should equal(URI.create("s3://bucket/batchId/metadata.json"))
  }

  "lambda handler" should "send all lock table items to the metadata builder" in {
    val uuids = List.fill(100)(UUID.randomUUID)
    val lockTableItems = uuids.map(id => IngestLockTableItem(id, "", "", ""))

    val output = runLambda(Map(), lockTableItems)

    output.res.isRight should equal(true)
    output.builderArgs.map(_.assetId).sorted should equal(uuids.sorted)

  }

  "lambda" should "deduplicate archive folders where there are multiple with the same name" in {
    val folderOneId = UUID.randomUUID
    val assetOneId = UUID.randomUUID
    val folderTwoId = UUID.randomUUID
    val assetTwoId = UUID.randomUUID
    val folderThreeId = UUID.randomUUID
    val assetThreeId = UUID.randomUUID
    val folderFourId = UUID.randomUUID
    val assetFourId = UUID.randomUUID

    def folder(id: UUID, name: String) =
      ArchiveFolderMetadataObject(id, None, Option("title"), name, None, Nil)

    def asset(id: UUID, parentId: UUID) =
      AssetMetadataObject(id, Option(parentId), "", "", Nil, None, None, None, `TRE: FCL Parser workflow`, "Born Digital", None, "", None, Nil)

    val input = Map(
      item(assetOneId) -> List(folder(folderOneId, "name"), asset(assetOneId, folderOneId)),
      item(assetTwoId) -> List(folder(folderTwoId, "name"), asset(assetTwoId, folderTwoId)),
      item(assetThreeId) -> List(folder(folderThreeId, "name"), asset(assetThreeId, folderThreeId)),
      item(assetFourId) -> List(folder(folderFourId, "differentName"), asset(assetFourId, folderFourId))
    )

    val items = input.keys.toList

    val output = runLambda(input, items)

    val uploadedMetadata = decode[List[MetadataObject]](output.uploads.head.content.array().map(_.toChar).mkString).value

    uploadedMetadata.length should equal(6)

    val potentialFolders = uploadedMetadata.collect { case a: ArchiveFolderMetadataObject => a }
    potentialFolders.length should equal(2)

    uploadedMetadata.count(_.parentId.contains(folderOneId)) should equal(3)
    uploadedMetadata.count(_.parentId.contains(folderTwoId)) should equal(0)
    uploadedMetadata.count(_.parentId.contains(folderThreeId)) should equal(0)
    uploadedMetadata.count(_.parentId.contains(folderFourId)) should equal(1)
  }
