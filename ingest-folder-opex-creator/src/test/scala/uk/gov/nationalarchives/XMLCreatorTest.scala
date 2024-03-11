package uk.gov.nationalarchives

import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import uk.gov.nationalarchives.DynamoFormatters._
import uk.gov.nationalarchives.Lambda.{FolderOrAssetTable, AssetWithFileSize}

import java.util.UUID
import scala.xml.{Elem, PrettyPrinter}

class XMLCreatorTest extends AnyFlatSpec {
  private val opexNamespace = "http://www.openpreservationexchange.org/opex/v1.2"

  val expectedStandardNonArchiveFolderXml: Elem = <opex:OPEXMetadata xmlns:opex={opexNamespace}>
    <opex:Properties>
      <opex:Title>title</opex:Title>
      <opex:Description>description</opex:Description>
      <opex:SecurityDescriptor>open</opex:SecurityDescriptor>
    </opex:Properties>
    <opex:Transfer>
      <opex:Manifest>
        <opex:Folders>
          <opex:Folder>a814ee41-89f4-4975-8f92-303553fe9a02.pax</opex:Folder>
          <opex:Folder>9ecbba86-437f-42c6-aeba-e28b678bbf4c.pax</opex:Folder>
          <opex:Folder>7fcd94a9-be3f-456d-875f-bc697f7ed106</opex:Folder>
          <opex:Folder>9ecbba86-437f-42c6-aeba-e28b678bbf4c</opex:Folder>
        </opex:Folders>
        <opex:Files>
          <opex:File type="metadata" size="1">a814ee41-89f4-4975-8f92-303553fe9a02.pax.opex</opex:File>
          <opex:File type="metadata" size="1">9ecbba86-437f-42c6-aeba-e28b678bbf4c.pax.opex</opex:File>
        </opex:Files>
      </opex:Manifest>
    </opex:Transfer>
  </opex:OPEXMetadata>

  val expectedStandardArchivedFolderXml: Elem = <opex:OPEXMetadata xmlns:opex={opexNamespace}>
    <opex:Properties>
      <opex:Title>title</opex:Title>
      <opex:Description>description</opex:Description>
      <opex:SecurityDescriptor>open</opex:SecurityDescriptor>
      <opex:Identifiers>
        <opex:Identifier type="Code">name</opex:Identifier>
      </opex:Identifiers>
    </opex:Properties>
    <opex:Transfer>
      <opex:SourceID>name</opex:SourceID>
      <opex:Manifest>
        <opex:Folders>
          <opex:Folder>a814ee41-89f4-4975-8f92-303553fe9a02.pax</opex:Folder>
          <opex:Folder>9ecbba86-437f-42c6-aeba-e28b678bbf4c.pax</opex:Folder>
          <opex:Folder>7fcd94a9-be3f-456d-875f-bc697f7ed106</opex:Folder>
          <opex:Folder>9ecbba86-437f-42c6-aeba-e28b678bbf4c</opex:Folder>
        </opex:Folders>
        <opex:Files>
          <opex:File type="metadata" size="1">a814ee41-89f4-4975-8f92-303553fe9a02.pax.opex</opex:File>
          <opex:File type="metadata" size="1">9ecbba86-437f-42c6-aeba-e28b678bbf4c.pax.opex</opex:File>
        </opex:Files>
      </opex:Manifest>
    </opex:Transfer>
  </opex:OPEXMetadata>

  val expectedXmlNoTitle: Elem = <opex:OPEXMetadata xmlns:opex={opexNamespace}>
    <opex:Properties>
      <opex:Title>name</opex:Title>
      <opex:Description>description</opex:Description>
      <opex:SecurityDescriptor>open</opex:SecurityDescriptor>
    </opex:Properties>
    <opex:Transfer>
      <opex:SourceID>name</opex:SourceID>
      <opex:Manifest>
        <opex:Folders>
          <opex:Folder>a814ee41-89f4-4975-8f92-303553fe9a02.pax</opex:Folder>
          <opex:Folder>9ecbba86-437f-42c6-aeba-e28b678bbf4c.pax</opex:Folder>
          <opex:Folder>7fcd94a9-be3f-456d-875f-bc697f7ed106</opex:Folder>
          <opex:Folder>9ecbba86-437f-42c6-aeba-e28b678bbf4c</opex:Folder>
        </opex:Folders>
        <opex:Files>
          <opex:File type="metadata" size="1">a814ee41-89f4-4975-8f92-303553fe9a02.pax.opex</opex:File>
          <opex:File type="metadata" size="1">9ecbba86-437f-42c6-aeba-e28b678bbf4c.pax.opex</opex:File>
        </opex:Files>
      </opex:Manifest>
    </opex:Transfer>
  </opex:OPEXMetadata>

  val expectedXMLNoHierarchyFolder: Elem = <opex:OPEXMetadata xmlns:opex={opexNamespace}>
    <opex:Properties>
      <opex:Title>name</opex:Title>
      <opex:Description>description</opex:Description>
      <opex:SecurityDescriptor>open</opex:SecurityDescriptor>
    </opex:Properties>
    <opex:Transfer>
      <opex:Manifest>
        <opex:Folders>
          <opex:Folder>a814ee41-89f4-4975-8f92-303553fe9a02.pax</opex:Folder>
          <opex:Folder>9ecbba86-437f-42c6-aeba-e28b678bbf4c.pax</opex:Folder>
          <opex:Folder>7fcd94a9-be3f-456d-875f-bc697f7ed106</opex:Folder>
          <opex:Folder>9ecbba86-437f-42c6-aeba-e28b678bbf4c</opex:Folder>
        </opex:Folders>
        <opex:Files>
          <opex:File type="metadata" size="1">a814ee41-89f4-4975-8f92-303553fe9a02.pax.opex</opex:File>
          <opex:File type="metadata" size="1">9ecbba86-437f-42c6-aeba-e28b678bbf4c.pax.opex</opex:File>
        </opex:Files>
      </opex:Manifest>
    </opex:Transfer>
  </opex:OPEXMetadata>

  val archiveFolder: ArchiveFolderDynamoTable = ArchiveFolderDynamoTable(
    "TEST-ID",
    UUID.fromString("90730c77-8faa-4dbf-b20d-bba1046dac87"),
    Option("parentPath"),
    "name",
    ArchiveFolder,
    Option("title"),
    Option("description"),
    Nil
  )
  val assetUuids: List[UUID] = List(UUID.fromString("a814ee41-89f4-4975-8f92-303553fe9a02"), UUID.fromString("9ecbba86-437f-42c6-aeba-e28b678bbf4c"))
  val folderUuids: List[UUID] = List(UUID.fromString("7fcd94a9-be3f-456d-875f-bc697f7ed106"), UUID.fromString("9ecbba86-437f-42c6-aeba-e28b678bbf4c"))
  val childAssets: List[AssetWithFileSize] = assetUuids.zipWithIndex.map { case (uuid, suffix) =>
    AssetWithFileSize(
      FolderOrAssetTable(
        "TEST-ID",
        uuid,
        Option(s"parentPath$suffix"),
        s"name$suffix Asset",
        Asset,
        Option(s"title$suffix Asset"),
        Option(s"description$suffix Asset"),
        Nil
      ),
      1
    )
  }

  val childFolders: List[FolderOrAssetTable] = folderUuids.zipWithIndex.map { case (uuid, suffix) =>
    FolderOrAssetTable(
      "TEST-ID",
      uuid,
      Option(s"parentPath$suffix"),
      s"name$suffix Folder",
      ContentFolder,
      Option(s"title$suffix Folder"),
      Option(s"description$suffix Folder"),
      Nil
    )
  }

  private val prettyPrinter = new PrettyPrinter(200, 2)

  "createFolderOpex" should "create the correct opex xml, excluding the SourceId, if folder type is not 'ArchiveFolder' and there are no identifiers" in {
    val xml = XMLCreator().createFolderOpex(archiveFolder.copy(`type` = ContentFolder), childAssets, childFolders, Nil).unsafeRunSync()
    xml should equal(prettyPrinter.format(expectedStandardNonArchiveFolderXml))
  }

  "createFolderOpex" should "create the correct opex xml, including the SourceId, if folder type is 'ArchiveFolder' and there are identifiers" in {
    val xml = XMLCreator().createFolderOpex(archiveFolder, childAssets, childFolders, List(Identifier("Code", "name"))).unsafeRunSync()
    xml should equal(prettyPrinter.format(expectedStandardArchivedFolderXml))
  }

  "createFolderOpex" should "create the correct opex xml, using the name if the title is blank" in {
    val xml = XMLCreator().createFolderOpex(archiveFolder.copy(title = None), childAssets, childFolders, Nil).unsafeRunSync()
    xml should equal(prettyPrinter.format(expectedXmlNoTitle))
  }
}
