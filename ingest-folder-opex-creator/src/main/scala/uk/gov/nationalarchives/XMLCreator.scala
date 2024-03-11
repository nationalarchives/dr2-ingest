package uk.gov.nationalarchives

import cats.effect.IO
import uk.gov.nationalarchives.DynamoFormatters._
import uk.gov.nationalarchives.Lambda.{FolderOrAssetTable, AssetWithFileSize}

import scala.xml.PrettyPrinter

class XMLCreator {
  private val opexNamespace = "http://www.openpreservationexchange.org/opex/v1.2"

  def createFolderOpex(
      folder: ArchiveFolderDynamoTable,
      childAssets: List[AssetWithFileSize],
      childFolders: List[FolderOrAssetTable],
      identifiers: List[Identifier],
      securityDescriptor: String = "open"
  ): IO[String] = IO {
    val isHierarchyFolder: Boolean = folder.`type` == ArchiveFolder
    val prettyPrinter = new PrettyPrinter(180, 2)
    val xml = <opex:OPEXMetadata xmlns:opex={opexNamespace}>
      <opex:Properties>
        <opex:Title>{folder.title.getOrElse(folder.name)}</opex:Title>
        <opex:Description>{folder.description.getOrElse("")}</opex:Description>
        <opex:SecurityDescriptor>{securityDescriptor}</opex:SecurityDescriptor>
        {
      if (identifiers.nonEmpty) {
        <opex:Identifiers>
          {identifiers.map(identifier => <opex:Identifier type={identifier.identifierName}>{identifier.value}</opex:Identifier>)}
        </opex:Identifiers>
      }
    }
      </opex:Properties>
      <opex:Transfer>
        {if (isHierarchyFolder) <opex:SourceID>{folder.name}</opex:SourceID>}
        <opex:Manifest>
          <opex:Folders>
            {childAssets.map(assetWithFileSize => <opex:Folder>{assetWithFileSize.asset.id}.pax</opex:Folder>)}
            {childFolders.map(folder => <opex:Folder>{folder.id}</opex:Folder>)}
          </opex:Folders>
          <opex:Files>
            {childAssets.map(assetWithFileSize => <opex:File type="metadata" size={assetWithFileSize.fileSize.toString}>{assetWithFileSize.asset.id.toString}.pax.opex</opex:File>)}
          </opex:Files>
        </opex:Manifest>
      </opex:Transfer>
    </opex:OPEXMetadata>
    prettyPrinter.format(xml)
  }
}
object XMLCreator {
  def apply(): XMLCreator = new XMLCreator()
}
