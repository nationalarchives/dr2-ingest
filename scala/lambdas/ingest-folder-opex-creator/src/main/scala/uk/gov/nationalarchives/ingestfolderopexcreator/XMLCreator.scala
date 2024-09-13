package uk.gov.nationalarchives.ingestfolderopexcreator

import cats.effect.IO
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.ingestfolderopexcreator.Lambda.{AssetWithFileSize, FolderOrAssetTable}

class XMLCreator {
  private val opexNamespace = "http://www.openpreservationexchange.org/opex/v1.2"

  def createFolderOpex(
      folder: ArchiveFolderDynamoTable,
      childAssets: List[AssetWithFileSize],
      childFolders: List[FolderOrAssetTable],
      identifiers: List[Identifier],
      securityDescriptor: String = "open"
  ): IO[String] = IO.pure {
    val isHierarchyFolder: Boolean = folder.`type` == ArchiveFolder
    <opex:OPEXMetadata xmlns:opex={opexNamespace}>
      <opex:Transfer>
        {if isHierarchyFolder then <opex:SourceID>{folder.name}</opex:SourceID> else ()}
        <opex:Manifest>
          <opex:Files>
            {childAssets.map(assetWithFileSize => <opex:File type="metadata" size={assetWithFileSize.fileSize.toString}>{assetWithFileSize.asset.id.toString}.pax.opex</opex:File>)}
          </opex:Files>
          <opex:Folders>
            {childAssets.map(assetWithFileSize => <opex:Folder>{assetWithFileSize.asset.id}.pax</opex:Folder>)}
            {childFolders.map(folder => <opex:Folder>{folder.id}</opex:Folder>)}
          </opex:Folders>
        </opex:Manifest>
      </opex:Transfer>
      <opex:Properties>
        <opex:Title>{folder.potentialTitle.getOrElse(folder.name)}</opex:Title>
        <opex:Description>{folder.potentialDescription.getOrElse("")}</opex:Description>
        <opex:SecurityDescriptor>{securityDescriptor}</opex:SecurityDescriptor>
        {
      if identifiers.nonEmpty then <opex:Identifiers>
          {identifiers.map(identifier => <opex:Identifier type={identifier.identifierName}>{identifier.value}</opex:Identifier>)}
        </opex:Identifiers>
      else ()

    }
      </opex:Properties>
    </opex:OPEXMetadata>.toString
  }
}
object XMLCreator {
  def apply(): XMLCreator = new XMLCreator()
}
