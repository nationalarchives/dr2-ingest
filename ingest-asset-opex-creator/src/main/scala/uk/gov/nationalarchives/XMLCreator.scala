package uk.gov.nationalarchives

import cats.effect.IO
import uk.gov.nationalarchives.DynamoFormatters._

import java.time.OffsetDateTime

class XMLCreator(ingestDateTime: OffsetDateTime) {
  private val opexNamespace = "http://www.openpreservationexchange.org/opex/v1.2"
  private[nationalarchives] def bitstreamPath(child: DynamoTable) =
    s"Representation_Preservation/${child.id}/Generation_1"

  private[nationalarchives] def childFileName(child: FileDynamoTable) =
    s"${child.id}.${child.fileExtension}"

  private def getAllPaths(path: String): List[String] = {
    def generator(path: String, paths: List[String]): List[String] = {
      paths match {
        case Nil => path :: Nil
        case head :: tail =>
          val newPath = if path.isEmpty then head else s"$path/$head"
          newPath :: generator(newPath, tail)
      }
    }
    generator("", path.split("/").toList.filter(_.nonEmpty))
  }

  private[nationalarchives] def createOpex(
      asset: AssetDynamoTable,
      children: List[FileDynamoTable],
      assetXipSize: Long,
      identifiers: List[Identifier],
      securityDescriptor: String = "open"
  ): IO[String] = {
    val transferCompleteDatetime = asset.transferCompleteDatetime
    IO.raiseWhen(transferCompleteDatetime.isAfter(ingestDateTime))(new Exception("'ingestDateTime' is before 'transferCompleteDatetime'!")).map { _ =>
      val xml =
        <opex:OPEXMetadata xmlns:opex={opexNamespace}>
          <opex:DescriptiveMetadata>
            <Source xmlns="http://dr2.nationalarchives.gov.uk/source">
              <DigitalAssetSource>{asset.digitalAssetSource}</DigitalAssetSource>
              <DigitalAssetSubtype>{asset.digitalAssetSubtype}</DigitalAssetSubtype>
              <IngestDateTime>{ingestDateTime}</IngestDateTime>
              <OriginalFiles>
                {asset.originalFiles.map(originalFile => <File>{originalFile}</File>)}
              </OriginalFiles>
              <OriginalMetadataFiles>
                {asset.originalMetadataFiles.map(originalMetadataFile => <File>{originalMetadataFile}</File>)}
              </OriginalMetadataFiles>
              <TransferDateTime>{transferCompleteDatetime}</TransferDateTime>
              <TransferringBody>{asset.transferringBody}</TransferringBody>
              <UpstreamSystem>{asset.upstreamSystem}</UpstreamSystem>
              <UpstreamSystemRef>{identifiers.find(_.identifierName == "UpstreamSystemReference").get.value}</UpstreamSystemRef>
            </Source>
          </opex:DescriptiveMetadata>
          <opex:Transfer>
            <opex:SourceID>{asset.name}</opex:SourceID>
            <opex:Manifest>
              <opex:Folders>
                {
          children
            .map(bitstreamPath)
            .flatMap(path => getAllPaths(path))
            .distinct
            .zipWithIndex
            .map { case (folder: String, index: Int) =>
              val folderOpex = <opex:Folder>{folder}</opex:Folder>
              List(if index == 0 then "" else "\n                ", folderOpex)
            }
        }
              </opex:Folders>
              <opex:Files>
                <opex:File type="metadata" size={assetXipSize.toString}>{asset.id}.xip</opex:File>
                {
          children.zipWithIndex
            .map { case (child, index) =>
              val fileOpex = <opex:File type="content" size={child.fileSize.toString}>{bitstreamPath(child)}/{childFileName(child)}</opex:File>
              List(if index == 0 then "" else "\n                ", fileOpex)
            }
        }
              </opex:Files>
            </opex:Manifest>
          </opex:Transfer>
          <opex:Properties>
            <opex:Title>{asset.title.getOrElse(asset.name)}</opex:Title>
            <opex:Description>{asset.description.getOrElse("")}</opex:Description>
            <opex:SecurityDescriptor>{securityDescriptor}</opex:SecurityDescriptor>
            {
          if identifiers.nonEmpty then
            <opex:Identifiers>
              {
              identifiers.zipWithIndex
                .map { case (identifier, index) =>
                  val identifierOpex = <opex:Identifier type={identifier.identifierName}>{identifier.value}</opex:Identifier>
                  List(if index == 0 then "" else "\n              ", identifierOpex)
                }
            }
            </opex:Identifiers>
          else ()
        }
          </opex:Properties>
        </opex:OPEXMetadata>
      xml.toString()
    }
  }

  private[nationalarchives] def createXip(asset: AssetDynamoTable, children: List[FileDynamoTable], securityTag: String = "open"): IO[String] = {
    val xip =
      <XIP xmlns="http://preservica.com/XIP/v7.0">
      <InformationObject>
        <Ref>{asset.id}</Ref>
        <SecurityTag>{securityTag}</SecurityTag>
        <Title>Preservation</Title>
      </InformationObject>
      {
        children.groupBy(child => (child.representationType.toString, child.representationSuffix)) map { case ((representationType, representationSuffix), files) =>
          <Representation>
            <InformationObject>{asset.id}</InformationObject>
            <Type>{representationType}</Type>
            <Name>{s"${representationType}_$representationSuffix"}</Name>
            <ContentObjects>
              {
            files.zipWithIndex
              .map { case (child, index) =>
                val contentObjectElement = <ContentObject>{child.id}</ContentObject>
                List(if index == 0 then "" else "\n          ", contentObjectElement)
              }
          }
            </ContentObjects>
          </Representation>
        }
      }
      {
        children.zipWithIndex
          .map { case (child, index) =>
            val contentElement =
              <ContentObject>
        <Ref>{child.id}</Ref>
        <Title>{child.name}</Title>
        <Parent>{asset.id}</Parent>
        <SecurityTag>{securityTag}</SecurityTag>
      </ContentObject>
            val generationElement =
              <Generation original="true" active="true">
        <ContentObject>{child.id}</ContentObject>
        <Bitstreams>
          <Bitstream>{bitstreamPath(child)}/{childFileName(child)}</Bitstream>
        </Bitstreams>
      </Generation>
            val bitstreamElement =
              <Bitstream>
        <Filename>{childFileName(child)}</Filename>
        <FileSize>{child.fileSize}</FileSize>
        <PhysicalLocation>{bitstreamPath(child)}</PhysicalLocation>
        <Fixities>
          <Fixity>
            <FixityAlgorithmRef>SHA256</FixityAlgorithmRef>
            <FixityValue>{child.checksumSha256}</FixityValue>
          </Fixity>
        </Fixities>
      </Bitstream>
            List(if index == 0 then "" else "\n      ", contentElement, "\n      ", generationElement, "\n      ", bitstreamElement)
          }
      }
  </XIP>
    IO(xip.toString + "\n")
  }
}
object XMLCreator {
  def apply(ingestDateTime: OffsetDateTime): XMLCreator = new XMLCreator(ingestDateTime)
}
