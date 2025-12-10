package uk.gov.nationalarchives.ingestassetopexcreator

import cats.effect.IO
import uk.gov.nationalarchives.dp.client.EntityClient.SecurityTag
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*

import java.time.OffsetDateTime

class XMLCreator(ingestDateTime: OffsetDateTime) {
  private val opexNamespace = "http://www.openpreservationexchange.org/opex/v1.2"
  private[nationalarchives] def bitstreamPath(child: DynamoItem) =
    s"Representation_Preservation/${child.id}/Generation_1"

  private[nationalarchives] def childFileName(child: FileDynamoItem) =
    child.potentialFileExtension.map(extension => s"${child.id}.$extension").getOrElse(child.id)

  private def getAllPaths(path: String): List[String] = {
    def generator(path: String, paths: List[String]): List[String] = {
      paths match {
        case Nil => path :: Nil
        case head :: tail =>
          val newPath = if path.isEmpty then head else s"$path/$head"
          newPath :: generator(newPath, tail)
      }
    }
    generator("", path.split('/').toList.filter(_.nonEmpty))
  }

  private[nationalarchives] def createOpex(
      asset: AssetDynamoItem,
      children: List[FileDynamoItem],
      assetXipSize: Long,
      identifiers: List[Identifier],
      securityTag: SecurityTag
  ): IO[String] = {
    val transferCompleteDatetime = asset.transferCompleteDatetime
    IO.raiseWhen(transferCompleteDatetime.exists(_.isAfter(ingestDateTime)))(new Exception("'ingestDateTime' is before 'transferCompleteDatetime'!")).map { _ =>
      val xml =
        <opex:OPEXMetadata xmlns:opex={opexNamespace}>
          <opex:Transfer>
            <opex:SourceID>{asset.id}</opex:SourceID>
            <opex:Manifest>
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
            </opex:Manifest>
          </opex:Transfer>
          <opex:Properties>
            <opex:Title>{asset.potentialTitle.getOrElse(asset.id)}</opex:Title>
            <opex:Description>{asset.potentialDescription.getOrElse("")}</opex:Description>
            <opex:SecurityDescriptor>{securityTag.toString}</opex:SecurityDescriptor>
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
          <opex:DescriptiveMetadata>
            <Source xmlns="http://dr2.nationalarchives.gov.uk/source">
              <DigitalAssetSource>{asset.digitalAssetSource}</DigitalAssetSource>
              <DigitalAssetSubtype>{asset.potentialDigitalAssetSubtype.getOrElse("")}</DigitalAssetSubtype>
              <IngestDateTime>{ingestDateTime}</IngestDateTime>
              {}
              <OriginalMetadataFiles>
                {asset.originalMetadataFiles.map(originalMetadataFile => <File>{originalMetadataFile}</File>)}
              </OriginalMetadataFiles>
              <TransferDateTime>{transferCompleteDatetime.getOrElse("")}</TransferDateTime>
              <TransferringBody>{asset.transferringBody.getOrElse("")}</TransferringBody>
              <UpstreamSystem>{asset.upstreamSystem}</UpstreamSystem>
              <UpstreamSystemRef>{identifiers.find(_.identifierName == "UpstreamSystemReference").map(_.value).getOrElse("")}</UpstreamSystemRef>
              <UpstreamPath>{asset.filePath}</UpstreamPath>
            </Source>
          </opex:DescriptiveMetadata>
        </opex:OPEXMetadata>
      xml.toString()
    }
  }

  private[nationalarchives] def createXip(asset: AssetDynamoItem, children: List[FileDynamoItem], securityTag: SecurityTag): IO[String] = {
    val xip =
      <XIP xmlns="http://preservica.com/XIP/v7.7">
      <InformationObject>
        <Ref>{asset.id}</Ref>
        <SecurityTag>{securityTag.toString}</SecurityTag>
        <Title>Preservation</Title>
      </InformationObject>
      {
        children.groupBy(child => (child.representationType.toString, child.representationSuffix)) map { case ((representationType, representationSuffix), files) =>
          <Representation>
            <InformationObject>{asset.id}</InformationObject>
            <Name>{s"${representationType}_$representationSuffix"}</Name>
            <Type>{representationType}</Type>
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
        <SecurityTag>{securityTag.toString}</SecurityTag>
        <Parent>{asset.id}</Parent>
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
          {
                child.checksums
                  .sortBy(_.algorithm)
                  .map(eachChecksum => <Fixity>
              <FixityAlgorithmRef>{eachChecksum.algorithm.toUpperCase}</FixityAlgorithmRef>
              <FixityValue>{eachChecksum.fingerprint}</FixityValue>
            </Fixity>)
              }
        </Fixities>
      </Bitstream>
            List(if index == 0 then "" else "\n      ", contentElement, "\n      ", generationElement, "\n      ", bitstreamElement)
          }
      }
  </XIP>
    IO.pure {
      <XIP xmlns="http://preservica.com/XIP/v7.7">
        {xip \ "InformationObject"}
        {xip \ "Representation"}
        {xip \ "ContentObject"}
        {xip \ "Generation"}
        {xip \ "Bitstream"}
      </XIP>.toString + "\n"
    }
  }
}
object XMLCreator {
  def apply(ingestDateTime: OffsetDateTime): XMLCreator = new XMLCreator(ingestDateTime)
}
