package uk.gov.nationalarchives.ingestassetopexcreator

import cats.effect.unsafe.implicits.global
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.FileRepresentationType.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*

import java.net.URI
import scala.xml.{Utility, XML}
import java.time.OffsetDateTime
import java.util.UUID
import scala.xml.Elem
import uk.gov.nationalarchives.dp.client.EntityClient.SecurityTag.*

class XMLCreatorTest extends AnyFlatSpec {
  private val opexNamespace = "http://www.openpreservationexchange.org/opex/v1.2"
  private val ingestDateTime = OffsetDateTime.parse("2023-12-04T10:55:44.848622Z")

  private def verifyXmlEqual(xmlStringOne: String, xmlElem: Elem) =
    Utility.trim(XML.loadString(xmlStringOne)).toString should equal(Utility.trim(xmlElem).toString)
  private def verifyXmlEqual(xmlStringOne: String, xmlStringTwo: String) =
    Utility.trim(XML.loadString(xmlStringOne)) should equal(Utility.trim(XML.loadString(xmlStringTwo)))

  val expectedOpexXml: Elem =
    <opex:OPEXMetadata xmlns:opex={opexNamespace}>
      <opex:Transfer>
        <opex:SourceID>90730c77-8faa-4dbf-b20d-bba1046dac87</opex:SourceID>
        <opex:Manifest>
          <opex:Files>
            <opex:File type="metadata" size="4">90730c77-8faa-4dbf-b20d-bba1046dac87.xip</opex:File>
            <opex:File type="content" size="1">Representation_Preservation/a814ee41-89f4-4975-8f92-303553fe9a02/Generation_1/a814ee41-89f4-4975-8f92-303553fe9a02.ext0</opex:File>
            <opex:File type="content" size="1">Representation_Preservation/9ecbba86-437f-42c6-aeba-e28b678bbf4c/Generation_1/9ecbba86-437f-42c6-aeba-e28b678bbf4c.ext1</opex:File>
          </opex:Files>
          <opex:Folders>
            <opex:Folder>Representation_Preservation</opex:Folder>
            <opex:Folder>Representation_Preservation/a814ee41-89f4-4975-8f92-303553fe9a02</opex:Folder>
            <opex:Folder>Representation_Preservation/a814ee41-89f4-4975-8f92-303553fe9a02/Generation_1</opex:Folder>
            <opex:Folder>Representation_Preservation/9ecbba86-437f-42c6-aeba-e28b678bbf4c</opex:Folder>
            <opex:Folder>Representation_Preservation/9ecbba86-437f-42c6-aeba-e28b678bbf4c/Generation_1</opex:Folder>
          </opex:Folders>
        </opex:Manifest>
      </opex:Transfer>
      <opex:Properties>
        <opex:Title>title</opex:Title>
        <opex:Description>description</opex:Description>
        <opex:SecurityDescriptor>unknown</opex:SecurityDescriptor>
        <opex:Identifiers>
          <opex:Identifier type="Test1">Value1</opex:Identifier>
          <opex:Identifier type="Test2">Value2</opex:Identifier>
          <opex:Identifier type="UpstreamSystemReference">testSystemRef2</opex:Identifier>
        </opex:Identifiers>
      </opex:Properties>
        <opex:DescriptiveMetadata>
          <Source xmlns="http://dr2.nationalarchives.gov.uk/source">
            <DigitalAssetSource>digitalAssetSource</DigitalAssetSource>
            <DigitalAssetSubtype>digitalAssetSubtype</DigitalAssetSubtype>
            <IngestDateTime>{ingestDateTime}</IngestDateTime>
            <OriginalFiles>
              <File>dec2b921-20e3-41e8-a299-f3cbc13131a2</File>
            </OriginalFiles>
            <OriginalMetadataFiles>
              <File>3f42e3f2-fffe-4fe9-87f7-262e95b86d75</File>
            </OriginalMetadataFiles>
            <TransferDateTime>2023-06-01T00:00Z</TransferDateTime>
            <TransferringBody>transferringBody</TransferringBody>
            <UpstreamSystem>upstreamSystem</UpstreamSystem>
            <UpstreamSystemRef>testSystemRef2</UpstreamSystemRef>
            <UpstreamPath>/a/file/path</UpstreamPath>
          </Source>
        </opex:DescriptiveMetadata>
    </opex:OPEXMetadata>

  val expectedXipXml: Elem =
    <XIP xmlns="http://preservica.com/XIP/v7.7">
      <InformationObject xmlns="http://preservica.com/XIP/v7.7">
        <Ref>90730c77-8faa-4dbf-b20d-bba1046dac87</Ref>
        <SecurityTag>unknown</SecurityTag>
        <Title>Preservation</Title>
      </InformationObject>
      <Representation xmlns="http://preservica.com/XIP/v7.7">
        <InformationObject>90730c77-8faa-4dbf-b20d-bba1046dac87</InformationObject>
        <Name>Preservation_1</Name>
        <Type>Preservation</Type>
        <ContentObjects>
          <ContentObject>a814ee41-89f4-4975-8f92-303553fe9a02</ContentObject>
        </ContentObjects>
      </Representation><Representation xmlns="http://preservica.com/XIP/v7.7">
      <InformationObject>90730c77-8faa-4dbf-b20d-bba1046dac87</InformationObject>
      <Name>Access_1</Name>
      <Type>Access</Type>
      <ContentObjects>
        <ContentObject>9ecbba86-437f-42c6-aeba-e28b678bbf4c</ContentObject>
      </ContentObjects>
    </Representation>
      <ContentObject xmlns="http://preservica.com/XIP/v7.7">
        <Ref>a814ee41-89f4-4975-8f92-303553fe9a02</Ref>
        <Title>name0</Title>
        <SecurityTag>unknown</SecurityTag>
        <Parent>90730c77-8faa-4dbf-b20d-bba1046dac87</Parent>
      </ContentObject><ContentObject xmlns="http://preservica.com/XIP/v7.7">
      <Ref>9ecbba86-437f-42c6-aeba-e28b678bbf4c</Ref>
      <Title>name1</Title>
      <SecurityTag>unknown</SecurityTag>
      <Parent>90730c77-8faa-4dbf-b20d-bba1046dac87</Parent>
    </ContentObject>
      <Generation original="true" active="true" xmlns="http://preservica.com/XIP/v7.7">
        <ContentObject>a814ee41-89f4-4975-8f92-303553fe9a02</ContentObject>
        <Bitstreams>
          <Bitstream>Representation_Preservation/a814ee41-89f4-4975-8f92-303553fe9a02/Generation_1/a814ee41-89f4-4975-8f92-303553fe9a02.ext0</Bitstream>
        </Bitstreams>
      </Generation><Generation original="true" active="true" xmlns="http://preservica.com/XIP/v7.7">
      <ContentObject>9ecbba86-437f-42c6-aeba-e28b678bbf4c</ContentObject>
      <Bitstreams>
        <Bitstream>Representation_Preservation/9ecbba86-437f-42c6-aeba-e28b678bbf4c/Generation_1/9ecbba86-437f-42c6-aeba-e28b678bbf4c.ext1</Bitstream>
      </Bitstreams>
    </Generation>
      <Bitstream xmlns="http://preservica.com/XIP/v7.7">
        <Filename>a814ee41-89f4-4975-8f92-303553fe9a02.ext0</Filename>
        <FileSize>1</FileSize>
        <PhysicalLocation>Representation_Preservation/a814ee41-89f4-4975-8f92-303553fe9a02/Generation_1</PhysicalLocation>
        <Fixities>
          <Fixity>
            <FixityAlgorithmRef>ALGORITHM1</FixityAlgorithmRef>
            <FixityValue>testChecksumAlgo1</FixityValue>
          </Fixity>
          <Fixity>
            <FixityAlgorithmRef>ALGORITHM2</FixityAlgorithmRef>
            <FixityValue>testChecksumAlgo2</FixityValue>
          </Fixity>
        </Fixities>
      </Bitstream><Bitstream xmlns="http://preservica.com/XIP/v7.7">
      <Filename>9ecbba86-437f-42c6-aeba-e28b678bbf4c.ext1</Filename>
      <FileSize>1</FileSize>
      <PhysicalLocation>Representation_Preservation/9ecbba86-437f-42c6-aeba-e28b678bbf4c/Generation_1</PhysicalLocation>
      <Fixities>
        <Fixity>
          <FixityAlgorithmRef>ALGORITHM1</FixityAlgorithmRef>
          <FixityValue>testChecksumAlgo1</FixityValue>
        </Fixity>
        <Fixity>
          <FixityAlgorithmRef>ALGORITHM2</FixityAlgorithmRef>
          <FixityValue>testChecksumAlgo2</FixityValue>
        </Fixity>
      </Fixities>
    </Bitstream>
    </XIP>

  val asset: AssetDynamoItem = AssetDynamoItem(
    "TEST-ID",
    UUID.fromString("90730c77-8faa-4dbf-b20d-bba1046dac87"),
    Option("parentPath"),
    Asset,
    Option("title"),
    Option("description"),
    Option("transferringBody"),
    Option(OffsetDateTime.parse("2023-06-01T00:00Z")),
    "upstreamSystem",
    "digitalAssetSource",
    Option("digitalAssetSubtype"),
    List(UUID.fromString("dec2b921-20e3-41e8-a299-f3cbc13131a2")),
    List(UUID.fromString("3f42e3f2-fffe-4fe9-87f7-262e95b86d75")),
    true,
    true,
    List(Identifier("Test2", "testIdentifier2"), Identifier("Test", "testIdentifier"), Identifier("UpstreamSystemReference", "testSystemRef")),
    1,
    false,
    None,
    "/a/file/path"
  )
  val uuids: List[UUID] = List(UUID.fromString("a814ee41-89f4-4975-8f92-303553fe9a02"), UUID.fromString("9ecbba86-437f-42c6-aeba-e28b678bbf4c"))
  val representationTypes: List[(FileRepresentationType, Int)] = List((PreservationRepresentationType, 1), (AccessRepresentationType, 1))
  val children: List[FileDynamoItem] = uuids.zipWithIndex.map { case (uuid, suffix) =>
    FileDynamoItem(
      "TEST-ID",
      uuid,
      Option(s"parentPath$suffix"),
      s"name$suffix",
      File,
      Option(s"title$suffix"),
      Option(s"description$suffix"),
      suffix,
      1,
      List(Checksum("Algorithm2", "testChecksumAlgo2"), Checksum("Algorithm1", "testChecksumAlgo1")),
      Option(s"ext$suffix"),
      representationTypes(suffix)._1,
      representationTypes(suffix)._2,
      true,
      true,
      List(Identifier("Test2", "testIdentifier4"), Identifier("Test", "testIdentifier3"), Identifier("UpstreamSystemReference", "testSystemRef2")),
      1,
      URI.create("s3://bucket/key")
    )
  }

  "createOpex" should "throw an 'Exception' if 'ingestDateTime' is before 'transferCompleteDatetime'" in {
    val identifiers = List(Identifier("Test1", "Value1"), Identifier("Test2", "Value2"), Identifier("UpstreamSystemReference", "testSystemRef2"))
    val ingestDateTimeBeforeTransferDateTime = OffsetDateTime.parse("2023-05-31T23:59:44.848622Z")
    val ex = intercept[Exception] {
      XMLCreator(ingestDateTimeBeforeTransferDateTime).createOpex(asset, children, 4, identifiers, Unknown).unsafeRunSync()
    }

    ex.getMessage should equal("'ingestDateTime' is before 'transferCompleteDatetime'!")
  }

  "createOpex" should "create the correct opex xml with identifiers" in {
    val identifiers = List(Identifier("Test1", "Value1"), Identifier("Test2", "Value2"), Identifier("UpstreamSystemReference", "testSystemRef2"))
    val xml = XMLCreator(ingestDateTime).createOpex(asset, children, 4, identifiers, Unknown).unsafeRunSync()
    verifyXmlEqual(xml, expectedOpexXml)
  }

  "createOpex" should "create the opex xml with empty digital asset subtype element " in {
    val identifiers = List(Identifier("Test1", "Value1"), Identifier("Test2", "Value2"), Identifier("UpstreamSystemReference", "testSystemRef2"))
    val assetWithoutDigitalAssetSubType = asset.copy(
      potentialDigitalAssetSubtype = None
    )
    val xml = XMLCreator(ingestDateTime).createOpex(assetWithoutDigitalAssetSubType, children, 4, identifiers, Unknown).unsafeRunSync()
    val expectedOpexXmlWithoutDigitalAssetSubType = expectedOpexXml.toString.replace("<DigitalAssetSubtype>digitalAssetSubtype</DigitalAssetSubtype>", "<DigitalAssetSubtype/>")

    verifyXmlEqual(xml, expectedOpexXmlWithoutDigitalAssetSubType)
  }

  "createOpex" should "create the correct opex xml with identifiers and an asset with the exact title that was in the table " +
    "(with relevant chars escaped) even if the title has an ASCII character in it" in {
      val identifiers = List(Identifier("Test1", "Value1"), Identifier("Test2", "Value2"), Identifier("UpstreamSystemReference", "testSystemRef2"))

      val assetWithTitleWithChars = asset.copy(
        potentialTitle = Some("""Title_with_ASCII_Chars_!"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~""")
      )
      val xml = XMLCreator(ingestDateTime).createOpex(assetWithTitleWithChars, children, 4, identifiers, Unknown).unsafeRunSync()
      val expectedOpexXmlWithNewTitle =
        expectedOpexXml.toString.replace(
          "<opex:Title>title</opex:Title>",
          """<opex:Title>Title_with_ASCII_Chars_!&quot;#$%&amp;'()*+,-./0123456789:;&lt;=&gt;?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~</opex:Title>"""
        )
      verifyXmlEqual(xml, expectedOpexXmlWithNewTitle)
    }

  "createOpex" should "create the correct opex xml with identifiers and an asset with the exact title that was in the table " +
    "(with relevant chars escaped) even if the title has multiple spaces" in {
      val identifiers = List(Identifier("Test1", "Value1"), Identifier("Test2", "Value2"), Identifier("UpstreamSystemReference", "testSystemRef2"))

      val assetWithTitleWithChars = asset.copy(potentialTitle = Some("A title     with   spaces  in            it"))
      val xml = XMLCreator(ingestDateTime).createOpex(assetWithTitleWithChars, children, 4, identifiers, Unknown).unsafeRunSync()
      val expectedOpexXmlWithNewTitle =
        expectedOpexXml.toString.replace("<opex:Title>title</opex:Title>", "<opex:Title>A title     with   spaces  in            it</opex:Title>")
      verifyXmlEqual(xml, expectedOpexXmlWithNewTitle)
    }

  "createXip" should "create the correct xip xml" in {
    val xml = XMLCreator(ingestDateTime).createXip(asset, children, Unknown).unsafeRunSync()
    verifyXmlEqual(xml, expectedXipXml)
  }

  "createXip" should "create the correct xip xml with children that have the exact title that was in the table " +
    "(with relevant chars escaped) even if the title has an ASCII character in it" in {
      val childrenWithTitleWithChars =
        children.map(
          _.copy(name = """Title_with_ASCII_Chars_!"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~""")
        )
      val xml = XMLCreator(ingestDateTime).createXip(asset, childrenWithTitleWithChars, Unknown).unsafeRunSync()
      val expectedXipXmlWithNewTitle = s"${expectedXipXml.toString()}\n"
        .replace(
          "<Title>name0</Title>",
          """<Title>Title_with_ASCII_Chars_!&quot;#$%&amp;'()*+,-./0123456789:;&lt;=&gt;?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~</Title>"""
        )
        .replace(
          "<Title>name1</Title>",
          """<Title>Title_with_ASCII_Chars_!&quot;#$%&amp;'()*+,-./0123456789:;&lt;=&gt;?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz{|}~</Title>"""
        )

      verifyXmlEqual(xml, expectedXipXmlWithNewTitle)
    }

  "createXip" should "create the correct xip xml with children that have the exact title that was in the table " +
    "(with relevant chars escaped) even if the title has multiple spaces" in {
      val childrenWithTitleWithChars =
        children.map(_.copy(name = "A title     with   spaces  in            it"))
      val xml = XMLCreator(ingestDateTime).createXip(asset, childrenWithTitleWithChars, Unknown).unsafeRunSync()
      val expectedXipXmlWithNewTitle = s"${expectedXipXml.toString()}\n"
        .replace("<Title>name0</Title>", "<Title>A title     with   spaces  in            it</Title>")
        .replace("<Title>name1</Title>", "<Title>A title     with   spaces  in            it</Title>")
      verifyXmlEqual(xml, expectedXipXmlWithNewTitle)
    }
}
