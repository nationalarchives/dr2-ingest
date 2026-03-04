package uk.gov.nationalarchives.preingestrestorepackagebuilder

import io.circe.parser.decode
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.preingestrestorepackagebuilder.TestUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.RepresentationType.Preservation
import uk.gov.nationalarchives.utils.ExternalUtils.*

import java.time.OffsetDateTime
import java.util.UUID

class LambdaTest extends AnyFlatSpec with EitherValues:

  "lambda" should "write the correct metadata to s3 given valid input xml metadata" in {
    val xml = <XIP>
      <InformationObject>
        <Ref>deaef74e-9870-4303-9985-3a8ce64edf0c</Ref>
        <Title>Test title</Title>
        <xip:Description/>
      </InformationObject>
      <Identifier>
        <Type>SourceID</Type>
        <Value>8e5b9cea-62a5-43f1-a268-74bb740e2f70</Value>
      </Identifier>
      <Identifier>
        <Type>Code</Type>
        <Value>ABC/123</Value>
      </Identifier>
      <Metadata>
        <Ref>0e24acb1-cfaa-42be-8dcd-cad7d7cf0baf</Ref>
        <Entity>4b44388f-d788-44f7-a61e-21944941598b</Entity>
        <Content>
          <Source>
            <DigitalAssetSource>Digital Surrogate</DigitalAssetSource>
            <DigitalAssetSubtype/>
            <IngestDateTime>2025-12-08T19:56:08.271843112Z</IngestDateTime>
            <OriginalMetadataFiles>
              <File>9deee21f-5062-4aaa-82f4-2d31fb4262b4</File>
            </OriginalMetadataFiles>
            <TransferDateTime>2023-03-24T10:13:04.280Z</TransferDateTime>
            <TransferringBody/>
            <UpstreamSystem>Parliament Migration</UpstreamSystem>
            <UpstreamSystemRef>SAM/A/123</UpstreamSystemRef>
            <UpstreamPath/>
          </Source>
        </Content>
      </Metadata>
      <CCContentObjects>
        <CCContentObject>
          <ContentObject>
            <Ref>2f162f2b-4fac-417a-b484-3bc68c02ae1d</Ref>
            <Title>Test title.txt</Title>
          </ContentObject>
          <Bitstreams>
            <Bitstream>e7c8511b-6a9b-4146-839d-33d7a40e9232.tif</Bitstream>
          </Bitstreams>
          <Bitstream>
            <FileSize>1</FileSize>
            <Fixities>
              <Fixity>
                <FixityAlgorithmRef>SHA256</FixityAlgorithmRef>
                <FixityValue>1b6f487a4f27f1e94ff4e794f4dce56e6d9ba5ec49919def6a3aded7f527b769
                </FixityValue>
              </Fixity>
            </Fixities>
          </Bitstream>
        </CCContentObject>
      </CCContentObjects>
    </XIP>
    val res = runLambda(List(lockTableItem), Map("key" -> xml.toString))
    val output = res.output.value
    output.batchId should equal("TEST_0")
    output.groupId should equal("TEST")
    output.metadataPackage.getHost should equal("output-bucket")
    output.metadataPackage.getPath should equal("/TEST_0/metadata.json")

    val metadataObjects = decode[List[MetadataObject]](res.s3Map("TEST_0/metadata.json")).value
    metadataObjects.size should equal(3)

    val contentFolder = metadataObjects.collect { case c: ContentFolderMetadataObject => c }.head
    val asset = metadataObjects.collect { case a: AssetMetadataObject => a }.head
    val file = metadataObjects.collect { case f: FileMetadataObject => f }.head

    contentFolder.name should equal("Restored records")
    contentFolder.series.get should equal("Unknown")

    asset.id should equal(UUID.fromString("8e5b9cea-62a5-43f1-a268-74bb740e2f70"))
    asset.parentId.get should equal(contentFolder.id)
    asset.originalMetadataFiles should equal(List(UUID.fromString("9deee21f-5062-4aaa-82f4-2d31fb4262b4")))
    asset.description.isEmpty should equal(true)
    asset.transferringBody should equal(None)
    asset.transferCompleteDatetime.get should equal(OffsetDateTime.parse("2023-03-24T10:13:04.280Z"))
    asset.upstreamSystem.display should equal("Parliament Migration")
    asset.digitalAssetSource should equal("Digital Surrogate")
    asset.digitalAssetSubtype should equal(None)
    asset.filePath should equal("")
    asset.correlationId should equal(None)
    asset.idFields should equal(List(IdField("Code", "ABC/123")))

    file.id should equal(UUID.fromString("e7c8511b-6a9b-4146-839d-33d7a40e9232"))
    file.parentId.get should equal(UUID.fromString("8e5b9cea-62a5-43f1-a268-74bb740e2f70"))
    file.title should equal("Test title.txt")
    file.sortOrder should equal(1)
    file.name should equal("Test title.txt")
    file.fileSize should equal(1)
    file.representationType should equal(Preservation)
    file.representationSuffix should equal(1)
    file.location.toString should equal("s3://output-bucket/8e5b9cea-62a5-43f1-a268-74bb740e2f70/2f162f2b-4fac-417a-b484-3bc68c02ae1d")
    file.checksums.head.algorithm should equal("SHA256")
    file.checksums.head.fingerprint.trim should equal("1b6f487a4f27f1e94ff4e794f4dce56e6d9ba5ec49919def6a3aded7f527b769")
  }

  "lambda" should "error if the IO preservation system ref is missing" in {
    val xml = <XIP><InformationObject></InformationObject></XIP>
    val res = runLambda(List(lockTableItem), Map("key" -> xml.toString))
    val err = res.output.left.value

    err.getMessage should equal("Preservation system Ref not found")
  }

  "lambda" should "error if the SourceID identifier is missing" in {
    val xml = <XIP>
      <InformationObject><Ref>1f1cd3e0-4700-4c04-b556-4fccd83bd819</Ref></InformationObject>
    </XIP>
    val res = runLambda(List(lockTableItem), Map("key" -> xml.toString))
    val err = res.output.left.value

    err.getMessage should equal("Identifier SourceID not found")
  }

  "lambda" should "error if the title is missing" in {
    val xml = <XIP>
      <InformationObject><Ref>1f1cd3e0-4700-4c04-b556-4fccd83bd819</Ref></InformationObject>
      <Identifier><Type>SourceID</Type> <Value>8e5b9cea-62a5-43f1-a268-74bb740e2f70</Value></Identifier>
      </XIP>
    val res = runLambda(List(lockTableItem), Map("key" -> xml.toString))
    val err = res.output.left.value

    err.getMessage should equal("Title not found for 1f1cd3e0-4700-4c04-b556-4fccd83bd819")
  }

  "lambda" should "error if the source system is missing" in {
    val xml = <XIP>
      <InformationObject><Ref>1f1cd3e0-4700-4c04-b556-4fccd83bd819</Ref><Title>Test</Title></InformationObject>
      <Identifier><Type>SourceID</Type> <Value>8e5b9cea-62a5-43f1-a268-74bb740e2f70</Value></Identifier>
      </XIP>
    val res = runLambda(List(lockTableItem), Map("key" -> xml.toString))
    val err = res.output.left.value

    err.getMessage should equal("Source system not found for 1f1cd3e0-4700-4c04-b556-4fccd83bd819")
  }

  "lambda" should "error if the asset source is missing" in {
    val xml = <XIP>
      <InformationObject><Ref>1f1cd3e0-4700-4c04-b556-4fccd83bd819</Ref> <Title>Test</Title></InformationObject>
      <Identifier><Type>SourceID</Type> <Value>8e5b9cea-62a5-43f1-a268-74bb740e2f70</Value></Identifier>
      <Metadata>
        <Ref>0e24acb1-cfaa-42be-8dcd-cad7d7cf0baf</Ref>
        <Entity>4b44388f-d788-44f7-a61e-21944941598b</Entity>
        <Content>
          <Source>
            <UpstreamSystem>Parliament Migration</UpstreamSystem>
          </Source>
        </Content>
      </Metadata>
    </XIP>
    val res = runLambda(List(lockTableItem), Map("key" -> xml.toString))
    val err = res.output.left.value

    err.getMessage should equal("Asset source not found for 1f1cd3e0-4700-4c04-b556-4fccd83bd819")
  }

  "lambda" should "error if the content object ref is missing" in {
    val xml = <XIP>
      <InformationObject>
        <Ref>1f1cd3e0-4700-4c04-b556-4fccd83bd819</Ref> <Title>Test</Title>
      </InformationObject>
      <Identifier>
        <Type>SourceID</Type> <Value>8e5b9cea-62a5-43f1-a268-74bb740e2f70</Value>
      </Identifier>
      <Metadata>
        <Ref>0e24acb1-cfaa-42be-8dcd-cad7d7cf0baf</Ref>
        <Entity>4b44388f-d788-44f7-a61e-21944941598b</Entity>
        <Content>
          <Source>
            <UpstreamSystem>Parliament Migration</UpstreamSystem>
            <DigitalAssetSource>Digital Surrogate</DigitalAssetSource>
          </Source>
        </Content>
      </Metadata>
      <CCContentObjects>
        <CCContentObject>
          <ContentObject>
          </ContentObject>
        </CCContentObject>
      </CCContentObjects>
    </XIP>
    val res = runLambda(List(lockTableItem), Map("key" -> xml.toString))
    val err = res.output.left.value

    err.getMessage should equal("Cannot get a Ref from a CO")
  }

  "lambda" should "error if the bitstream name is missing" in {
    val xml = <XIP>
      <InformationObject>
        <Ref>1f1cd3e0-4700-4c04-b556-4fccd83bd819</Ref> <Title>Test</Title>
      </InformationObject>
      <Identifier>
        <Type>SourceID</Type> <Value>8e5b9cea-62a5-43f1-a268-74bb740e2f70</Value>
      </Identifier>
      <Metadata>
        <Ref>0e24acb1-cfaa-42be-8dcd-cad7d7cf0baf</Ref>
        <Entity>4b44388f-d788-44f7-a61e-21944941598b</Entity>
        <Content>
          <Source>
            <UpstreamSystem>Parliament Migration</UpstreamSystem>
            <DigitalAssetSource>Digital Surrogate</DigitalAssetSource>
          </Source>
        </Content>
      </Metadata>
      <CCContentObjects>
        <CCContentObject>
          <ContentObject><Ref>4b02b6a2-8daf-46b4-af73-2c6e485cca69</Ref></ContentObject>
        </CCContentObject>
      </CCContentObjects>
    </XIP>
    val res = runLambda(List(lockTableItem), Map("key" -> xml.toString))
    val err = res.output.left.value

    err.getMessage should equal("Cannot extract id from bitstream name for CO 4b02b6a2-8daf-46b4-af73-2c6e485cca69")
  }

  "lambda" should "error if the content object title is missing" in {
    val xml = <XIP>
      <InformationObject>
        <Ref>1f1cd3e0-4700-4c04-b556-4fccd83bd819</Ref> <Title>Test</Title>
      </InformationObject>
      <Identifier>
        <Type>SourceID</Type> <Value>8e5b9cea-62a5-43f1-a268-74bb740e2f70</Value>
      </Identifier>
      <Metadata>
        <Ref>0e24acb1-cfaa-42be-8dcd-cad7d7cf0baf</Ref>
        <Entity>4b44388f-d788-44f7-a61e-21944941598b</Entity>
        <Content>
          <Source>
            <UpstreamSystem>Parliament Migration</UpstreamSystem>
            <DigitalAssetSource>Digital Surrogate</DigitalAssetSource>
          </Source>
        </Content>
      </Metadata>
      <CCContentObjects>
        <CCContentObject>
          <ContentObject>
            <Ref>4b02b6a2-8daf-46b4-af73-2c6e485cca69</Ref>
          </ContentObject>
          <Bitstreams>
            <Bitstream>e7c8511b-6a9b-4146-839d-33d7a40e9232.tif</Bitstream>
          </Bitstreams>
        </CCContentObject>
      </CCContentObjects>
    </XIP>
    val res = runLambda(List(lockTableItem), Map("key" -> xml.toString))
    val err = res.output.left.value

    err.getMessage should equal("Title not found for file e7c8511b-6a9b-4146-839d-33d7a40e9232")
  }

  "lambda" should "error if the file size is missing" in {
    val xml = <XIP>
      <InformationObject>
        <Ref>1f1cd3e0-4700-4c04-b556-4fccd83bd819</Ref> <Title>Test</Title>
      </InformationObject>
      <Identifier>
        <Type>SourceID</Type> <Value>8e5b9cea-62a5-43f1-a268-74bb740e2f70</Value>
      </Identifier>
      <Metadata>
        <Ref>0e24acb1-cfaa-42be-8dcd-cad7d7cf0baf</Ref>
        <Entity>4b44388f-d788-44f7-a61e-21944941598b</Entity>
        <Content>
          <Source>
            <UpstreamSystem>Parliament Migration</UpstreamSystem>
            <DigitalAssetSource>Digital Surrogate</DigitalAssetSource>
          </Source>
        </Content>
      </Metadata>
      <CCContentObjects>
        <CCContentObject>
          <ContentObject>
            <Ref>4b02b6a2-8daf-46b4-af73-2c6e485cca69</Ref>
            <Title>Test title</Title>
          </ContentObject>
          <Bitstreams>
            <Bitstream>e7c8511b-6a9b-4146-839d-33d7a40e9232.tif</Bitstream>
          </Bitstreams>
        </CCContentObject>
      </CCContentObjects>
    </XIP>
    val res = runLambda(List(lockTableItem), Map("key" -> xml.toString))
    val err = res.output.left.value

    err.getMessage should equal("File size not found for file e7c8511b-6a9b-4146-839d-33d7a40e9232")
  }

  "lambda" should "not upload anything if there are no items in the lock table" in {
    val res = runLambda(Nil, Map.empty)
    res.s3Map.size should equal(0)
  }

  "lambda" should "fail if the key is not in S3" in {
    val res = runLambda(List(lockTableItem), Map("invalid-key" -> ""))
    res.output.left.value.getMessage should equal("Error downloading key")
  }
