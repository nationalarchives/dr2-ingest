package uk.gov.nationalarchives.dp

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import fs2.interop.reactivestreams._
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.reactivestreams.Publisher
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.mockito._
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jFactory
import uk.gov.nationalarchives.DAS3Client
import uk.gov.nationalarchives.dp.FileProcessors._
import uk.gov.nationalarchives.dp.client.AdminClient
import uk.gov.nationalarchives.dp.client.FileInfo._

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._
import scala.xml.Elem

class FileProcessorsTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {

  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jFactory.create[IO].getLogger
  val secretName = "testSecret"
  val bucket = "testBucket"

  def result[T](res: IO[T]): T = res.unsafeRunSync()

  "processFiles" should "add the schema files correctly" in {
    val preservicaClient = mock[AdminClient[IO]]
    val s3Client = mock[DAS3Client[IO]]
    val schemaCaptor = ArgumentCaptor.forClass(classOf[List[SchemaFileInfo]])

    when(preservicaClient.addOrUpdateSchemas(schemaCaptor.capture())).thenReturn(IO.unit)
    when(preservicaClient.addOrUpdateTransforms(any[List[TransformFileInfo]]())).thenReturn(IO.unit)
    when(preservicaClient.addOrUpdateMetadataTemplates(any[List[MetadataTemplateInfo]]()))
      .thenReturn(IO.unit)
    when(preservicaClient.addOrUpdateIndexDefinitions(any[List[IndexDefinitionInfo]]()))
      .thenReturn(IO.unit)

    val s3ClosureDataEvent: S3Object =
      createS3Object(s3Client, <ClosureData></ClosureData>, "schemas/closure-data-schema.xsd")
    val s3ClosureResultEvent: S3Object =
      createS3Object(s3Client, <ClosureResult></ClosureResult>, "schemas/closure-result-schema.xsd")

    result(processFiles(preservicaClient, s3Client, List(s3ClosureDataEvent, s3ClosureResultEvent)))

    val schemaFileInfo: List[SchemaFileInfo] = schemaCaptor.getAllValues.asScala.toList.flatten
    val sortedFileInfo = schemaFileInfo.sortBy(_.name)
    sortedFileInfo.size should equal(2)
    val closureDataSchema = sortedFileInfo.head
    closureDataSchema.name should equal("closure-data-schema")
    closureDataSchema.description should equal("")
    closureDataSchema.originalName should equal("closure-data-schema.xsd")
    closureDataSchema.xmlData should equal(<ClosureData></ClosureData>.toString)

    val closureResultSchema = sortedFileInfo.last
    closureResultSchema.name should equal("closure-result-schema")
    closureResultSchema.description should equal("")
    closureResultSchema.originalName should equal("closure-result-schema.xsd")
    closureResultSchema.xmlData should equal(<ClosureResult></ClosureResult>.toString)
  }

  "processFiles" should "add the transform files correctly" in {
    val preservicaClient = mock[AdminClient[IO]]
    val s3Client = mock[DAS3Client[IO]]

    val transformCaptor = ArgumentCaptor.forClass(classOf[List[TransformFileInfo]])
    when(preservicaClient.addOrUpdateSchemas(any[List[SchemaFileInfo]])).thenReturn(IO.unit)
    when(preservicaClient.addOrUpdateTransforms(transformCaptor.capture())).thenReturn(IO.unit)
    when(preservicaClient.addOrUpdateMetadataTemplates(any[List[MetadataTemplateInfo]]()))
      .thenReturn(IO.unit)
    when(preservicaClient.addOrUpdateIndexDefinitions(any[List[IndexDefinitionInfo]]()))
      .thenReturn(IO.unit)

    val dataEditElem = <EditDataTransform xmlns:tns="https://nationalarchives.gov.uk/ClosureData"></EditDataTransform>
    val dataViewElem = <ViewDataTransform xmlns:tns="https://nationalarchives.gov.uk/ClosureData"></ViewDataTransform>
    val resultViewElem =
      <ViewResultTransform xmlns:tns="https://nationalarchives.gov.uk/ClosureResult"></ViewResultTransform>
    val dataEditObject = createS3Object(s3Client, dataEditElem, "transforms/closure-data-edit-transform.xsl")
    val dataViewObject = createS3Object(s3Client, dataViewElem, "transforms/closure-data-view-transform.xsl")
    val resultViewObject = createS3Object(s3Client, resultViewElem, "transforms/closure-result-view-transform.xsl")
    result(processFiles(preservicaClient, s3Client, List(dataEditObject, dataViewObject, resultViewObject)))

    val transformFileInfo: List[TransformFileInfo] = transformCaptor.getAllValues.asScala.toList.flatten
    val sortedFileInfo = transformFileInfo.sortBy(_.name)
    sortedFileInfo.size should equal(3)
    val closureDataEdit = sortedFileInfo.head
    val closureDataView = sortedFileInfo.tail.head
    val closureResultView = sortedFileInfo.last

    closureDataEdit.name should equal("closure-data-edit-transform")
    closureDataEdit.originalName should equal("closure-data-edit-transform.xsl")
    closureDataEdit.from should equal("https://nationalarchives.gov.uk/ClosureData")
    closureDataEdit.to should equal("http://www.w3.org/1999/xhtml")
    closureDataEdit.purpose should equal("edit")
    closureDataEdit.xmlData should equal(dataEditElem.toString)

    closureDataView.name should equal("closure-data-view-transform")
    closureDataView.originalName should equal("closure-data-view-transform.xsl")
    closureDataView.from should equal("https://nationalarchives.gov.uk/ClosureData")
    closureDataView.to should equal("http://www.w3.org/1999/xhtml")
    closureDataView.purpose should equal("view")
    closureDataView.xmlData should equal(dataViewElem.toString)

    closureResultView.name should equal("closure-result-view-transform")
    closureResultView.originalName should equal("closure-result-view-transform.xsl")
    closureResultView.from should equal("https://nationalarchives.gov.uk/ClosureResult")
    closureResultView.to should equal("http://www.w3.org/1999/xhtml")
    closureResultView.purpose should equal("view")
    closureResultView.xmlData should equal(resultViewElem.toString)
  }

  "processFiles" should "add the metadata templates correctly" in {
    val preservicaClient = mock[AdminClient[IO]]
    val s3Client = mock[DAS3Client[IO]]
    val metadataTemplateCaptor = ArgumentCaptor.forClass(classOf[List[MetadataTemplateInfo]])
    when(preservicaClient.addOrUpdateSchemas(any[List[SchemaFileInfo]])).thenReturn(IO.unit)
    when(preservicaClient.addOrUpdateTransforms(any[List[TransformFileInfo]])).thenReturn(IO.unit)
    when(preservicaClient.addOrUpdateMetadataTemplates(metadataTemplateCaptor.capture()))
      .thenReturn(IO.unit)
    when(preservicaClient.addOrUpdateIndexDefinitions(any[List[IndexDefinitionInfo]]()))
      .thenReturn(IO.unit)

    val closureDataObject =
      createS3Object(s3Client, <ClosureData></ClosureData>, "metadata_templates/closure-data-template.xml")

    result(processFiles(preservicaClient, s3Client, List(closureDataObject)))

    val closureDataTemplateValues: List[MetadataTemplateInfo] = metadataTemplateCaptor.getAllValues.asScala.head
    val sortedFileInfo = closureDataTemplateValues.sortBy(_.name)
    sortedFileInfo.size should equal(1)
    sortedFileInfo.head.name should equal("closure-data-template")
  }

  "processFiles" should "add the index definitions correctly" in {
    val preservicaClient = mock[AdminClient[IO]]
    val s3Client = mock[DAS3Client[IO]]
    val indexDefinitionCaptor = ArgumentCaptor.forClass(classOf[List[IndexDefinitionInfo]])
    when(preservicaClient.addOrUpdateSchemas(any[List[SchemaFileInfo]])).thenReturn(IO.unit)
    when(preservicaClient.addOrUpdateTransforms(any[List[TransformFileInfo]])).thenReturn(IO.unit)
    when(preservicaClient.addOrUpdateMetadataTemplates(any[List[MetadataTemplateInfo]]))
      .thenReturn(IO.unit)
    when(preservicaClient.addOrUpdateIndexDefinitions(indexDefinitionCaptor.capture()))
      .thenReturn(IO.unit)

    val closureResultDefinitionObject = createS3Object(
      s3Client,
      <ClosureResultDefinition></ClosureResultDefinition>,
      "index_definitions/closure-result-index-definition"
    )

    result(processFiles(preservicaClient, s3Client, List(closureResultDefinitionObject)))

    val indexDefinitionInfo: List[IndexDefinitionInfo] = indexDefinitionCaptor.getAllValues.asScala.head
    val sortedFileInfo = indexDefinitionInfo.sortBy(_.name)
    sortedFileInfo.size should equal(1)
    sortedFileInfo.head.name should equal("closure-result-index-definition")
    sortedFileInfo.head.xmlData should equal(<ClosureResultDefinition></ClosureResultDefinition>.toString)
  }

  private def createS3Object(s3Client: DAS3Client[IO], elem: Elem, key: String) = {
    val closureDataPublisher = createPublisher(elem)

    when(s3Client.download(bucket, key)).thenReturn(IO(closureDataPublisher))
    S3Object(bucket, key)
  }

  def createPublisher(elem: Elem): Publisher[ByteBuffer] = {
    val response = ByteBuffer.wrap(elem.toString.getBytes)
    val stream: Stream[IO, ByteBuffer] = Stream.emit(response)
    stream.toUnicastPublisher.allocated.unsafeRunSync()._1.publisher
  }
}
