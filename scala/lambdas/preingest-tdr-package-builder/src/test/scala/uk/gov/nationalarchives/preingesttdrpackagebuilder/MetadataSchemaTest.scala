package uk.gov.nationalarchives.preingesttdrpackagebuilder

import com.networknt.schema.InputFormat.JSON
import com.networknt.schema.{SchemaRegistry, SpecificationVersion}
import io.circe.Json
import org.scalatest.flatspec.AnyFlatSpec

import java.io.{File, FileInputStream}
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.UUID

class MetadataSchemaTest extends AnyFlatSpec with TableDrivenPropertyChecks {

  def driPackageMetadata(description: Option[String]): String =
    val transferDateTime: String = DateTimeFormatter
      .ofPattern("yyyy-MM-dd HH:mm:ss")
      .format(LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC")))

    val driFields = List(
      Option("Series" -> Json.fromString("Series")),
      Option("UUID" -> Json.fromString(UUID.randomUUID.toString)),
      description.map(d => "description" -> Json.fromString(d)),
      Option("TransferInitiatedDatetime" -> Json.fromString(transferDateTime)),
      Option("driBatchReference" -> Json.fromString("DRIBatchRef")),
      Option("Filename" -> Json.fromString("File")),
      Option("checksum_sha256" -> Json.fromString("checksum")),
      Option("FileReference" -> Json.fromString("FileRef")),
      Option("ClientSideOriginalFilepath" -> Json.fromString("/path/to/file"))
    ).flatten
    Json.obj(driFields*).noSpaces

  def tdrPackageMetadata(description: Option[String]): String =
    val transferDateTime: String = DateTimeFormatter
      .ofPattern("yyyy-MM-dd HH:mm:ss")
      .format(LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC")))

    val tdrFields = List(
      Option("Series" -> Json.fromString("Series")),
      Option("UUID" -> Json.fromString(UUID.randomUUID.toString)),
      description.map(d => "description" -> Json.fromString(d)),
      Option("TransferringBody" -> Json.fromString("Body")),
      Option("TransferInitiatedDatetime" -> Json.fromString(transferDateTime)),
      Option("ConsignmentReference" -> Json.fromString("ConsignmentRef")),
      Option("Filename" -> Json.fromString("File")),
      Option("SHA256ServerSideChecksum" -> Json.fromString("checksum")),
      Option("FileReference" -> Json.fromString("FileRef")),
      Option("ClientSideOriginalFilepath" -> Json.fromString("/path/to/file"))
    ).flatten
    Json.obj(tdrFields*).noSpaces

  val sources: TableFor2[String, Option[String] => String] = Table(
    ("sourceSystem", "metadataFn"),
    ("dri", driPackageMetadata),
    ("tdr", tdrPackageMetadata)
  )

  forAll(sources) { (sourceSystem, metadataFn) =>
    s"the $sourceSystem metadata schema" should "validate against the PackageMetadata case class" in {
      val projectRoot = new File(".").getCanonicalPath
        .replace("/scala/lambdas/preingest-tdr-package-builder", "")

      val schemaPath = s"$projectRoot/common/preingest-$sourceSystem/metadata-schema.json"

      List(None, Option("description")).foreach { description =>
        val fileStream = new FileInputStream(schemaPath)
        val registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
        registry.getSchema(fileStream).validate(metadataFn(description), JSON).isEmpty should equal(true)
        fileStream.close()
      }
    }
  }

}
