package uk.gov.nationalarchives.preingesttdrpackagebuilder

import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.SpecVersion.VersionFlag
import com.networknt.schema.InputFormat.JSON
import org.scalatest.flatspec.AnyFlatSpec
import uk.gov.nationalarchives.preingesttdrpackagebuilder.Lambda.TDRMetadata

import java.io.{File, FileInputStream}
import io.circe.syntax.*
import io.circe.generic.auto.*
import org.scalatest.matchers.should.Matchers.*

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.UUID

class MetadataSchemaTest extends AnyFlatSpec {

  "the metadata schema" should "validate against the TDRMetadata case class" in {
    val projectRoot = new File(".").getCanonicalPath
      .replace("/scala/lambdas/preingest-tdr-package-builder", "")

    val schemaPath = s"$projectRoot/python/lambdas/copy-files-from-tdr/metadata-schema.json"

    val transferDateTime = DateTimeFormatter
      .ofPattern("yyyy-MM-dd HH:mm:ss")
      .format(LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC")))

    def tdrMetadata(description: Option[String]) = TDRMetadata(
      "Series",
      UUID.randomUUID,
      description,
      "Body",
      transferDateTime,
      "ConsignmentRef",
      "File",
      "checksum",
      "FileRef"
    ).asJson.noSpaces

    List(None, Option("description")).foreach { description =>

      val fileStream = new FileInputStream(schemaPath)
      val factory = JsonSchemaFactory.getInstance(VersionFlag.V202012)
      factory.getSchema(fileStream).validate(tdrMetadata(description), JSON).isEmpty should equal(true)
      fileStream.close()
    }
  }
}
