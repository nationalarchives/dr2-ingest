package uk.gov.nationalarchives.preingesttdrpackagebuilder

import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.SpecVersion.VersionFlag
import com.networknt.schema.InputFormat.JSON
import org.scalatest.flatspec.AnyFlatSpec
import uk.gov.nationalarchives.preingesttdrpackagebuilder.Lambda.PackageMetadata
import uk.gov.nationalarchives.preingesttdrpackagebuilder.TestUtils.given

import java.io.{File, FileInputStream}
import io.circe.syntax.*
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Checksum

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.UUID

class MetadataSchemaTest extends AnyFlatSpec {

  "the metadata schema" should "validate against the TDRMetadata case class" in {
    val projectRoot = new File(".").getCanonicalPath
      .replace("/scala/lambdas/preingest-tdr-package-builder", "")

    val schemaPath = s"$projectRoot/common/preingest-tdr/metadata-schema.json"

    val transferDateTime = DateTimeFormatter
      .ofPattern("yyyy-MM-dd HH:mm:ss")
      .format(LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC")))

    def packageMetadata(description: Option[String]) = PackageMetadata(
      "Series",
      UUID.randomUUID,
      None,
      description,
      Option("Body"),
      transferDateTime,
      "ConsignmentRef",
      "File",
      List(Checksum("sha256", "checksum")),
      "FileRef",
      "/path/to/file",
      None
    ).asJson.noSpaces

    List(None, Option("description")).foreach { description =>

      val fileStream = new FileInputStream(schemaPath)
      val factory = JsonSchemaFactory.getInstance(VersionFlag.V202012)
      factory.getSchema(fileStream).validate(packageMetadata(description), JSON).isEmpty should equal(true)
      fileStream.close()
    }
  }
}
