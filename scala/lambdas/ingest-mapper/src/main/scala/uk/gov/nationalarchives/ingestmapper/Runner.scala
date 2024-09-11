package uk.gov.nationalarchives.ingestmapper

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import sttp.client3.HttpClientSyncBackend
import uk.gov.nationalarchives.DADynamoDBClient
import uk.gov.nationalarchives.ingestmapper.Lambda.*

import java.net.URI
import java.time.Instant
import java.util.UUID

object Runner extends App {
  val input = Input("TDR_7114d947-360e-42db-b2f8-d6628c276e67_3", URI.create("s3://intg-dr2-ingest-raw-cache/TDR_7114d947-360e-42db-b2f8-d6628c276e67_3/metadata.json"))
  val backend = HttpClientSyncBackend()
  private val apiUrl = "https://discovery.nationalarchives.gov.uk"
  val discoveryService = DiscoveryService(apiUrl, () => UUID.randomUUID).unsafeRunSync()
  val dependencies = Dependencies(MetadataService(discoveryService), DADynamoDBClient[IO](), () => Instant.now)
  val config = Config("intg-dr2-ingest-files", apiUrl)
  val res = new Lambda().handler(input, config, dependencies).unsafeRunSync()

}
