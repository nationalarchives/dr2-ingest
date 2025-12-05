package uk.gov.nationalarchives.ingestassetreconciler.testUtils

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.{BatchWriteItemResponse, ResourceNotFoundException}
import sttp.capabilities
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DADynamoDBClient
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, IdentifierResponse}
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.InformationObject
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.Original
import uk.gov.nationalarchives.dp.client.EntityClient.{AddEntityRequest, EntitiesUpdated, EntityType, UpdateEntityRequest, Identifier as PreservicaIdentifier}
import uk.gov.nationalarchives.dp.client.{Client, DataProcessor, Entities, EntityClient}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{Identifier as DynamoIdentifier, *}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.FileRepresentationType.*
import uk.gov.nationalarchives.ingestassetreconciler.Lambda
import uk.gov.nationalarchives.ingestassetreconciler.Lambda.*

import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.{OffsetDateTime, ZonedDateTime}
import java.util.UUID
import scala.jdk.CollectionConverters.*

object ExternalServicesTestUtils extends AnyFlatSpec with TableDrivenPropertyChecks {
  val batchId: String = "TEST-ID"
  case class AssetWithChildren(asset: AssetDynamoItem, children: List[FileDynamoItem])
  case class BitStreamEntity(entity: Entity, bitstreams: List[BitStreamInfo])
  case class FullEntity(entity: Entity, identifiers: List[PreservicaIdentifier], contentObjects: List[BitStreamEntity])

  def generateInput(assetId: UUID): Input = Input("", batchId, assetId)
  val config: Config = Config("", "", "")

  def resourceNotFound(dynamoError: Boolean): IO[Unit] = IO.raiseWhen(dynamoError)(ResourceNotFoundException.builder.message(s"Error getting Dynamo items").build)

  def notImplemented[T]: IO[T] = IO.raiseError(new Exception("Not implemented"))

  def dynamoClient(ref: Ref[IO, List[AssetWithChildren]], dynamoError: Boolean): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = notImplemented

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      ref.get.map { existing =>
        val parentPath = (for {
          dynamoValues <- Option(requestCondition.attributes.values)
          value <- dynamoValues.toExpressionAttributeValues
          parentPath <- value.asScala.get(":parentPath")
        } yield parentPath.s()).getOrElse(UUID.randomUUID.toString)
        existing.filter(_.asset.id.toString == parentPath).flatMap(_.children).map(_.asInstanceOf[U])
      }

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] =
      val key = primaryKeys.head.asInstanceOf[FilesTablePrimaryKey]
      resourceNotFound(dynamoError) >> ref.get.map { existing =>
        existing.map(_.asset).filter(_.id == key.partitionKey.id).map(_.asInstanceOf[T])
      }

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = notImplemented

  def preservicaClient(ref: Ref[IO, List[FullEntity]], apiError: Boolean): EntityClient[IO, Fs2Streams[IO]] =
    new EntityClient[IO, Fs2Streams[IO]] {

      override val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME

      override def metadataForEntity(entity: Entity): IO[EntityClient.EntityMetadata] = notImplemented

      override def getBitstreamInfo(contentRef: UUID): IO[Seq[Client.BitStreamInfo]] = ref.get.map { existing =>
        existing.flatMap(_.contentObjects).filter(_.entity.ref == contentRef).flatMap(_.bitstreams)
      }

      override def streamAllEntityRefs(repTypeFilter: Option[EntityClient.RepresentationType]): fs2.Stream[IO, Entities.EntityRef] = fs2.Stream.empty[IO]

      override def getEntity(entityRef: UUID, entityType: EntityType): IO[Entity] = notImplemented

      override def getEntityIdentifiers(entity: Entity): IO[Seq[IdentifierResponse]] = notImplemented

      override def getUrlsToIoRepresentations(ioEntityRef: UUID, representationType: Option[EntityClient.RepresentationType]): IO[Seq[String]] = IO.pure(Seq("/1"))

      override def getContentObjectsFromRepresentation(ioEntityRef: UUID, representationType: EntityClient.RepresentationType, repTypeIndex: Int): IO[Seq[Entity]] = ref.get.map {
        existing =>
          existing.filter(_.entity.ref == ioEntityRef).flatMap(_.contentObjects.map(_.entity))
      }

      override def addEntity(aer: AddEntityRequest): IO[UUID] = notImplemented

      override def updateEntity(uer: UpdateEntityRequest): IO[String] = notImplemented

      override def updateEntityIdentifiers(entity: Entity, identifiers: Seq[IdentifierResponse]): IO[Seq[IdentifierResponse]] = notImplemented

      override def streamBitstreamContent[T](stream: capabilities.Streams[Fs2Streams[IO]])(url: String, streamFn: stream.BinaryStream => IO[T]): IO[T] = notImplemented

      override def entitiesUpdatedSince(sinceDateTime: ZonedDateTime, startEntry: Int, maxEntries: Int, potentialEndDate: Option[ZonedDateTime]): IO[EntitiesUpdated] =
        notImplemented

      override def entityEventActions(entity: Entity, startEntry: Int, maxEntries: Int): IO[Seq[DataProcessor.EventAction]] = notImplemented

      override def entitiesPerIdentifier(identifiers: Seq[PreservicaIdentifier]): IO[Map[PreservicaIdentifier, Seq[Entity]]] =
        IO.raiseWhen(apiError)(new Exception("API has encountered an error")) >>
          ref.get.map { existing =>
            existing
              .flatMap(_.identifiers)
              .map { value =>
                value -> existing.filter(_.identifiers.contains(value)).map(_.entity)
              }
              .toMap
              .view
              .filterKeys(identifiers.contains)
              .toMap
          }

      override def addIdentifierForEntity(entityRef: UUID, entityType: EntityType, identifier: PreservicaIdentifier): IO[String] = notImplemented

      override def getPreservicaNamespaceVersion(endpoint: String): IO[Float] = notImplemented
    }

  def entity(title: Option[String] = None): Entity = Entity(
    Some(InformationObject),
    UUID.randomUUID,
    title,
    None,
    false,
    Some(InformationObject.entityPath),
    None,
    None
  )

  def bitstreamInfo(checksum: Option[String], title: Option[String]): BitStreamInfo = BitStreamInfo(
    s"${UUID.randomUUID}.json",
    1235,
    "http://localhost/api/entity/content-objects/4dee285b-64e4-49f8-942e-84ab460b5af6/generations/1/bitstreams/1/content",
    List(Fixity("SHA256", checksum.getOrElse("checksum")), Fixity("SHA1", checksum.getOrElse("checksum2"))),
    1,
    Original,
    title,
    None
  )

  def generateFullEntity(assetId: UUID, title: Option[String] = None, checksum: Option[String] = None): FullEntity = {
    val bitStreamEntity = BitStreamEntity(entity(), List(bitstreamInfo(checksum, title)))
    FullEntity(entity(title), List(PreservicaIdentifier("SourceID", assetId.toString)), List(bitStreamEntity))
  }

  def generateAsset: AssetDynamoItem = AssetDynamoItem(
    batchId,
    UUID.randomUUID,
    None,
    Asset,
    None,
    None,
    Option(transferringBody),
    Option(OffsetDateTime.parse("2023-06-01T00:00Z")),
    upstreamSystem,
    digitalAssetSource,
    Option(digitalAssetSubtype),
    Nil,
    Nil,
    1,
    false,
    None,
    "/a/file/path"
  )

  def generateFile: FileDynamoItem = FileDynamoItem(
    "TEST-ID",
    UUID.randomUUID,
    Option("parentPath"),
    "name",
    File,
    Option("title"),
    Option("description"),
    1,
    1,
    List(Checksum("SHA256", "checksum")),
    Option(s"ext"),
    PreservationRepresentationType,
    1,
    List(DynamoIdentifier("Test2", "testIdentifier4")),
    1,
    URI.create("s3://bucket/key")
  )

  def runLambda(
      input: Input,
      items: List[AssetWithChildren],
      entities: List[FullEntity],
      dynamoError: Boolean = false,
      apiError: Boolean = false
  ): (List[FullEntity], List[AssetWithChildren], Either[Throwable, StateOutput]) = (for {
    itemsRef <- Ref.of[IO, List[AssetWithChildren]](items)
    entitiesRef <- Ref.of[IO, List[FullEntity]](entities)
    res <- Lambda()
      .handler(input, config, Dependencies(preservicaClient(entitiesRef, apiError), dynamoClient(itemsRef, dynamoError), UUID.randomUUID, () => OffsetDateTime.now()))
      .attempt
    entities <- entitiesRef.get
    items <- itemsRef.get
  } yield (entities, items, res)).unsafeRunSync()
}
