package uk.gov.nationalarchives.ingestfindexistingasset.testUtils

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import io.circe.syntax.given
import io.circe.generic.auto.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import io.circe.Encoder
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.{BatchWriteItemResponse, ResourceNotFoundException}
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse
import sttp.capabilities
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, IdentifierResponse}
import uk.gov.nationalarchives.dp.client.EntityClient.{AddEntityRequest, EntitiesUpdated, EntityType, Identifier, UpdateEntityRequest, Identifier as PreservicaIdentifier}
import uk.gov.nationalarchives.dp.client.EntityClient.SecurityTag.*
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.{Client, DataProcessor, Entities, EntityClient}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{AssetDynamoItem, digitalAssetSource, digitalAssetSubtype, transferringBody, upstreamSystem}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*
import uk.gov.nationalarchives.ingestfindexistingasset.Lambda
import uk.gov.nationalarchives.ingestfindexistingasset.Lambda.*

import scala.jdk.CollectionConverters.*
import java.time.{OffsetDateTime, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.UUID

class ExternalServicesTestUtils extends AnyFlatSpec with EitherValues {

  val input: Input = Input(List(InputItems(UUID.randomUUID, "batchId")), "taskToken")
  val firstItem: InputItems = input.Items.head
  val assetId: UUID = firstItem.id
  val batchId: String = firstItem.batchId
  val config: Config = Config("", "table-name")

  def generateAsset: AssetDynamoItem = AssetDynamoItem(
    batchId,
    assetId,
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
    "/a/file/path",
    1779382126L
  )

  def generateEntity(identifierValue: String, entityType: Option[EntityType] = Some(InformationObject)): EntityWithIdentifiers = {
    val entity = Entity(
      entityType,
      UUID.randomUUID,
      Some(s"mock title"),
      Some(s"mock description"),
      deleted = false,
      entityType.map(_.entityPath),
      Some(Unknown),
      None
    )
    EntityWithIdentifiers(entity, List(Identifier("SourceID", identifierValue)))
  }

  case class EntityWithIdentifiers(entity: Entity, identifiers: List[PreservicaIdentifier])

  case class SfnSendFailureOutput(taskToken: String, error: String)

  def notImplemented[T]: IO[T] = IO.raiseError(new Exception("Not implemented"))

  def sfnClient(ref: Ref[IO, List[StateOutput]], sfnSendFailureOutputRef: Ref[IO, List[SfnSendFailureOutput]], successError: Boolean, failureError: Boolean): DASFNClient[IO] =
    new DASFNClient[IO] {
      override def startExecution[T <: Product](stateMachineArn: String, input: T, name: Option[String])(using enc: Encoder[T]): IO[StartExecutionResponse] = IO.stub

      override def listStepFunctions(stepFunctionArn: String, status: DASFNClient.Status): IO[List[String]] = IO.stub

      override def sendTaskSuccess[T: Encoder](taskToken: String, potentialOutput: Option[T]): IO[Unit] =
        if successError then IO.raiseError(new Exception(s"Failure sending task success for task token $taskToken"))
        else
          ref.update { sentOutputs =>
            potentialOutput.get.asInstanceOf[StateOutput] :: sentOutputs
          }

      override def sendTaskFailure(taskToken: String, potentialError: Option[String]): IO[Unit] = {
        if failureError then IO.raiseError(new Exception(s"Failure sending task failure for task token $taskToken"))
        else
          sfnSendFailureOutputRef.update { existing =>
            SfnSendFailureOutput(taskToken, potentialError.getOrElse("")) :: existing
          }
      }
    }

  def dynamoClient(ref: Ref[IO, List[AssetDynamoItem]], dynamoError: Boolean): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = notImplemented

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      notImplemented

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] =
      IO.raiseWhen(dynamoError)(ResourceNotFoundException.builder.message(s"$tableName not found").build) >> ref.get.map(_.map(_.asInstanceOf[T]))

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = ref
      .update { existing =>
        val id = UUID.fromString(dynamoDbRequest.primaryKeyAndItsValue("id").s())
        val skipIngest = dynamoDbRequest.attributeNamesAndValuesToUpdate("skipIngest").bool()
        existing.map { each =>
          if each.id == id then each.copy(skipIngest = skipIngest) else each
        }
      }
      .map(_ => 1)

  def preservicaClient(ref: Ref[IO, List[EntityWithIdentifiers]], apiError: Boolean): EntityClient[IO, Fs2Streams[IO]] =
    new EntityClient[IO, Fs2Streams[IO]] {

      override val dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME

      override def metadataForEntity(entity: Entity): IO[EntityClient.EntityMetadata] = notImplemented

      override def getBitstreamInfo(contentRef: UUID): IO[Seq[Client.BitStreamInfo]] = notImplemented

      override def getEntity(entityRef: UUID, entityType: EntityType): IO[Entity] = notImplemented

      override def getEntityIdentifiers(entity: Entity): IO[Seq[IdentifierResponse]] = notImplemented

      override def getUrlsToIoRepresentations(ioEntityRef: UUID, representationType: Option[EntityClient.RepresentationType]): IO[Seq[String]] = notImplemented

      override def getContentObjectsFromRepresentation(ioEntityRef: UUID, representationType: EntityClient.RepresentationType, repTypeIndex: Int): IO[Seq[Entity]] = notImplemented

      override def addEntity(aer: AddEntityRequest): IO[UUID] = notImplemented

      override def updateEntity(uer: UpdateEntityRequest): IO[String] = notImplemented

      override def updateEntityIdentifiers(entity: Entity, identifiers: Seq[IdentifierResponse]): IO[Seq[IdentifierResponse]] = notImplemented

      override def streamBitstreamContent[T](stream: capabilities.Streams[Fs2Streams[IO]])(url: String, streamFn: stream.BinaryStream => IO[T]): IO[T] = notImplemented

      override def entityEventActions(entity: Entity, startEntry: Int, maxEntries: Int): IO[Seq[DataProcessor.EventAction]] = notImplemented

      override def streamAllEntityRefs(repTypeFilter: Option[EntityClient.RepresentationType]): fs2.Stream[IO, Entities.EntityRef] = fs2.Stream.empty[IO]

      override def entitiesPerIdentifier(identifiers: Seq[PreservicaIdentifier]): IO[Map[PreservicaIdentifier, Seq[Entity]]] =
        IO.raiseWhen(apiError)(new Exception("API has encountered an error")) >>
          ref.get.map { existing =>
            existing
              .flatMap(_.identifiers)
              .collect {
                case value if identifiers.contains(value) => value -> existing.filter(_.identifiers.contains(value)).map(_.entity)
              }
              .toMap
          }

      override def addIdentifierForEntity(entityRef: UUID, entityType: EntityType, identifier: PreservicaIdentifier): IO[String] = notImplemented

      override def getPreservicaNamespaceVersion(endpoint: String): IO[Float] = notImplemented

      override def entitiesUpdatedSince(sinceDateTime: ZonedDateTime, startEntry: Int, maxEntries: Int, potentialEndDate: Option[ZonedDateTime]): IO[EntitiesUpdated] =
        notImplemented
    }

  val sqsEvent: SQSEvent = {
    val sqsEvent = new SQSEvent()
    val sqsMessage = new SQSMessage()
    sqsMessage.setBody(input.asJson.noSpaces)
    sqsEvent.setRecords(List(sqsMessage).asJava)
    sqsEvent
  }

  case class LambdaTestOutput(
      entities: List[EntityWithIdentifiers],
      dynamoItems: List[AssetDynamoItem],
      stateOutput: List[StateOutput],
      sfnSendFailureOutput: List[SfnSendFailureOutput]
  )

  def runLambda(
      items: List[AssetDynamoItem],
      entities: List[EntityWithIdentifiers],
      dynamoError: Boolean = false,
      apiError: Boolean = false,
      sfnSuccessError: Boolean = false,
      sfnFailureError: Boolean = false
  ): LambdaTestOutput = (for {
    itemsRef <- Ref.of[IO, List[AssetDynamoItem]](items)
    entitiesRef <- Ref.of[IO, List[EntityWithIdentifiers]](entities)
    sfnOutputRef <- Ref.of[IO, List[StateOutput]](Nil)
    sfnErrorsRef <- Ref.of[IO, List[SfnSendFailureOutput]](Nil)
    dynamo = dynamoClient(itemsRef, dynamoError)
    sfn = sfnClient(sfnOutputRef, sfnErrorsRef, sfnSuccessError, sfnFailureError)
    dependencies = Dependencies(preservicaClient(entitiesRef, apiError), dynamo, sfn)
    res <- Lambda().handler(sqsEvent, config, dependencies)
    entities <- entitiesRef.get
    items <- itemsRef.get
    sfnOutput <- sfnOutputRef.get
    sfnErrors <- sfnErrorsRef.get
  } yield LambdaTestOutput(entities, items, sfnOutput, sfnErrors)).unsafeRunSync()
}
