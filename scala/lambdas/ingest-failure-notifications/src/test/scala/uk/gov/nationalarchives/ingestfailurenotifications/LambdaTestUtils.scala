package uk.gov.nationalarchives.ingestfailurenotifications

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import io.circe.Encoder
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.sns.model.PublishBatchResponse
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.IngestLockTableItem
import uk.gov.nationalarchives.ingestfailurenotifications.Lambda.{Config, Dependencies, SfnDetail, SfnEvent, SfnInput}
import uk.gov.nationalarchives.utils.ExternalUtils.OutputMessage

import java.time.Instant
import java.util.UUID

object LambdaTestUtils:

  val uuids: List[UUID] = List(UUID.randomUUID)

  val config: Config = Config("topicArn", "lockTableName", "lockTableGsiName")

  def createDependencies(
      dynamoItemsRef: Ref[IO, List[IngestLockTableItem]],
      snsMessagesRef: Ref[IO, List[OutputMessage]],
      dynamoError: Boolean,
      snsError: Boolean
  ): Dependencies = {
    val dynamoClient = new DADynamoDBClient[IO] {
      override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.pure(Nil)

      override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = IO.pure(1)

      override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.pure(Nil)

      override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
        if dynamoError then IO.raiseError(new Exception("Error getting dynamo items"))
        else
          dynamoItemsRef.get.map { rows =>
            (for {
              values <- Option(requestCondition.attributes.values)
              map <- values.toMap[String].toOption
            } yield rows
              .filter(row => map.get("conditionAttributeValue0").contains(row.groupId))
              .map(_.asInstanceOf[U])).getOrElse(Nil)
          }

      override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = IO.pure(Nil)

      override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = IO(1)
    }

    val snsClient: DASNSClient[IO] = new DASNSClient[IO]:
      override def publish[T <: Product](topicArn: String)(messages: List[T])(using enc: Encoder[T]): IO[List[PublishBatchResponse]] =
        if snsError then IO.raiseError(new Exception("Error sending to SNS"))
        else
          snsMessagesRef
            .update { existingMessages =>
              existingMessages ++ messages.map(_.asInstanceOf[OutputMessage])
            }
            .map(_ => List(PublishBatchResponse.builder.build))

    val uuidIterator: () => UUID = () => {
      val uuidsIterator: Iterator[UUID] = uuids.iterator
      uuidsIterator.next()
    }
    Dependencies(snsClient, dynamoClient, uuidIterator)
  }

  def generateItems(): List[IngestLockTableItem] = List.fill(100)(UUID.randomUUID).map { id =>
    IngestLockTableItem(id, "groupId", "", Some(Instant.now().toString))
  }

  def runLambda(lockTableItems: List[IngestLockTableItem], input: SfnInput, dynamoError: Boolean = false, snsError: Boolean = false): List[OutputMessage] = (for {
    dynamoItemsRef <- Ref.of[IO, List[IngestLockTableItem]](lockTableItems)
    snsMessagesRef <- Ref.of[IO, List[OutputMessage]](Nil)
    dependencies = createDependencies(dynamoItemsRef, snsMessagesRef, dynamoError, snsError)
    _ <- new Lambda().handler(SfnEvent(SfnDetail(input)), config, dependencies)
    snsMessages <- snsMessagesRef.get
  } yield snsMessages).unsafeRunSync()
