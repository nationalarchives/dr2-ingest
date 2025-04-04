package uk.gov.nationalarchives.ingestfileschangehandler

import cats.effect.{IO, Ref}
import io.circe.Encoder
import org.scanamo.{DynamoFormat, DynamoReadError, DynamoValue, MissingProperty}
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.sns.model.PublishBatchResponse
import uk.gov.nationalarchives.{DADynamoDBClient, DASNSClient}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.FileRepresentationType.PreservationRepresentationType
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.{Asset, File}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{
  AssetDynamoItem,
  FileDynamoItem,
  FilesTablePartitionKey,
  FilesTablePrimaryKey,
  FilesTableSortKey,
  Type,
  digitalAssetSource,
  name,
  transferringBody,
  upstreamSystem,
  Checksum
}
import uk.gov.nationalarchives.utils.ExternalUtils.OutputMessage

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID

object Utils {

  extension (row: DynamoRow)
    def getPrimaryKey: FilesTablePrimaryKey =
      FilesTablePrimaryKey(FilesTablePartitionKey(row.id), FilesTableSortKey(row.batchId))

    def createAsset(): AssetDynamoItem = AssetDynamoItem(
      row.batchId,
      row.id,
      row.parentPath,
      Asset,
      None,
      None,
      transferringBody,
      OffsetDateTime.parse("2023-06-01T00:00Z"),
      upstreamSystem,
      digitalAssetSource,
      None,
      Nil,
      Nil,
      row.ingestedPreservica,
      row.ingestedCustodialCopy,
      Nil,
      2,
      row.skipIngest,
      Option("correlationId")
    )

    def createFile(): FileDynamoItem = FileDynamoItem(
      row.batchId,
      row.id,
      row.parentPath,
      name,
      File,
      None,
      None,
      1,
      2,
      List(Checksum("checksum_SHA512", "checksum")),
      Option("ext"),
      PreservationRepresentationType,
      1,
      row.ingestedPreservica,
      row.ingestedCustodialCopy,
      Nil,
      1,
      URI.create("s3://bucket/key")
    )

  case class DynamoRow(
      id: UUID,
      batchId: String,
      rowType: Type,
      parentPath: Option[String],
      ingestedPreservica: Boolean = false,
      ingestedCustodialCopy: Boolean = false,
      skipIngest: Boolean = false
  )

  def createSnsClient(ref: Ref[IO, List[OutputMessage]]): DASNSClient[IO] = new DASNSClient[IO]() {
    override def publish[T <: Product](topicArn: String)(messages: List[T])(using enc: Encoder[T]): IO[List[PublishBatchResponse]] = ref
      .update { messageList =>
        messageList ++ messages.map(_.asInstanceOf[OutputMessage])
      }
      .map(_ => Nil)
  }

  def createDynamoClient(ref: Ref[IO, List[DynamoRow]]): DADynamoDBClient[IO] = {
    new DADynamoDBClient[IO]():

      given DynamoFormat[String] = new DynamoFormat[String]:
        override def read(av: DynamoValue): Either[DynamoReadError, String] = av.asString.toRight(MissingProperty)

        override def write(t: String): DynamoValue = DynamoValue.fromString(t)

      override def deleteItems[T](tableName: String, primaryKeys: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = {
        ref
          .update { r =>
            r.filterNot { row =>
              primaryKeys.contains(getPrimaryKey(row))
            }
          }
          .map(_ => Nil)
      }

      override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = IO.pure(1)

      override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.pure(Nil)

      override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = ref.get.map { rows =>
        rows
          .filter(row => primaryKeys.contains(getPrimaryKey(row)))
          .flatMap { row =>
            row.rowType match
              case Type.Asset => Option(row.createAsset().asInstanceOf[T])
              case Type.File  => Option(row.createFile().asInstanceOf[T])
              case _          => None
          }
      }

      override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = IO.pure(1)

      override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String] = None)(using
          returnTypeFormat: DynamoFormat[U]
      ): IO[List[U]] = ref.get.map { rows =>
        (for {
          values <- Option(requestCondition.attributes.values)
          map <- values.toMap[String].toOption
        } yield
          if potentialGsiName.isEmpty then
            rows
              .filter(row => map.get("conditionAttributeValue0").contains(row.id.toString))
              .map(_.createAsset().asInstanceOf[U])
          else
            rows
              .filter(row => row.parentPath == map.get("parentPath") && map.get("batchId").contains(row.batchId))
              .map(_.createFile().asInstanceOf[U])
        ).getOrElse(Nil)
      }
  }
}
