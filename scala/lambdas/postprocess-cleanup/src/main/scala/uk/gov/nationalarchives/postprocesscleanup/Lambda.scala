package uk.gov.nationalarchives.postprocesscleanup

import cats.effect.IO
import cats.syntax.traverse.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.circe.Decoder
import org.scanamo.syntax.*
import pureconfig.ConfigReader
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import uk.gov.nationalarchives.DADynamoDBClient.{DADynamoDbRequest, given}
import uk.gov.nationalarchives.postprocesscleanup.Lambda.*
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.utils.EventCodecs.given
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*

import scala.jdk.CollectionConverters.*
import io.circe.parser.decode
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class Lambda extends LambdaRunner[SQSEvent, Unit, Config, Dependencies] {
  private val deletionTagMap = Map("TO_BE_DELETED" -> "true")

  override def handler: (SQSEvent, Config, Dependencies) => IO[Unit] = { (sqsEvent, config, dependencies) =>
    val ttlAfter1Day = System.currentTimeMillis() / 1000 + 24 * 3600

    def updateTtl(itemId: String): IO[Unit] = 
      dependencies.dynamoClient
        .updateAttributeValues(
          DADynamoDbRequest(
            config.filesTableName,
            primaryKeyAndItsValue = Map(id -> AttributeValue.builder().s(itemId).build(), batchId -> AttributeValue.builder().s("batchId").build()),
            attributeNamesAndValuesToUpdate = Map(ttl -> AttributeValue.builder().n(ttlAfter1Day.toString).build())
          )
        )
        .void

    def updateAllAncestorsTtl(parentPath: String): IO[Unit] =
      if parentPath.isEmpty then IO.unit
      else parentPath.split('/').toList.reverse.traverse(ancestorId => updateTtl(ancestorId)).void
    
    sqsEvent.getRecords.asScala.toList.traverse { record =>
      for {
        sqsMessage <- IO.fromEither(decode[SqsMessage](record.getBody).left.map(err => new RuntimeException(s"Failed to decode SQS message body: ${err.getMessage}")))
        assetId = sqsMessage.body.params.assetId
        batchId = sqsMessage.body.properties.executionId
        assetItems <- dependencies.dynamoClient.queryItems[AssetDynamoItem](
          config.filesTableName,
          DynamoFormatters.id === assetId and DynamoFormatters.batchId === batchId
        )

        assetItem <- assetItems match {
          case Nil => IO.raiseError(new RuntimeException(s"No item found for assetId=$assetId"))
          case head :: Nil => IO.pure(head)
          case _ => IO.raiseError(new RuntimeException(s"More than one item found for assetId=$assetId"))
        }

        _ <- updateTtl(assetId)

        childrenParentPath = getParentPathForChildren(assetItem)
        fileItems <- dependencies.dynamoClient.queryItems[FileDynamoItem](
          config.filesTableName,
          DynamoFormatters.parentPath === childrenParentPath and DynamoFormatters.batchId === assetItem.batchId,
          Option(config.dynamoGsiName)
        )

        _ <- fileItems.traverse { fileItem =>
          dependencies.s3Client.updateObjectTags(config.rawCacheBucketName, fileItem.location.getPath.drop(1), deletionTagMap) >>
            updateTtl(fileItem.id.toString)
        }
        _ <- updateAllAncestorsTtl(assetItem.potentialParentPath.getOrElse(""))
      } yield ()
    }.void
  }

  def getParentPathForChildren(assetItem: DynamoItem): String = {
    s"${assetItem.potentialParentPath.map(path => s"$path/").getOrElse("")}${assetItem.id}"
  }

  override def dependencies(config: Config): IO[Dependencies] = IO(
    Dependencies(DADynamoDBClient[IO](), DAS3Client[IO]())
  )
  
}

object Lambda {

  given Decoder[SqsMessageParams] = (c: io.circe.HCursor) =>
    for {
      assetId <- c.downField("assetId").as[String]
      status <- c.downField("status").as[String]
    } yield SqsMessageParams(assetId, status)

  given Decoder[SqsMessageProps] = (c: io.circe.HCursor) =>
    for {
      executionId <- c.downField("executionId").as[String]
      messageType <- c.downField("messageType").as[String]
    } yield SqsMessageProps(executionId, messageType)

  given Decoder[SqsMessageBody] = (c: io.circe.HCursor) =>
    for {
      params <- c.downField("parameters").as[SqsMessageParams]
      properties <- c.downField("properties").as[SqsMessageProps]
    } yield SqsMessageBody(params, properties)

  given Decoder[SqsMessage] = (c: io.circe.HCursor) =>
    for {
      body <- c.downField("body").as[SqsMessageBody]
    } yield SqsMessage(body)
    
  case class Config(filesTableName: String, dynamoGsiName: String, rawCacheBucketName: String) derives ConfigReader
  case class Dependencies(dynamoClient: DADynamoDBClient[IO], s3Client: DAS3Client[IO])

  case class SqsMessageParams(assetId: String, status: String)
  case class SqsMessageProps(executionId: String, messageType: String)
  case class SqsMessageBody(params: SqsMessageParams, properties: SqsMessageProps)
  case class SqsMessage(body: SqsMessageBody)
}
