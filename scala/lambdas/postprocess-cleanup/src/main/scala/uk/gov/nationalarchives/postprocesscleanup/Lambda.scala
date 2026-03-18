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
  private val deletionTag: (String, String) = "TO_BE_DELETED" -> "true"

  override def handler: (SQSEvent, Config, Dependencies) => IO[Unit] = { (sqsEvent, config, dependencies) =>
    val ttlAfter1Day = System.currentTimeMillis() / 1000 + 24 * 3600

    def updateTtl(itemId: String): IO[Unit] = {
      dependencies.dynamoClient
        .updateAttributeValues(
          DADynamoDbRequest(
            config.filesTableName,
            primaryKeyAndItsValue = Map(id -> AttributeValue.builder().s(itemId).build()),
            attributeNamesAndValuesToUpdate = Map(ttl -> AttributeValue.builder().n(ttlAfter1Day.toString).build())
          )
        )
        .void
    }

    def updateAllAncestorsTtl(parentPath: String): IO[Unit] =
      if parentPath.isEmpty then IO.unit
      else parentPath.split('/').toList.reverse.traverse(ancestorId => updateTtl(ancestorId)).void

    for {
      record <- IO.fromOption(Option(sqsEvent.getRecords).flatMap(_.asScala.headOption))(new RuntimeException("No SQS records"))
      messageBody <- IO.fromEither(decode[SqsMessageBody](record.getBody).left.map(err => new RuntimeException(s"Failed to decode SQS message body: ${err.getMessage}")))

      assetId = messageBody.params.assetId

      items <- dependencies.dynamoClient.queryItems[FileDynamoItem](
        config.filesTableName,
        DynamoFormatters.id === assetId,
        Option(config.dynamoGsiName)
      )

      item <- IO.fromEither(
        items.size match {
          case 0 => Left(new RuntimeException(s"No item found for assetId=$assetId"))
          case 1 => Right(items.head)
          case _ => Left(new RuntimeException(s"More than one item found for assetId=$assetId"))
        }
      )

      _ <- updateTtl(item.id.toString)

      childrenParentPath = s"${item.potentialParentPath.map(path => s"$path/").getOrElse("")}${item.id}"
      children <- dependencies.dynamoClient.queryItems[FileDynamoItem](
        config.filesTableName,
        DynamoFormatters.parentPath === childrenParentPath and DynamoFormatters.batchId === item.batchId,
        Option(config.dynamoGsiName)
      )

      _ <- children.traverse { child =>
        dependencies.s3Client.updateObjectTags(config.rawCacheBucketName, child.location.toString, Map(deletionTag)) >>
          updateTtl(child.id.toString)
      }
      _ <- updateAllAncestorsTtl(item.potentialParentPath.getOrElse(""))
    } yield ()
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

  case class Config(filesTableName: String, dynamoGsiName: String, rawCacheBucketName: String) derives ConfigReader
  case class Dependencies(dynamoClient: DADynamoDBClient[IO], s3Client: DAS3Client[IO])

  case class SqsMessageParams(assetId: String, status: String)
  case class SqsMessageProps(executionId: String, messageType: String)
  case class SqsMessageBody(params: SqsMessageParams, properties: SqsMessageProps)

}
