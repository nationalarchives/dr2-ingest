package uk.gov.nationalarchives.postprocesscleanup

import cats.effect.IO
import cats.syntax.traverse.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
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

  override def handler: (SQSEvent, Config, Dependencies) => IO[Unit] = { (sqsEvent, config, dependencies) =>
    val ttlAfter1Day = System.currentTimeMillis() / 1000 + 24 * 3600

    def updateTtl(parentPath: String): IO[Unit] = {
      IO {
        dependencies.dynamoClient.updateAttributeValues(
          DADynamoDbRequest(
            config.filesTableName,
            primaryKeyAndItsValue = Map("id" -> AttributeValue.builder().s(id).build()),
            attributeNamesAndValuesToUpdate = Map("ttl" -> AttributeValue.builder().n(ttlAfter1Day.toString).build())
          )
        )
      }
    }

    def updateParentsTtl(parentPath: String): IO[Unit] =
      if (parentPath.isEmpty) then IO.unit
      else
        val ancestorIds = parentPath.split('/').toList
        ancestorIds.reverse.traverse(ancestorId => updateTtl(ancestorId)).void

    for {
      record <- IO.fromOption(Option(sqsEvent.getRecords).flatMap(_.asScala.headOption))(new RuntimeException("No SQS records"))
      messageBody <- IO.fromEither(decode[SqsMessageBody](record.getBody))
      assetId = messageBody.params.assetId

      items <- dependencies.dynamoClient.queryItems[FileDynamoItem](
        config.filesTableName,
        DynamoFormatters.id === assetId,
        Option(config.dynamoGsiName)
      )

      item <- IO.fromEither(
        items match {
          case Nil =>
            Left(new RuntimeException(s"No item found for assetId=$assetId"))
          case firstItem :: Nil =>
            Right(firstItem)
          case _ =>
            Left(new RuntimeException(s"More than one item found for assetId=$assetId"))
        }
      )
      childrenParentPath = s"${item.potentialParentPath.map(path => s"$path/").getOrElse("")}${item.id}"

      children <- dependencies.dynamoClient.queryItems[FileDynamoItem](
        config.filesTableName,
        DynamoFormatters.parentPath === childrenParentPath and DynamoFormatters.batchId === item.batchId,
        Option(config.dynamoGsiName)
      )

      _ <- updateTtl(item.id.toString)
      _ <- children.traverse { child =>
        dependencies.s3Client.updateObjectTags(config.rawCacheBucketName, child.location.toString, Map("TO_BE_DELETED" -> "true"))
        updateTtl(child.id.toString)
      }
      _ <- updateParentsTtl(item.potentialParentPath.getOrElse(""))
    } yield ()
  }

  override def dependencies(config: Config): IO[Dependencies] = IO(
    Dependencies(DADynamoDBClient[IO](), DAS3Client[IO]())
  )
}

object Lambda {
  case class Config(filesTableName: String, dynamoGsiName: String, rawCacheBucketName: String) derives ConfigReader
  case class Dependencies(dynamoClient: DADynamoDBClient[IO], s3Client: DAS3Client[IO])

  case class SqsMessageParams(assetId: String, status: String)
  case class SqsMessageProps(executionId: String, messageType: String)
  case class SqsMessageBody(params: SqsMessageParams, properties: SqsMessageProps)

  given Decoder[SqsMessageParams] = deriveDecoder
  given Decoder[SqsMessageProps] = deriveDecoder
  given Decoder[SqsMessageBody] = deriveDecoder

}
