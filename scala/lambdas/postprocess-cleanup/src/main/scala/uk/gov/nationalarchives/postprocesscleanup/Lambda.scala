package uk.gov.nationalarchives.postprocesscleanup

import cats.effect.IO
import cats.implicits.catsSyntaxEq
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import org.scanamo.query.*
import org.scanamo.request.RequestCondition
import pureconfig.ConfigReader
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}
import uk.gov.nationalarchives.postprocesscleanup.Lambda.*
import uk.gov.nationalarchives.utils.LambdaRunner
import uk.gov.nationalarchives.utils.EventCodecs.given
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.Type.*

import scala.jdk.CollectionConverters.*
import io.circe.parser.decode
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters



class Lambda extends LambdaRunner[SQSEvent, Unit, Config, Dependencies] {
  
  override def handler: (SQSEvent, Config, Dependencies) => IO[Unit] = { (sqsEvent, config, dependencies) =>
    for {
      record <- IO.fromOption(Option(sqsEvent.getRecords).flatMap(_.asScala.headOption)
      )(new RuntimeException("No SQS records"))
      messageBody <- IO.fromEither(decode[SqsMessageBody](record.getBody))
      assetId = messageBody.params.assetId

//      items <- dependencies.dynamoClient.queryItems[FileDynamoItem](
//        config.filesTableName,
//        RequestCondition.fromQuery("id" === assetId),
//        Option(config.dynamoGsiName)
//      )
      
      items <- dependencies.dynamoClient.queryItems[FileDynamoItem](
                                                   config.filesTableName,
                                                  (DynamoFormatters.id === assetId),
                                                   Option(config.dynamoGsiName)
                                                   )
    
      
      itemOpt <- dependencies.dynamoClient.getItems[FileDynamoItem](config.filesTableName, assetId)
      
    
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
  case class SqsMessageProps(executionId: String, messageType:String)
  case class SqsMessageBody(params: SqsMessageParams, properties: SqsMessageProps)
  
  given Decoder[SqsMessageParams] = deriveDecoder
  given Decoder[SqsMessageProps] = deriveDecoder
  given Decoder[SqsMessageBody] = deriveDecoder
    
}