package uk.gov.nationalarchives.ingestflowcontrol

import cats.effect.*
import cats.effect.unsafe.implicits.global
import io.circe.{Decoder, Encoder}
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse
import uk.gov.nationalarchives.ingestflowcontrol.Lambda.*
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient, DASSMClient}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*

import java.time.Instant

object Helpers {
  case class StepFunctionExecution(name: String, taskToken: String, taskTokenSuccess: Boolean = false)

  val config: Config = Config("", "", "")

  def notImplemented[T]: IO[Nothing] = IO.raiseError(new Exception("Not implemented"))

  def predictableRandomNumberSelector(selected: Int = 10): (Int, Int) => Int = (min, max) => if selected > max then max else selected

  def runLambda(
      input: Input,
      initialItemsInTable: List[IngestQueueTableItem],
      flowControlConfig: FlowControlConfig,
      initialExecutions: List[StepFunctionExecution],
      randomSelection: (Int, Int) => Int,
      errors: Option[Errors] = None
  ): LambdaRunResults = {
    for {
      dynamoRef <- Ref.of[IO, List[IngestQueueTableItem]](initialItemsInTable)
      ssmRef <- Ref.of[IO, FlowControlConfig](flowControlConfig)
      sfnRef <- Ref.of[IO, List[StepFunctionExecution]](initialExecutions)
      dependencies = Dependencies(dynamoClient(dynamoRef, errors), sfnClient(sfnRef, errors), ssmClient(ssmRef, errors), randomSelection)
      result <- new Lambda().handler(input, config, dependencies).attempt
      dynamoResult <- dynamoRef.get
      ssmResult <- ssmRef.get
      sfnResult <- sfnRef.get
    } yield LambdaRunResults(result, dynamoResult, ssmResult, sfnResult)
  }.unsafeRunSync()

  case class LambdaRunResults(
      result: Either[Throwable, Unit],
      finalItemsInTable: List[IngestQueueTableItem],
      flowConfig: FlowControlConfig,
      finalStepFnExecutions: List[StepFunctionExecution]
  )

  case class Errors(
      getParameter: Boolean = false,
      writeItem: Boolean = false,
      getItem: Boolean = false,
      deleteItems: Boolean = false,
      listStepFunctions: Boolean = false,
      sendTaskSuccess: Boolean = false
  )

  def ssmClient(ref: Ref[IO, FlowControlConfig], errors: Option[Errors]): DASSMClient[IO] = new DASSMClient[IO]:
    override def getParameter[T](parameterName: String, withDecryption: Boolean)(using Decoder[T]): IO[T] =
      errors.raise(_.getParameter, "Error getting parameter") >>
        ref.get.map(_.asInstanceOf[T])

  def dynamoClient(ref: Ref[IO, List[IngestQueueTableItem]], errors: Option[Errors]): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] =
      errors.raise(_.deleteItems, "Error deleting item from dynamo table") >>
        ref
          .update { r =>
            r.filterNot { row =>
              primaryKeyAttributes.contains(IngestQueuePrimaryKey(IngestQueuePartitionKey(row.sourceSystem), IngestQueueSortKey(row.queuedAt)))
            }
          }
          .map(_ => Nil)

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] =
      errors.raise(_.writeItem, "Error writing item to dynamo table") >>
        ref
          .update { existing =>
            IngestQueueTableItem(
              dynamoDbWriteRequest.attributeNamesAndValuesToWrite(sourceSystem).s(),
              Instant.parse(dynamoDbWriteRequest.attributeNamesAndValuesToWrite(queuedAt).s()),
              dynamoDbWriteRequest.attributeNamesAndValuesToWrite(taskToken).s()
            ) :: existing
          }
          .map(_ => 1)

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      notImplemented

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] =
      val firstPK = primaryKeys.head.asInstanceOf[IngestQueuePartitionKey]
      errors.raise(_.getItem, "Error getting item from dynamo table") >>
        ref.get.map { existing =>
          existing.filter(_.sourceSystem == firstPK.sourceSystem).sortBy(_.queuedAt).map(_.asInstanceOf[T])
        }

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = notImplemented

  def sfnClient(ref: Ref[IO, List[StepFunctionExecution]], errors: Option[Errors]): DASFNClient[IO] = new DASFNClient[IO]:
    override def startExecution[T <: Product](stateMachineArn: String, input: T, name: Option[String])(using enc: Encoder[T]): IO[StartExecutionResponse] = notImplemented

    override def listStepFunctions(stepFunctionArn: String, status: DASFNClient.Status): IO[List[String]] =
      errors.raise(_.listStepFunctions, "Error generating a list of step functions") >>
        ref.get.map { existing =>
          existing.map(_.name)
        }

    override def sendTaskSuccess(token: String): IO[Unit] = {
      errors.raise(_.sendTaskSuccess, "Error sending task success to step function") >>
        ref
          .update { existing =>
            val updatedExecution = existing.filter(_.taskToken == token).map(_.copy(taskTokenSuccess = true))
            existing.filter(_.taskToken != token) ++ updatedExecution
          }
    }

  extension (errors: Option[Errors]) def raise(fn: Errors => Boolean, errorMessage: String): IO[Unit] = IO.raiseWhen(errors.exists(fn))(new Exception(errorMessage))

}
