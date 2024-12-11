package uk.gov.nationalarchives.ingestflowcontrol

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import cats.effect.*
import cats.effect.unsafe.implicits.global
import io.circe.{Decoder, Encoder}
import org.scalatest.EitherValues
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse
import uk.gov.nationalarchives.{DADynamoDBClient, DASFNClient, DASSMClient}
import uk.gov.nationalarchives.ingestflowcontrol.Lambda.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.ingestflowcontrol.helpers.StepFunctionExecution

import java.time.Instant

class LambdaTest extends AnyFlatSpec with EitherValues:

  val config: Config = Config("", "", "")
  
  def notImplemented[T]: IO[Nothing] = IO.raiseError(new Exception("Not implemented"))

  private def runLambda(input: Input, rowsInTable: List[IngestQueueTableItem], initialConfig: FlowControlConfig, initialExecutions: List[StepFunctionExecution], errors: Option[Errors] = None): (Either[Throwable, Unit], List[IngestQueueTableItem], FlowControlConfig, List[StepFunctionExecution]) = {
    for {
      dynamoRef <- Ref.of[IO, List[IngestQueueTableItem]](rowsInTable)
      ssmRef <- Ref.of[IO, FlowControlConfig](initialConfig)
      sfnRef <- Ref.of[IO, List[StepFunctionExecution]](initialExecutions)
      dependencies = Dependencies(dynamoClient(dynamoRef), sfnClient(sfnRef), ssmClient(ssmRef, errors))
      result <- new Lambda().handler(input, config, dependencies).attempt
      dynamoResult <- dynamoRef.get
      ssmResult <- ssmRef.get
      sfnResult <- sfnRef.get
    } yield (result, dynamoResult, ssmResult, sfnResult)
  }.unsafeRunSync()

  class LambdaRunResults(val tuple: (Either[Throwable, Unit], List[IngestQueueTableItem], FlowControlConfig, List[StepFunctionExecution])) {
    def result = tuple._1
    def tableItems = tuple._2
    def flowConfig = tuple._3
    def stepFnExecutions = tuple._4
  }
  
  case class Errors(getParameter: Boolean = false, writeItem: Boolean = false, getItem: Boolean = false)

  def ssmClient(ref: Ref[IO, FlowControlConfig], errors: Option[Errors]): DASSMClient[IO] = new DASSMClient[IO]:
    override def getParameter[T](parameterName: String, withDecryption: Boolean)(using Decoder[T]): IO[T] =
      IO.whenA(errors.exists(_.getParameter))(IO.raiseError(new Exception("Error getting parameter"))) >> ref.get.map(_.asInstanceOf[T])

  def dynamoClient(ref: Ref[IO, List[IngestQueueTableItem]]): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = {
      ref
        .update { r =>
          r.filterNot { row =>
            primaryKeyAttributes.contains(IngestQueuePrimaryKey(IngestQueuePartitionKey(row.sourceSystem), IngestQueueSortKey(row.queuedAt)))
          }
        }
        .map(_ => Nil)
    }

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = ref.update { existing =>
      IngestQueueTableItem(
        dynamoDbWriteRequest.attributeNamesAndValuesToWrite(sourceSystem).s(),
        Instant.parse(dynamoDbWriteRequest.attributeNamesAndValuesToWrite(queuedAt).s()),
        dynamoDbWriteRequest.attributeNamesAndValuesToWrite(taskToken).s()) :: existing
    }.map(_ => 1)


    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = notImplemented
    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] = notImplemented

    //FIXME: This is the implementation I changed recently to only returns items for head sourceSystem. This is okay because we are getting items for only one 
    // system at a time - however, has this changed introduced a problem in one of the other ones???
    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] =
      val firstPK = primaryKeys.head.asInstanceOf[IngestQueuePartitionKey]
      ref.get.map { existing =>
      existing.filter(_.sourceSystem == firstPK.sourceSystem).sortBy(_.queuedAt).map(_.asInstanceOf[T])
    }

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = notImplemented


  def sfnClient(ref: Ref[IO, List[StepFunctionExecution]]): DASFNClient[IO] = new DASFNClient[IO]:
    override def startExecution[T <: Product](stateMachineArn: String, input: T, name: Option[String])(using enc: Encoder[T]): IO[StartExecutionResponse] = notImplemented

    override def listStepFunctions(stepFunctionArn: String, status: DASFNClient.Status): IO[List[String]] = ref.get.map { existing =>
      existing.map(_.name)
    }

    //FIXME: This gets called but the line where we are updating the taskTokenSuccess, does not get called
    // its almost like "existing" is empty?? This was working before. 
    override def sendTaskSuccess(token: String): IO[Unit] = {
      ref
        .update {
          existing =>
          val updatedExecution = existing.filter(_.taskToken == token).map(_.copy(taskTokenSuccess = true))
          existing.filter(_.taskToken != token) ++ updatedExecution
      }
    }


  //  "lambda" should "do stuff" in {
//    val initialDynamo = List(IngestQueueTableItem("TDR", Instant.now, "taskToken"))
//    val ssmParam = FlowControlConfig(1, List(SourceSystem("default", 1, 100)))
//    val sfnThing = List(StepFunctionExecution("", "taskToken"))
//    val input = Input("SomeExecutionName", "differentTaskToken")
//
//    val lambdaResult  = LambdaRunResults(runLambda(input, initialDynamo, ssmParam, sfnThing, Option(Errors(true))))
//    lambdaResult.result.isLeft should be(true)
//  }
//
//  "lambda" should "not affect the states when no task token is provided in the input" in {
//    val initialItem = IngestQueueTableItem("TDR", Instant.now, "taskToken")
//    val initialDynamo = List(initialItem)
//    val validSourceSystems = List(SourceSystem("SystemOne", 2, 25), SourceSystem("SystemTwo", 3, 65), SourceSystem("SystemThree", 1), SourceSystem("default", 0, 10))
//    val ssmParam = FlowControlConfig(6, validSourceSystems)
//    val sfnThing = List(StepFunctionExecution("someName", "taskToken"))
//    val input = Input("SomeExecutionName", "")
//
//    val lambdaRunResult = LambdaRunResults(runLambda(input, initialDynamo, ssmParam, sfnThing))
//    lambdaRunResult.result.isRight should be(true)
//    lambdaRunResult.result.getOrElse(NotImplementedError()) should be(())
//    lambdaRunResult.tableItems.size should be(1)
//    lambdaRunResult.tableItems.head should be(initialItem)
//    lambdaRunResult.stepFnExecutions.size should be(1)
//  }
//
  "lambda" should "add a new task to dynamo table and turn the task success to true when it is processed" in {
    val initialDynamo = List.empty
    val validSourceSystems = List(SourceSystem("TDR", 2, 25), SourceSystem("FCL", 3, 65), SourceSystem("SystemThree", 1), SourceSystem("default", 0, 10))
    val initialconfig = FlowControlConfig(6, validSourceSystems)
    val existingExecutions = List(StepFunctionExecution("FCL_execution_name", "a-task-token-for-fcl-task", false), StepFunctionExecution("TDR_execution_name", "a-task-token-for-tdr-task", false))
    val input = Input("TDR_execution_name", "a-task-token-for-tdr-task")

    val lambdaRunResult = LambdaRunResults(runLambda(input, initialDynamo, initialconfig, existingExecutions))
    lambdaRunResult.result.isRight should be(true)
    lambdaRunResult.stepFnExecutions.size should be(2)
    lambdaRunResult.stepFnExecutions.find(_.taskToken == "a-task-token-for-fcl-task").get.taskTokenSuccess should be(false)
    lambdaRunResult.stepFnExecutions.find(_.taskToken == "a-task-token-for-tdr-task").get.taskTokenSuccess should be(true)

    lambdaRunResult.tableItems.size should be(0)
  }
//
//  "lambda" should "add new task to dynamo but not send success when dedicated channel is not available for the system" in {
//    val initialDynamo = List(IngestQueueTableItem("TDR", Instant.now, "a-task-already-running"))
//    val validSourceSystems = List(SourceSystem("TDR", 1, 25), SourceSystem("SystemTwo", 3, 65), SourceSystem("SystemThree", 1), SourceSystem("default", 0, 10))
//    val initialconfig = FlowControlConfig(6, validSourceSystems)
//    val existingExecutions = List(StepFunctionExecution("TDR_execution_name_1", "a-task-already-running", false))
//    val input = Input("TDR_execution_name_2", "a-task-token-for-new-tdr-task")
//
//    val lambdaRunResult = LambdaRunResults(runLambda(input, initialDynamo, initialconfig, existingExecutions))
//    lambdaRunResult.result.isRight should be(true)
//    lambdaRunResult.stepFnExecutions.size should be(1)
//    lambdaRunResult.stepFnExecutions.head.taskTokenSuccess should be(false)
//
//    lambdaRunResult.tableItems.size should be(2)
//    lambdaRunResult.tableItems.map(_.taskToken).contains("a-task-token-for-new-tdr-task") should be(true)
//  }

//  "lambda" should "send success on dedicated channels of all systems that are available" in {
//    val initialDynamo = List(IngestQueueTableItem("TDR", Instant.now, "tdr-task-1"), IngestQueueTableItem("FCL", Instant.now, "fcl-task-1"), IngestQueueTableItem("ABC", Instant.now, "abc-task-1"))
//    val validSourceSystems = List(SourceSystem("TDR", 2, 25), SourceSystem("FCL", 2, 65), SourceSystem("ABC", 2), SourceSystem("default", 0, 10))
//    val initialconfig = FlowControlConfig(7, validSourceSystems)
//    val existingExecutions = List(StepFunctionExecution("TDR_execution_name_1", "tdr-task-1", false), StepFunctionExecution("FCL_execution_name_1", "fcl-task-1", false), StepFunctionExecution("ABC_execution_name_1", "abc-task-1", false))
//    val input = Input("TDR_execution_name_2", "a-task-token-for-new-tdr-task")
//
//    val lambdaRunResult = LambdaRunResults(runLambda(input, initialDynamo, initialconfig, existingExecutions))
//    lambdaRunResult.result.isRight should be(true)
//    lambdaRunResult.stepFnExecutions.size should be(3)
//    lambdaRunResult.stepFnExecutions.head.taskTokenSuccess should be(false)
//
//    lambdaRunResult.tableItems.size should be(2)
//    lambdaRunResult.tableItems.map(_.taskToken).contains("a-task-token-for-new-tdr-task") should be(true)
//  }

//  "lambda" should "send task to default channel when the task is for a system not explicitly configured in the configuration" in {
//  }

//  "lambda" should "send success on a task which is already in dynamo table when the input is empty" in {
//       NEEDS TO CHANGE THE IMPLEMENTATION FOR THIS. In fact the test marked as  
//       "lambda" should "not affect the states when no task token is provided in the input"
//       is probably wrong behaviour, and should go away ??  
//  }


  // some case class validations
  "SourceSystem" should "error when the name is empty" in {
    intercept[IllegalArgumentException] {
      SourceSystem("")
    }.getMessage should be("requirement failed: System name should not be empty")
  }

  "SourceSystem" should "error when the dedicated channels count is negative" in {
    intercept[IllegalArgumentException] {
      SourceSystem("something", -1)
    }.getMessage should be("requirement failed: Dedicated channels should not be fewer than zero")
  }

  "SourceSystem" should "error when the probability is not between 0 and 100" in {
    intercept[IllegalArgumentException] {
      SourceSystem("something", 5, -23)
    }.getMessage should be("requirement failed: Probability must be between 0 and 100")

    intercept[IllegalArgumentException] {
      SourceSystem("something", 5, 123)
    }.getMessage should be("requirement failed: Probability must be between 0 and 100")
  }

  "FlowControlConfig" should "error when the source systems list is empty" in {
    intercept[IllegalArgumentException] {
      FlowControlConfig(5, List.empty)
    }.getMessage should be("requirement failed: Source systems list cannot be empty")
  }

  "FlowControlConfig" should "error when the sum of probabilities in the flow control is not 100" in {
    intercept[IllegalArgumentException] {
      FlowControlConfig(6,
        List(SourceSystem("SystemOne", 2, 25), SourceSystem("SystemTwo", 3, 65), SourceSystem("SystemThree", 1), SourceSystem("default", 0, 5)
        )
      )
    }.getMessage should be("requirement failed: The probability of all systems together should equate to 100%")
  }

  "FlowControlConfig" should "error when the dedicated channels exceed maximum concurrency" in {
    intercept[IllegalArgumentException] {
      Lambda.FlowControlConfig(
        4,
        List(
          Lambda.SourceSystem("SystemOne", 1, 25),
          Lambda.SourceSystem("SystemTwo", 2, 65),
          Lambda.SourceSystem("SystemThree", 1, 10),
          Lambda.SourceSystem("default", 2)
        )
      )
    }.getMessage should be("requirement failed: Total of dedicated channels exceed maximum concurrency")
  }

  "FlowControlConfig" should "error when there is a duplicate system name in the config" in {
    intercept[IllegalArgumentException] {
      Lambda.FlowControlConfig(
        4,
        List(
          Lambda.SourceSystem("SystemOne", 1, 25),
          Lambda.SourceSystem("SystemTwo", 0, 65),
          Lambda.SourceSystem("SystemTwo", 1, 10),
          Lambda.SourceSystem("default", 2)
        )
      )
    }.getMessage should be("requirement failed: System name must be unique")
  }

  "FlowControlConfig" should "error when there is no `default` system in the" in {
    intercept[IllegalArgumentException] {
      Lambda.FlowControlConfig(
        4,
        List(
          Lambda.SourceSystem("SystemOne", 1, 25),
          Lambda.SourceSystem("SystemTwo", 0, 65),
          Lambda.SourceSystem("SystemThree", 1, 10),
          Lambda.SourceSystem("SystemFour", 2)
        )
      )
    }.getMessage should be("requirement failed: Missing 'default' system in the configuration")
  }

  "FlowControlConfig" should "give availability of spare channels when at least one non-dedicated channel is available" in {
    val configWithSpareChannels = Lambda.FlowControlConfig(4,
      List(Lambda.SourceSystem("SystemOne", 1, 25), Lambda.SourceSystem("SystemTwo", 0, 35), Lambda.SourceSystem("SystemThree", 0, 10), Lambda.SourceSystem("default", 2, 30))
    )
    configWithSpareChannels.hasSpareChannels should be(true)
  }

  "FlowControlConfig" should "return false when dedicated channels equal the maximum concurrency" in {
    val configWithAllChannelsDedicated = Lambda.FlowControlConfig(4,
      List(Lambda.SourceSystem("SystemOne", 1, 25), Lambda.SourceSystem("SystemTwo", 1, 35), Lambda.SourceSystem("default", 2, 40))
    )
    configWithAllChannelsDedicated.hasSpareChannels should be(false)
  }

  "FlowControlConfig" should "give availability of dedicated channels when at least one dedicated channel is available" in {
    val configWithSpareChannels = Lambda.FlowControlConfig(4,
      List(Lambda.SourceSystem("SystemOne", 0, 25), Lambda.SourceSystem("SystemTwo", 0, 35), Lambda.SourceSystem("SystemThree", 0, 10), Lambda.SourceSystem("default", 1, 30))
    )
    configWithSpareChannels.hasDedicatedChannels should be(true)
  }

  "FlowControlConfig" should "return false when there are no dedicated channels for any system" in {
    val configWithAllChannelsDedicated = Lambda.FlowControlConfig(4,
      List(Lambda.SourceSystem("SystemOne", 0, 25), Lambda.SourceSystem("SystemTwo", 0, 35), Lambda.SourceSystem("default", 0, 40))
    )
    configWithAllChannelsDedicated.hasDedicatedChannels should be(false)
  }

  "buildProbabilityRangesMap" should "build a map of system name to ranges for all systems" in {
    val probabilitiesMap = new Lambda().buildProbabilityRangesMap(
      List(Lambda.SourceSystem("SystemOne", 1, 25), Lambda.SourceSystem("SystemTwo", 0, 65), Lambda.SourceSystem("default", 1, 10)),
      List.empty,
      1,
      Map.empty[String, (Int, Int)]
    )
    probabilitiesMap.size should be(3)
    probabilitiesMap("SystemOne")._1 should be(1)
    probabilitiesMap("SystemOne")._2 should be(26)
    probabilitiesMap("SystemTwo")._1 should be(26)
    probabilitiesMap("SystemTwo")._2 should be(91)
    probabilitiesMap("default")._1 should be(91)
    probabilitiesMap("default")._2 should be(101)
  }

  "buildProbabilityRangesMap" should "skip over any system mentioned in the 'skip list' when generating the map" in {
    val sourceSystems = List(
      Lambda.SourceSystem("SystemOne", 1, 25),
      Lambda.SourceSystem("SystemTwo", 0, 65),
      Lambda.SourceSystem("SystemThree", 1, 10),
      Lambda.SourceSystem("default", 2)
    )

    val probabilitiesMap = new Lambda().buildProbabilityRangesMap(sourceSystems, List("SystemTwo", "SystemThree"), 1, Map.empty[String, (Int, Int)])
    probabilitiesMap.size should be(2)
    probabilitiesMap("SystemOne")._1 should be(1)
    probabilitiesMap("SystemOne")._2 should be(26)
    probabilitiesMap("default")._1 should be(26)
    probabilitiesMap("default")._2 should be(26)
  }
