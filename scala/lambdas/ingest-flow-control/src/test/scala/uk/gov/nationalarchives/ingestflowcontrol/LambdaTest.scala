package uk.gov.nationalarchives.ingestflowcontrol

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.EitherValues
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}
import uk.gov.nationalarchives.ingestflowcontrol.Lambda.*
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.*
import uk.gov.nationalarchives.ingestflowcontrol.Helpers.*

import java.time.{Duration, Instant}

class LambdaTest extends AnyFlatSpec with EitherValues with TableDrivenPropertyChecks:

  val enabledTable: TableFor1[Boolean] = Table(
    "enabled",
    true,
    false
  )

  "lambda" should "report error when SSM client fails to get parameter" in {
    val initialDynamo = List(IngestQueueTableItem("TDR", Instant.now.toString + "_TDR_6b6db6bf_0", "taskToken", "TDR_6b6db6bf_0"))
    val ssmParam = FlowControlConfig(1, List(SourceSystem("DEFAULT", 1, 100)), true)
    val sfnExecutions = List(StepFunctionExecution("", "taskToken"))
    val input = Option(Input("SomeExecutionName", "differentTaskToken"))

    val lambdaRunResult = runLambda(input, initialDynamo, ssmParam, sfnExecutions, predictableRandomNumberSelector(), Option(Errors(getParameter = true)))
    lambdaRunResult.result.isLeft should be(true)
    lambdaRunResult.result.left.value.getMessage should equal("Error getting parameter")
    lambdaRunResult.finalItemsInTable should have length 1
    lambdaRunResult.finalItemsInTable.head.taskToken should be("taskToken")
    lambdaRunResult.finalStepFnExecutions should have length 1
    lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "taskToken") should be(defined)
    lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "taskToken").exists(_.taskTokenSuccess) should be(false)
  }

  "lambda" should "report error when dynamo client is unable to delete an item" in {
    val initialDynamo = List.empty
    val validSourceSystems = List(SourceSystem("TDR", 2, 25), SourceSystem("FCL", 3, 65), SourceSystem("SystemThree", 1), SourceSystem("DEFAULT", 0, 10))
    val initialConfig = FlowControlConfig(6, validSourceSystems, true)
    val existingExecutions = List(StepFunctionExecution("FCL_execution_name", "a-task-token-for-fcl-task"))
    val input = Option(Input("TDR_execution_name", "a-task-token-for-tdr-task"))

    val lambdaRunResult = runLambda(input, initialDynamo, initialConfig, existingExecutions, predictableRandomNumberSelector(), Option(Errors(deleteItems = true)))
    lambdaRunResult.result.isLeft should be(true)
    lambdaRunResult.result.left.value.getMessage should equal("Error deleting item from dynamo table")
    lambdaRunResult.finalItemsInTable should have length 1
    lambdaRunResult.finalItemsInTable.head.taskToken should be("a-task-token-for-tdr-task")
    lambdaRunResult.finalStepFnExecutions should have length 1
    lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-token-for-fcl-task") should be(defined)
    lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-token-for-fcl-task").exists(_.taskTokenSuccess) should be(false)
  }

  "lambda" should "report error when dynamo client is unable to write an item" in {
    val initialDynamo = List.empty
    val validSourceSystems = List(SourceSystem("TDR", 2, 25), SourceSystem("FCL", 3, 65), SourceSystem("SystemThree", 1), SourceSystem("DEFAULT", 0, 10))
    val initialConfig = FlowControlConfig(6, validSourceSystems, true)
    val existingExecutions = List(StepFunctionExecution("FCL_execution_name", "a-task-token-for-fcl-task"))
    val input = Option(Input("TDR_execution_name", "a-task-token-for-tdr-task"))

    val lambdaRunResult = runLambda(input, initialDynamo, initialConfig, existingExecutions, predictableRandomNumberSelector(), Option(Errors(writeItem = true)))
    lambdaRunResult.result.isLeft should be(true)
    lambdaRunResult.result.left.value.getMessage should equal("Error writing item to dynamo table")
    lambdaRunResult.finalItemsInTable should have length 0
    lambdaRunResult.finalStepFnExecutions should have length 1
    lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-token-for-fcl-task") should be(defined)
    lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-token-for-fcl-task").exists(_.taskTokenSuccess) should be(false)
  }

  "lambda" should "report error when dynamo client is unable to get an item" in {
    val initialDynamo = List.empty
    val validSourceSystems = List(SourceSystem("TDR", 2, 25), SourceSystem("FCL", 3, 65), SourceSystem("SystemThree", 1), SourceSystem("DEFAULT", 0, 10))
    val initialConfig = FlowControlConfig(6, validSourceSystems, true)
    val existingExecutions = List(StepFunctionExecution("FCL_execution_name", "a-task-token-for-fcl-task"))
    val input = Option(Input("TDR_execution_name", "a-task-token-for-tdr-task"))

    val lambdaRunResult = runLambda(input, initialDynamo, initialConfig, existingExecutions, predictableRandomNumberSelector(), Option(Errors(queryItem = true)))
    lambdaRunResult.result.isLeft should be(true)
    lambdaRunResult.result.left.value.getMessage should equal("Error querying item from dynamo table")
    lambdaRunResult.finalItemsInTable should have length 1
    lambdaRunResult.finalItemsInTable.head.taskToken should be("a-task-token-for-tdr-task")
    lambdaRunResult.finalStepFnExecutions should have length 1
    lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-token-for-fcl-task") should be(defined)
    lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-token-for-fcl-task").exists(_.taskTokenSuccess) should be(false)
  }

  "lambda" should "report error when SFN client cannot list step functions " in {
    val initialDynamo = List(IngestQueueTableItem("TDR", Instant.now.toString + "_TDR_2ec6248e_0", "taskToken", "TDR_2ec6248e_0"))
    val ssmParam = FlowControlConfig(1, List(SourceSystem("DEFAULT", 1, 100)), true)
    val sfnExecutions = List(StepFunctionExecution("", "taskToken"))
    val input = Option(Input("SomeExecutionName", "differentTaskToken"))

    val lambdaRunResult = runLambda(input, initialDynamo, ssmParam, sfnExecutions, predictableRandomNumberSelector(), Option(Errors(listStepFunctions = true)))
    lambdaRunResult.result.isLeft should be(true)
    lambdaRunResult.result.left.value.getMessage should equal("Error generating a list of step functions")
    lambdaRunResult.finalItemsInTable should have length 2
    lambdaRunResult.finalItemsInTable.map(_.taskToken) should contain allElementsOf List("differentTaskToken", "taskToken")
    lambdaRunResult.finalStepFnExecutions should have length 1
    lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-token-for-fcl-task") should be(None)
  }

  "lambda" should "report error when SFN client cannot send success on task" in {
    val initialDynamo = List.empty
    val validSourceSystems = List(SourceSystem("TDR", 2, 25), SourceSystem("FCL", 3, 65), SourceSystem("SystemThree", 1), SourceSystem("DEFAULT", 0, 10))
    val initialConfig = FlowControlConfig(6, validSourceSystems, true)
    val existingExecutions = List(StepFunctionExecution("FCL_execution_name", "a-task-token-for-fcl-task"))
    val input = Option(Input("TDR_execution_name", "a-task-token-for-tdr-task"))

    val lambdaRunResult = runLambda(input, initialDynamo, initialConfig, existingExecutions, predictableRandomNumberSelector(), Option(Errors(sendTaskSuccess = true)))
    lambdaRunResult.result.isLeft should be(true)
    lambdaRunResult.result.left.value.getMessage should equal("Error sending task success to step function")
    lambdaRunResult.finalItemsInTable should have length 1
    lambdaRunResult.finalItemsInTable.head.taskToken should be("a-task-token-for-tdr-task")
    lambdaRunResult.finalStepFnExecutions should have length 1
    lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-token-for-fcl-task") should be(defined)
    lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-token-for-fcl-task").exists(_.taskTokenSuccess) should be(false)
  }

  "lambda" should "delete the task from dynamo table when SFN client sendTaskSuccess errors as task time out" in {
    val initialDynamo = List(IngestQueueTableItem("TDR", Instant.now.minus(Duration.ofHours(1)).toString + "_TDR_2ec6248e_0", "a-task-already-running", "TDR_2ec6248e_0"))
    val validSourceSystems =
      List(SourceSystem("TDR", 2), SourceSystem("SystemTwo", 2, 65), SourceSystem("SystemThree", 1, 25), SourceSystem("DEFAULT", 0, 10), SourceSystem("Zero", 1))
    val initialConfig = FlowControlConfig(7, validSourceSystems, true)
    val existingExecutions = List(StepFunctionExecution("TDR_execution_name_1", "a-task-already-running"))
    val input = Option(Input("TDR_execution_name_2", ""))

    val lambdaRunResult = runLambda(input, initialDynamo, initialConfig, existingExecutions, predictableRandomNumberSelector(), Option(Errors(sendTaskSuccessTimeOut = true)))
    lambdaRunResult.result.isRight should be(true)
    lambdaRunResult.finalItemsInTable should have length 0
    lambdaRunResult.finalStepFnExecutions should have length 1
    lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-already-running").exists(_.taskTokenSuccess) should be(false)
  }

  forAll(enabledTable) { enabled =>
    val prefix = if enabled then "" else "not"
    "lambda" should s"$prefix process tasks from existing entries in the dynamo table when no task token is passed in the input with enabled $enabled" in {
      val initialItem = IngestQueueTableItem("TDR", Instant.now.minus(Duration.ofHours(1)).toString + "_TDR_2ec6248e_0", "task-token-for-tdr", "TDR_2ec6248e_0")
      val initialDynamo = List(initialItem)
      val validSourceSystems = List(SourceSystem("TDR", 2), SourceSystem("SystemTwo", 3), SourceSystem("SystemThree", 1, 100), SourceSystem("DEFAULT"))
      val ssmParam = FlowControlConfig(6, validSourceSystems, enabled)
      val sfnExecutions = List(StepFunctionExecution("TDR", "task-token-for-tdr"))

      val lambdaRunResult = runLambda(Some(Input("SYS_EXECUTION_NAME", "")), initialDynamo, ssmParam, sfnExecutions, predictableRandomNumberSelector())
      lambdaRunResult.result.isRight should be(true)
      lambdaRunResult.finalItemsInTable should have length (if enabled then 0 else 1)
      lambdaRunResult.finalStepFnExecutions should have length 1
      lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "task-token-for-tdr").head.taskTokenSuccess should be(enabled)
    }

    "lambda" should s"$prefix process tasks from existing entries in the dynamo table when there is no input with enabled $enabled" in {
      val initialItem = IngestQueueTableItem("TDR", Instant.now.minus(Duration.ofHours(1)).toString + "_TDR_2ec6248e_0", "task-token-for-tdr", "TDR_2ec6248e_0")
      val initialDynamo = List(initialItem)
      val validSourceSystems = List(SourceSystem("TDR", 2), SourceSystem("SystemTwo", 3), SourceSystem("SystemThree", 1, 100), SourceSystem("DEFAULT"))
      val ssmParam = FlowControlConfig(6, validSourceSystems, enabled)
      val sfnExecutions = List(StepFunctionExecution("TDR", "task-token-for-tdr"))

      val lambdaRunResult = runLambda(None, initialDynamo, ssmParam, sfnExecutions, predictableRandomNumberSelector())
      lambdaRunResult.result.isRight should be(true)
      lambdaRunResult.finalItemsInTable should have length (if enabled then 0 else 1)
      lambdaRunResult.finalStepFnExecutions should have length 1
      lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "task-token-for-tdr").head.taskTokenSuccess should be(enabled)
    }

    "lambda" should s"$prefix add a new task to dynamo table and turn the task success to true when it is processed with enabled $enabled" in {
      val initialDynamo = List.empty
      val validSourceSystems = List(SourceSystem("TDR", 2, 25), SourceSystem("FCL", 3, 65), SourceSystem("SystemThree", 1), SourceSystem("DEFAULT", 0, 10))
      val initialConfig = FlowControlConfig(6, validSourceSystems, enabled)
      val existingExecutions = List(
        StepFunctionExecution("FCL_execution_name", "a-task-token-for-fcl-task"),
        StepFunctionExecution("TDR_execution_name", "a-task-token-for-tdr-task")
      )
      val input = Option(Input("TDR_execution_name", "a-task-token-for-tdr-task"))

      val lambdaRunResult = runLambda(input, initialDynamo, initialConfig, existingExecutions, predictableRandomNumberSelector())
      lambdaRunResult.result.isRight should be(true)
      lambdaRunResult.finalStepFnExecutions should have length 2
      lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-token-for-fcl-task") should be(defined)
      lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-token-for-fcl-task").exists(_.taskTokenSuccess) should be(false)
      lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-token-for-tdr-task").exists(_.taskTokenSuccess) should be(enabled)

      lambdaRunResult.finalItemsInTable should have length (if enabled then 0 else 1)
    }

    "lambda" should s"$prefix add new task to dynamo but not send success when a reserved channel is not available for the system with enabled $enabled" in {
      val initialDynamo = List(
        IngestQueueTableItem("TDR", Instant.now.minus(Duration.ofHours(1)).toString + "_TDR_6b6db6bf_0", "a-task-already-running", "TDR_6b6db6bf_0"),
        IngestQueueTableItem("TST", Instant.now.minus(Duration.ofHours(2)).toString + "_TST_7b7db7bf_0", "a-task-for-system-two", "TST_7b7db7bf_0")
      )
      val validSourceSystems = List(SourceSystem("TDR", 1), SourceSystem("TST", 3, 65), SourceSystem("SystemThree", 1, 25), SourceSystem("DEFAULT", 0, 10))
      val initialConfig = FlowControlConfig(6, validSourceSystems, enabled)
      val existingExecutions = List(
        StepFunctionExecution("TDR_execution_name_1", "a-task-already-running"),
        StepFunctionExecution("TST_execution_name_1", "a-task-for-system-two")
      )
      val input = Option(Input("TDR_execution_name_2", "a-task-token-for-new-tdr-task"))

      val lambdaRunResult = runLambda(input, initialDynamo, initialConfig, existingExecutions, predictableRandomNumberSelector())
      lambdaRunResult.result.isRight should be(true)
      lambdaRunResult.finalStepFnExecutions should have length 2
      lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-already-running") should be(defined)
      lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-already-running").exists(_.taskTokenSuccess) should be(false)
      lambdaRunResult.finalStepFnExecutions.find(_.taskToken == "a-task-for-system-two").exists(_.taskTokenSuccess) should be(enabled)

      lambdaRunResult.finalItemsInTable should have length (if enabled then 2 else 3)
      lambdaRunResult.finalItemsInTable.map(_.taskToken).contains("a-task-token-for-new-tdr-task") should be(true)
    }

    "lambda" should s"$prefix send success for only one system with one invocation when a reserved channel is available with enabled $enabled" in {
      val initialDynamo = List(
        IngestQueueTableItem("TDR", Instant.now.minus(Duration.ofHours(2)).toString + "_TDR_2ec6248e_0", "tdr-task-1", "TST_6b6db6bf_0"),
        IngestQueueTableItem("FCL", Instant.now.minus(Duration.ofHours(1)).toString + "_FCL_3ec124be_0", "fcl-task-1", "FCL_3ec124be_0"),
        IngestQueueTableItem("ABC", Instant.now.toString + "_ABC_2ec6248e_0", "abc-task-1", "ABC_2ec6248e_0")
      )
      val validSourceSystems = List(SourceSystem("TDR", 2, 25), SourceSystem("FCL", 2, 65), SourceSystem("ABC", 2), SourceSystem("DEFAULT", 0, 10))
      val initialConfig = FlowControlConfig(7, validSourceSystems, enabled)
      val existingExecutions = List(
        StepFunctionExecution("TDR_execution_name_1", "tdr-task-1"),
        StepFunctionExecution("FCL_execution_name_1", "fcl-task-1"),
        StepFunctionExecution("ABC_execution_name_1", "abc-task-1")
      )
      val input = Option(Input("TDR_execution_name_2", "a-task-token-for-new-tdr-task"))

      val lambdaRunResult = runLambda(input, initialDynamo, initialConfig, existingExecutions, predictableRandomNumberSelector())
      lambdaRunResult.result.isRight should be(true)
      lambdaRunResult.finalStepFnExecutions should have length 3
      lambdaRunResult.finalStepFnExecutions.find(_.name == "TDR_execution_name_1").exists(_.taskTokenSuccess) should be(enabled)
      lambdaRunResult.finalStepFnExecutions.find(_.name != "TDR_execution_name_1").map(_.taskTokenSuccess).forall(identity) should be(false)

      lambdaRunResult.finalItemsInTable should have length (if enabled then 3 else 4)
      lambdaRunResult.finalItemsInTable.map(_.taskToken).contains("a-task-token-for-new-tdr-task") should be(true)
    }

    "lambda" should s"$prefix send success on a task based on the probability assigned in the configuration with enabled $enabled" in {
      val initialDynamo = List(
        IngestQueueTableItem("TDR", Instant.now.minus(Duration.ofHours(2)).toString + "_TDR_6b6db6bf_0", "tdr-task-1", "TDR_6b6db6bf_0"),
        IngestQueueTableItem("FCL", Instant.now.toString + "_FCL_7b7db7bf_0", "fcl-task-1", "FCL_7b7db7bf_0"),
        IngestQueueTableItem("ABC", Instant.now.toString + "_ABC_8b8db8bf_0", "abc-task-1", "ABC_8b8db8bf_0")
      )
      val validSourceSystems = List(SourceSystem("TDR", 1, 25), SourceSystem("FCL", 1, 65), SourceSystem("ABC", 1), SourceSystem("DEFAULT", 0, 10))
      val initialConfig = FlowControlConfig(4, validSourceSystems, enabled)
      val existingExecutions = List(
        StepFunctionExecution("TDR_execution_name_1", "tdr-task-1", true),
        StepFunctionExecution("FCL_execution_name_1", "fcl-task-1", false),
        StepFunctionExecution("ABC_execution_name_1", "abc-task-1", true)
      )
      val input = Option(Input("TDR_execution_name_2", "a-task-token-for-new-tdr-task"))

      val lambdaRunResult = runLambda(input, initialDynamo, initialConfig, existingExecutions, predictableRandomNumberSelector(26))
      lambdaRunResult.result.isRight should be(true)
      lambdaRunResult.finalStepFnExecutions should have length 3
      lambdaRunResult.finalStepFnExecutions.map(_.taskTokenSuccess).forall(identity) should be(enabled)

      lambdaRunResult.finalItemsInTable should have length (if enabled then 3 else 4)
      lambdaRunResult.finalItemsInTable.map(_.taskToken).contains("a-task-token-for-new-tdr-task") should be(true)
    }

    "lambda" should s"$prefix send success on a task based on the probability when the first pick system does not have a waiting task with enabled $enabled" in {
      val initialDynamo = List(
        IngestQueueTableItem("FCL", Instant.now.minus(Duration.ofHours(1)).toString + "_FCL_6b6db6bf_0", "fcl-task-1", "FCL_6b6db6bf_0"),
        IngestQueueTableItem("ABC", Instant.now.toString + "_ABC_7b7db7bf_0", "abc-task-1", "ABC_7b7db7bf_0")
      )
      val validSourceSystems = List(SourceSystem("TDR", 1, 25), SourceSystem("FCL", 1, 65), SourceSystem("ABC", 1), SourceSystem("DEFAULT", 0, 10))
      val initialConfig = FlowControlConfig(4, validSourceSystems, enabled)
      val existingExecutions = List(
        StepFunctionExecution("TDR_execution_name_1", "tdr-task-1", true),
        StepFunctionExecution("FCL_execution_name_1", "fcl-task-1"),
        StepFunctionExecution("ABC_execution_name_1", "abc-task-1", true)
      )
      val input = Option(Input("XYZ_execution_name_2", "a-task-token-for-new-xyz-task"))

      val lambdaRunResult = runLambda(input, initialDynamo, initialConfig, existingExecutions, predictableRandomNumberSelector())
      lambdaRunResult.result.isRight should be(true)
      lambdaRunResult.finalStepFnExecutions should have length 3
      lambdaRunResult.finalStepFnExecutions.find(_.name == "FCL_execution_name_1").exists(_.taskTokenSuccess) should be(enabled)

      lambdaRunResult.finalItemsInTable should have length (if enabled then 2 else 3)
      lambdaRunResult.finalItemsInTable.map(_.taskToken).contains("a-task-token-for-new-xyz-task") should be(true)
      lambdaRunResult.finalItemsInTable.map(_.taskToken).contains("abc-task-1") should be(true)
    }

    "lambda" should s"$prefix write a system name as DEFAULT if the system name is not available in the config with enabled $enabled" in {
      val initialDynamo = List(IngestQueueTableItem("TDR", Instant.now.minus(Duration.ofHours(1)).toString + "_TDR_6b6db6bf_0", "tdr-task-1", "TDR_6b6db6bf_0"))
      val validSourceSystems = List(SourceSystem("TDR", 0, 25), SourceSystem("FCL", 0, 65), SourceSystem("ABC", 1, 10), SourceSystem("DEFAULT"))
      val initialConfig = FlowControlConfig(4, validSourceSystems, enabled)
      val existingExecutions = List(StepFunctionExecution("TDR_execution_name_1", "tdr-task-1"))
      val input = Option(Input("HDDexecutionname2", "a-task-token-for-new-hard-disk-task"))

      val lambdaRunResult = runLambda(input, initialDynamo, initialConfig, existingExecutions, predictableRandomNumberSelector())
      lambdaRunResult.result.isRight should be(true)
      lambdaRunResult.finalItemsInTable should have length (if enabled then 1 else 2)
      lambdaRunResult.finalItemsInTable.head.sourceSystem should be("DEFAULT")
    }

    "lambda" should s"$prefix send success for a task when the system is not explicitly configured and DEFAULT has a reserved channel with enabled $enabled" in {
      val initialDynamo = List.empty
      val validSourceSystems = List(SourceSystem("TDR", 1, 25), SourceSystem("FCL", 1, 65), SourceSystem("ABC", 1), SourceSystem("DEFAULT", 1, 10))
      val initialConfig = FlowControlConfig(4, validSourceSystems, enabled)
      val existingExecutions = List(StepFunctionExecution("HDD_execution_name_2", "a-task-token-for-new-hard-disk-task"))
      val input = Option(Input("HDD_execution_name_2", "a-task-token-for-new-hard-disk-task"))
      val lambdaRunResult = runLambda(input, initialDynamo, initialConfig, existingExecutions, predictableRandomNumberSelector())

      lambdaRunResult.result.isRight should be(true)
      lambdaRunResult.finalItemsInTable should have length (if enabled then 0 else 1)
      lambdaRunResult.finalStepFnExecutions should have length 1
      lambdaRunResult.finalStepFnExecutions.head.taskTokenSuccess should be(enabled)
    }

    "lambda" should s"$prefix only add the new task to dynamo table when the maximum concurrency has been reached with enabled $enabled" in {
      val initialDynamo = List(
        IngestQueueTableItem("TDR", Instant.now.minus(Duration.ofHours(1)).toString + "_TDR_6b6db6bf_0", "a-task-already-running", "TDR_6b6db6bf_0"),
        IngestQueueTableItem("TST", Instant.now.minus(Duration.ofHours(2)).toString + "_TST_7b7db7bf_0", "a-running-task-for-system-two", "TST_7b7db7bf_0")
      )
      val validSourceSystems = List(SourceSystem("TDR", 1), SourceSystem("TST", 1, 65), SourceSystem("SystemThree", 0, 25), SourceSystem("DEFAULT", 0, 10))
      val initialConfig = FlowControlConfig(2, validSourceSystems, enabled)
      val existingExecutions = List(
        StepFunctionExecution("TDR_execution_name_1", "a-task-already-running", true),
        StepFunctionExecution("TST_execution_name_1", "a-running-task-for-system-two", true)
      )
      val input = Option(Input("TDR_execution_name_2", "a-task-token-for-new-tdr-task"))

      val lambdaRunResult = runLambda(input, initialDynamo, initialConfig, existingExecutions, predictableRandomNumberSelector())
      lambdaRunResult.result.isRight should be(true)
      lambdaRunResult.finalStepFnExecutions should have length 2
      lambdaRunResult.finalStepFnExecutions.map(_.taskTokenSuccess).forall(identity) should be(true)
      lambdaRunResult.finalItemsInTable should have length 3
      lambdaRunResult.finalItemsInTable.map(_.taskToken).contains("a-task-token-for-new-tdr-task") should be(true)
    }
  }

  "lambda" should "return FLOW_CONTROL_DISABLED if flow control is disabled" in {
    val validSourceSystems = List(SourceSystem("DEFAULT", 0, 100))
    val initialConfig = FlowControlConfig(2, validSourceSystems, false)
    val lambdaRunResult = runLambda(None, Nil, initialConfig, Nil, predictableRandomNumberSelector())
    lambdaRunResult.result.value.successSender should equal("FLOW_CONTROL_DISABLED")
    lambdaRunResult.result.value.executionStarter should equal("NO_EXECUTION_NAME")
  }
  // some case class validations
  "SourceSystem" should "error when the name is empty" in {
    intercept[IllegalArgumentException] {
      SourceSystem("")
    }.getMessage should be("requirement failed: System name should not be empty")
  }

  "SourceSystem" should "error when the reserved channels count is negative" in {
    intercept[IllegalArgumentException] {
      SourceSystem("something", -1)
    }.getMessage should be("requirement failed: Reserved channels should not be fewer than zero")
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
      FlowControlConfig(5, List.empty, true)
    }.getMessage should be("requirement failed: Source systems list cannot be empty")
  }

  "FlowControlConfig" should "error when the sum of probabilities in the flow control is not 100" in {
    intercept[IllegalArgumentException] {
      FlowControlConfig(6, List(SourceSystem("SystemOne", 2, 25), SourceSystem("SystemTwo", 3, 65), SourceSystem("SystemThree", 1), SourceSystem("default", 0, 5)), true)
    }.getMessage should be("requirement failed: The probability of all systems together should equate to 100%; the probability currently equates to 95%")
  }

  "FlowControlConfig" should "error when the max concurrency is less than 1" in {
    intercept[IllegalArgumentException] {
      FlowControlConfig(-5, List(SourceSystem("SystemOne", 2, 25), SourceSystem("SystemTwo", 3, 75)), true)
    }.getMessage should be("requirement failed: The max concurrency must be greater than 0, currently it is -5")
  }

  "FlowControlConfig" should "error when the reserved channels exceed maximum concurrency" in {
    intercept[IllegalArgumentException] {
      Lambda.FlowControlConfig(
        4,
        List(
          Lambda.SourceSystem("SystemOne", 1, 25),
          Lambda.SourceSystem("SystemTwo", 2, 65),
          Lambda.SourceSystem("SystemThree", 1, 10),
          Lambda.SourceSystem("default", 2)
        ),
        true
      )
    }.getMessage should be("requirement failed: Total of reserved channels of 6 exceeds maximum concurrency of 4")
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
        ),
        true
      )
    }.getMessage should be("requirement failed: System name must be unique")
  }

  "FlowControlConfig" should "error when there is no `DEFAULT` system available in the configuration" in {
    intercept[IllegalArgumentException] {
      Lambda.FlowControlConfig(
        4,
        List(
          Lambda.SourceSystem("SystemOne", 1, 25),
          Lambda.SourceSystem("SystemTwo", 0, 65),
          Lambda.SourceSystem("SystemThree", 1, 10),
          Lambda.SourceSystem("SystemFour", 2)
        ),
        true
      )
    }.getMessage should be("requirement failed: Missing 'DEFAULT' system in the configuration")
  }

  "FlowControlConfig" should "give availability of spare channels when at least one non-reserved channel is available" in {
    val configWithSpareChannels = Lambda.FlowControlConfig(
      4,
      List(
        Lambda.SourceSystem("SystemOne", 1, 25),
        Lambda.SourceSystem("SystemTwo", 0, 35),
        Lambda.SourceSystem("SystemThree", 0, 10),
        Lambda.SourceSystem("DEFAULT", 2, 30)
      ),
      true
    )
    configWithSpareChannels.hasSpareChannels should be(true)
  }

  "FlowControlConfig" should "indicate lack of spare channels when reserved channels equal the maximum concurrency" in {
    val configWithAllChannelsReserved =
      Lambda.FlowControlConfig(4, List(Lambda.SourceSystem("SystemOne", 1, 25), Lambda.SourceSystem("SystemTwo", 1, 35), Lambda.SourceSystem("DEFAULT", 2, 40)), true)
    configWithAllChannelsReserved.hasSpareChannels should be(false)
  }

  "FlowControlConfig" should "indicate true when at least one of the systems in the config has a reserved channel" in {
    val configWithSpareChannels = Lambda.FlowControlConfig(
      4,
      List(
        Lambda.SourceSystem("SystemOne", 0, 25),
        Lambda.SourceSystem("SystemTwo", 0, 35),
        Lambda.SourceSystem("SystemThree", 0, 10),
        Lambda.SourceSystem("DEFAULT", 1, 30)
      ),
      true
    )
    configWithSpareChannels.hasReservedChannels should be(true)
  }

  "FlowControlConfig" should "indicate false when none of the systems in the config have a reserved channel" in {
    val configWithAllChannelsReserved =
      Lambda.FlowControlConfig(4, List(Lambda.SourceSystem("SystemOne", 0, 25), Lambda.SourceSystem("SystemTwo", 0, 35), Lambda.SourceSystem("DEFAULT", 0, 40)), true)
    configWithAllChannelsReserved.hasReservedChannels should be(false)
  }

  "buildProbabilityRangesMap" should "build a map of system name to probability ranges for all systems" in {
    val probabilitiesMap = new Lambda().buildProbabilityRangesMap(
      List(Lambda.SourceSystem("SystemOne", 1, 25), Lambda.SourceSystem("SystemTwo", 0, 65), Lambda.SourceSystem("DEFAULT", 1, 10)),
      1,
      Map.empty[String, Range]
    )
    probabilitiesMap should have size 3
    probabilitiesMap("SystemOne").startInclusive should be(1)
    probabilitiesMap("SystemOne").endExclusive should be(26)
    probabilitiesMap("SystemTwo").startInclusive should be(26)
    probabilitiesMap("SystemTwo").endExclusive should be(91)
    probabilitiesMap("DEFAULT").startInclusive should be(91)
    probabilitiesMap("DEFAULT").endExclusive should be(101)
  }

  "buildProbabilityRangesMap" should "not include a system in the map where probability is defined as 0" in {
    val probabilitiesMap = new Lambda().buildProbabilityRangesMap(
      List(Lambda.SourceSystem("Zero", 1, 0), Lambda.SourceSystem("TwentyFive", 1, 25), Lambda.SourceSystem("SixtyFive", 0, 65), Lambda.SourceSystem("DEFAULT", 1, 10)),
      1,
      Map.empty[String, Range]
    )
    probabilitiesMap should have size 3
    probabilitiesMap("TwentyFive").startInclusive should be(1)
    probabilitiesMap("TwentyFive").endExclusive should be(26)
    probabilitiesMap("SixtyFive").startInclusive should be(26)
    probabilitiesMap("SixtyFive").endExclusive should be(91)
    probabilitiesMap("DEFAULT").startInclusive should be(91)
    probabilitiesMap("DEFAULT").endExclusive should be(101)
  }
