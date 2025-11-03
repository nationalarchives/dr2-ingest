{
  "Comment": "A state machine to run the preingest process for TDR transfers",
  "StartAt": "Wait",
  "States": {
    "Wait": {
      "Type": "Wait",
      "SecondsPath": "$.waitFor",
      "Next": "Invoke Package Builder Lambda"
    },
    "Invoke Package Builder Lambda": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "arn:aws:lambda:eu-west-2:${account_id}:function:${package_builder_lambda_name}"
      },
      "Retry": ${retry_statement},
      "Next": "Start Ingest Step Function",
      "ResultSelector": {
        "groupId.$": "$.Payload.groupId",
        "batchId.$": "$.Payload.batchId",
        "retryCount.$": "$.Payload.retryCount",
        "retrySfnArn.$": "$$.StateMachine.Id",
        "metadataPackage.$": "$.Payload.metadataPackage"
      }
    },
    "Start Ingest Step Function": {
      "Type": "Task",
      "Resource": "arn:aws:states:::states:startExecution",
      "Retry": ${retry_statement},
      "Parameters": {
        "StateMachineArn": "${ingest_step_function_arn}",
        "Name.$": "$$.Execution.Name",
        "Input.$": "$"
      },
      "End": true
    }
  }
}