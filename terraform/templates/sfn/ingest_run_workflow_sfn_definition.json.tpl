{
  "Comment": "${step_function_name}: A child State machine to upsert folders in Preservica and run workflows",
  "StartAt": "Create or update folders in Preservica",
  "States": {
    "Create or update folders in Preservica": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_upsert_archive_folders_lambda_name}",
      "Retry": ${upsert_lambda_retry_statement},
      "Next": "Start workflow",
      "ResultPath": null
    },
    "Start workflow": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_start_workflow_lambda_name}",
        "Payload": {
          "workflowContextName": "Ingest OPEX (Incremental)",
          "executionId.$": "$.batchId"
        }
      },
      "Retry": ${retry_statement},
      "ResultPath": null,
      "Next": "Wait 5 minutes before getting status"
    },
    "Wait 5 minutes before getting status": {
      "Type": "Wait",
      "Next": "Get workflow status",
      "Seconds": 300
    },
    "Get workflow status": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "Payload": {
          "executionId.$": "$.batchId"
        },
        "FunctionName": "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_workflow_monitor_lambda_name}"
      },
      "Retry": ${retry_statement},
      "ResultSelector": {
        "status.$": "$.Payload.status",
        "mappedId.$": "$.Payload.mappedId"
      },
      "ResultPath": "$.WorkflowResult",
      "Next": "Check workflow status"
    },
    "Check workflow status": {
      "Type": "Choice",
      "Choices": [
        {
          "Or": [
            {
              "Variable": "$.WorkflowResult.status",
              "StringEquals": "Failed"
            },
            {
              "Variable": "$.WorkflowResult.status",
              "StringEquals": "Succeeded"
            }
          ],
          "Next": "SendTaskSuccess"
        }
      ],
      "Default": "Wait 5 minutes before getting status"
    },
    "SendTaskSuccess": {
      "Type": "Task",
      "Parameters": {
        "Output": "{}",
        "TaskToken.$": "$.taskToken"
      },
      "Retry": ${retry_statement},
      "Resource": "arn:aws:states:::aws-sdk:sfn:sendTaskSuccess",
      "End": true
    }
  }
}
