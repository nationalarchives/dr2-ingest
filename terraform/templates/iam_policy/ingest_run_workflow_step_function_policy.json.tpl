{
  "Statement": [
    {
      "Sid": "sfnPolicy",
      "Effect": "Allow",
      "Action": [
        "states:RedriveExecution",
        "states:SendTaskSuccess",
        "lambda:InvokeFunction"
      ],
      "Resource": [
        "arn:aws:states:eu-west-2:${account_id}:stateMachine:${ingest_step_function_name}",
        "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_upsert_archive_folders_lambda_name}",
        "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_start_workflow_lambda_name}",
        "arn:aws:lambda:eu-west-2:${account_id}:function:${ingest_workflow_monitor_lambda_name}"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "events:PutTargets",
        "events:PutRule",
        "events:DescribeRule"
      ],
      "Resource": [
        "arn:aws:events:eu-west-2:${account_id}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule"
      ]
    }
  ],
  "Version": "2012-10-17"
}
