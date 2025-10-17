{
  "Statement": [
    {
      "Action": [
        "states:listExecutions",
        "states:ListStateMachines"
      ],
      "Effect": "Allow",
      "Resource": ["${workflow_step_function_arn}", "arn:aws:states:eu-west-2:${account_id}:stateMachine:*"],
      "Sid": "listExecutions"
    },
    {
      "Action"   : "cloudwatch:PutMetricData",
      "Effect"   : "Allow",
      "Resource" : "*",
      "Sid"      : "PutMetricData"
    },
    {
      "Action": "dynamodb:Query",
      "Effect": "Allow",
      "Resource": "${ingest_queue_table_arn}",
      "Sid": "DynamoQuery"
    },
    {
      "Action": [
        "logs:PutLogEvents",
        "logs:CreateLogStream",
        "logs:CreateLogGroup"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/aws/lambda/${lambda_name}:*:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/aws/lambda/${lambda_name}:*"
      ],
      "Sid": "writeLogs"
    }
  ],
  "Version": "2012-10-17"
}