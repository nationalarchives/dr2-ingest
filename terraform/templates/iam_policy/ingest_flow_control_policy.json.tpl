{
  "Statement": [
    {
      "Action": [
        "dynamodb:DeleteItem",
        "dynamodb:Query",
        "dynamodb:PutItem",
        "dynamodb:BatchWriteItem"
      ],
      "Effect": "Allow",
      "Resource": "${dynamo_db_queue_table_arn}",
      "Sid": "readWriteDynamoDB"
    },
    {
      "Action": "ssm:GetParameter",
      "Effect": "Allow",
      "Resource": "${ssm_parameter_arn}",
      "Sid": "readSSM"
    },
    {
      "Action": [
        "states:sendTaskSuccess"
      ],
      "Effect": "Allow",
      "Resource": "${ingest_step_function_arn}",
      "Sid": "sendTaskSuccess"
    },
    {
      "Action": [
        "states:listExecutions"
      ],
      "Effect": "Allow",
      "Resource": "${workflow_step_function_arn}",
      "Sid": "listExecutions"
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