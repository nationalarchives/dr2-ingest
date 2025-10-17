{
  "Statement": [
    {
      "Action": [
        "dynamodb:BatchGetItem",
        "dynamodb:Query",
        "dynamodb:BatchWriteItem",
        "dynamodb:PutItem"
      ],
      "Effect": "Allow",
      "Resource": [
        "${dynamo_db_lock_table_arn}"
      ],
      "Sid": "getAndUpdateDynamoDB"
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
      "Sid": "readWriteLogs"
    },
    {
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:GetQueueAttributes",
        "sqs:DeleteMessage"
      ],
      "Effect": "Allow",
      "Resource": "${preingest_aggregator_queue}",
      "Sid": "readSqs"
    },
    {
      "Action": [
        "states:StartExecution"
      ],
      "Effect": "Allow",
      "Resource": [
        "${preingest_sfn_arn}"
      ],
      "Sid": "startIngestSfn"
    }
  ],
  "Version": "2012-10-17"
}
