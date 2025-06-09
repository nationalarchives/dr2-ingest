{
  "Statement": [
    {
      "Action": [
        "sqs:SendMessage"
      ],
      "Effect": "Allow",
      "Resource": "${custodial_copy_checker_queue_arn}",
      "Sid": "readSqs"
    },
    {
      "Sid": "APIAccessForDynamoDBStreams",
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetRecords",
        "dynamodb:GetShardIterator",
        "dynamodb:DescribeStream",
        "dynamodb:ListStreams"
      ],
      "Resource": "${dynamo_db_postingest_stream_arn}"
    },
    {
      "Action": [
        "dynamodb:BatchWriteItem",
        "dynamodb:PutItem",
        "dynamodb:DeleteItem"
      ],
      "Effect": "Allow",
      "Resource": [
        "${dynamo_db_postingest_arn}"
      ],
      "Sid": "readUpdateDeleteDynamoPostIngestTable"
    },
    {
      "Action": "sns:Publish",
      "Effect": "Allow",
      "Resource": "${sns_external_notifications_arn}",
      "Sid": "writeSNS"
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
    }
  ],
  "Version": "2012-10-17"
}
