{
  "Statement": [
    {
      "Action": [
        "sqs:SendMessage"
      ],
      "Effect": "Allow",
      "Resource": ["${custodial_copy_checker_queue_arn}", "${state_change_dlq_arn}"],
      "Sid": "readSqs",
      "Condition":  {
        "StringEquals": {
          "aws:sourceVpc": "${vpc_id}"
        }
      }
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
      "Resource": "${dynamo_db_postingest_stream_arn}",
      "Condition":  {
        "StringEquals": {
          "aws:sourceVpc": "${vpc_id}"
        }
      }
    },
    {
      "Action": [
        "dynamodb:BatchWriteItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem"
      ],
      "Effect": "Allow",
      "Resource": [
        "${dynamo_db_postingest_arn}"
      ],
      "Sid": "readUpdateDeleteDynamoPostIngestTable",
      "Condition":  {
        "StringEquals": {
          "aws:sourceVpc": "${vpc_id}"
        }
      }
    },
    {
      "Action": "sns:Publish",
      "Effect": "Allow",
      "Resource": "${sns_external_notifications_arn}",
      "Sid": "writeSNS",
      "Condition":  {
        "StringEquals": {
          "aws:sourceVpc": "${vpc_id}"
        }
      }
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
