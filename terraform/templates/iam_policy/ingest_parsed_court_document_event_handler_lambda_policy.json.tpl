{
  "Statement": [
    {
      "Action": [
        "states:StartExecution"
      ],
      "Effect": "Allow",
      "Resource": "${step_function_arn}",
      "Sid": "startSfnExecution",
      "Condition":  {
        "StringEquals": {
          "aws:SourceVpc": "${vpc_id}"
        }
      }
    },
    {
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:GetQueueAttributes",
        "sqs:DeleteMessage"
      ],
      "Effect": "Allow",
      "Resource": "${ingest_parsed_court_document_event_handler_queue_arn}",
      "Sid": "readSqs"
    },
    {
      "Action": [
        "s3:PutObject*",
        "s3:GetObject",
        "s3:DeleteObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${bucket_name}",
        "arn:aws:s3:::${bucket_name}/*"
      ],
      "Sid": "readWriteIngestRawCache",
      "Condition":  {
        "ArnEquals": {
          "aws:SourceVpcArn": "${vpc_arn}"
        }
      }
    },
    {
      "Action": [
        "dynamodb:PutItem"
      ],
      "Effect": "Allow",
      "Resource": "${dynamo_db_lock_table_arn}",
      "Sid": "writeDynamoDB",
      "Condition":  {
        "StringEquals": {
          "aws:SourceVpc": "${vpc_id}"
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
