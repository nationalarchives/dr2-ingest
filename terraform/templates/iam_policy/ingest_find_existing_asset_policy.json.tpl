{
  "Statement": [
    {
      "Action": [
        "dynamodb:BatchGetItem",
        "dynamodb:UpdateItem"
      ],
      "Effect": "Allow",
      "Resource": [
        "${dynamo_db_file_table_arn}"
      ],
      "Sid": "getAndUpdateDynamoDB",
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
      "Resource": [
        "${queue_arn}"
      ],
      "Sid": "readFromInputQueue"
    },
    {
      "Action": [
        "states:SendTaskSuccess",
        "states:SendTaskFailure"
      ],
      "Effect": "Allow",
      "Resource": [
        "${ingest_sfn_arn}"
      ],
      "Sid": "sendTaskSuccessOrFailure",
      "Condition":  {
        "StringEquals": {
          "aws:SourceVpc": "${vpc_id}"
        }
      }
    },
    {
      "Action": "secretsmanager:GetSecretValue",
      "Effect": "Allow",
      "Resource": "${secrets_manager_secret_arn}",
      "Sid": "readSecretsManager",
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
