{
  "Statement": [
    {
      "Action": [
        "sqs:SendMessage"
      ],
      "Effect": "Allow",
      "Resource": "${custodial_copy_checker_queue_arn}",
      "Sid": "writeSqs",
      "Condition":  {
        "StringEquals": {
          "aws:SourceVpc": "${vpc_id}"
        }
      }      
    },
    {
      "Action": [
        "sqs:GetQueueAttributes"
      ],
      "Effect": "Allow",
      "Resource": "${custodial_copy_checker_queue_arn}",
      "Sid": "readSqs"
    },
    {
      "Action": [
        "dynamodb:BatchWriteItem",
        "dynamodb:PutItem",
        "dynamodb:BatchGetItem",
        "dynamodb:Query",
        "dynamodb:UpdateItem"
      ],
      "Effect": "Allow",
      "Resource": [
        "${postingest_state_arn}",
        "${postingest_state_arn}/index/${gsi_name}"
      ],
      "Sid": "readUpdateDynamoPostIngestTable",
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
