{
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:UpdateTimeToLive",
        "dynamodb:Query",
        "dynamodb:UpdateItem"
      ],
      "Resource": [
        "${dynamodb_table_arn}",
        "${dynamodb_table_arn}/index/${index_name}"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObjectTagging",
        "s3:GetObjectTagging",
        "s3:DeleteObjectTagging"
      ],
      "Resource": [
        "arn:aws:s3:::${bucket_name}",
        "arn:aws:s3:::${bucket_name}/*"
      ],
      "Sid": "manageS3ObjectTags"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:${region_name}:${account_id}:log-group:/aws/lambda/${lambda_name}:*:*",
        "arn:aws:logs:${region_name}:${account_id}:log-group:/aws/lambda/${lambda_name}:*"
      ],
        "Sid": "writeCleanupLogs"
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "${sqs_queue_arn}",
      "Sid": "allowSqsPolling"
    }
  ],
  "Version": "2012-10-17"
}
