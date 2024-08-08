{
  "Statement": [
    {
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:GetQueueAttributes",
        "sqs:DeleteMessage"
      ],
      "Effect": "Allow",
      "Resource": "${copy_files_from_tdr_queue_arn}",
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
        "arn:aws:s3:::${raw_cache_bucket_name}",
        "arn:aws:s3:::${raw_cache_bucket_name}/*"
      ],
      "Sid": "readWriteIngestRawCache"
    },
    {
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${tdr_bucket_name}",
        "arn:aws:s3:::${tdr_bucket_name}/*"
      ],
      "Sid": "readFromTREBucket"
    },
    {
      "Action": [
        "sqs:SendMessage"
      ],
      "Effect": "Allow",
      "Resource": "${aggregator_queue_arn}",
      "Sid": "sendSqsMessage"
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
