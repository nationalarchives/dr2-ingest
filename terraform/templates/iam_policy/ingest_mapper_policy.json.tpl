{
  "Statement": [
    {
      "Action": [
        "dynamodb:BatchWriteItem"
      ],
      "Effect": "Allow",
      "Resource": "${dynamo_db_file_table_arn}",
      "Sid": "writeDynamoDB"
    },
    {
      "Action": [
        "s3:GetObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${raw_cache_bucket_name}",
        "arn:aws:s3:::${raw_cache_bucket_name}/*"
      ],
      "Sid": "readIngestRawCache"
    },
    {
      "Action": [
        "s3:PutObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${ingest_state_bucket_name}",
        "arn:aws:s3:::${ingest_state_bucket_name}/*"
      ],
      "Sid": "writeIngestState"
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
