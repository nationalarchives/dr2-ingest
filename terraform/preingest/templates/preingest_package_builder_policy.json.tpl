{
  "Statement": [
    {
      "Action": [
        "dynamodb:BatchGetItem",
        "dynamodb:Query"
      ],
      "Effect": "Allow",
      "Resource": [
        "${dynamo_db_lock_table_arn}",
        "${dynamo_db_lock_table_arn}/index/${gsi_name}"
      ],
      "Sid": "getAndQueryDynamoDB",
      "Condition":  {
        "StringEquals": {
          "aws:sourceVpc": "${vpc_id}"
        }
      }
    },
    {
      "Action": [
        "s3:PutObject*",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${raw_cache_bucket_name}",
        "arn:aws:s3:::${raw_cache_bucket_name}/*"
      ],
      "Sid": "readWriteIngestRawCache",
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
