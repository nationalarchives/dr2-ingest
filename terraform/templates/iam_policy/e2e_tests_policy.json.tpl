{
  "Statement": [
    {
      "Action": [
        "dynamodb:BatchWriteItem",
        "dynamodb:BatchGetItem",
        "dynamodb:PutItem"
      ],
      "Effect": "Allow",
      "Resource": [
        "${dynamo_db_lock_table_arn}"
      ],
      "Sid": "updateDynamoLockTable"
    },
    {
      "Action": [
        "states:DescribeExecution"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:states:eu-west-2:${account_id}:execution:${ingest_sfn_name}:*"
      ],
      "Sid": "describeIngestStepFunction"
    },
    {
      "Action": [
        "states:StartExecution"
      ],
      "Effect": "Allow",
      "Resource": [
        "${preingest_sfn_arn}"
      ],
      "Sid": "startPreingestSfn"
    },
    {
      "Action": [
        "sqs:SendMessage"
      ],
      "Effect": "Allow",
      "Resource": ["${judgment_input_queue}", "${copy_files_from_tdr_queue}", "${adhoc_import_queue}", "${dri_import_queue}"],
      "Sid": "sendSqsMessage"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:StartLiveTail",
        "logs:GetLogEvents"
      ],
      "Resource": [
        "${external_notifications_log_group}",
        "${copy_files_from_tdr_log_group}",
        "${copy_files_from_adhoc_log_group}",
        "${copy_files_from_dri_log_group}",
        "${copy_files_from_courtdoc_log_group}"
      ]
    },
    {
      "Action": [
        "s3:PutObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${input_bucket_name}",
        "arn:aws:s3:::${input_bucket_name}/*",
        "arn:aws:s3:::${adhoc_input_bucket_name}",
        "arn:aws:s3:::${adhoc_input_bucket_name}/*",
        "arn:aws:s3:::${dri_input_bucket_name}",
        "arn:aws:s3:::${dri_input_bucket_name}/*"
      ],
      "Sid": "writeToRawCache"
    },
    {
      "Action": [
        "s3:ListBucketVersions",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:GetObjectTagging"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${raw_cache_bucket_name}",
        "arn:aws:s3:::${raw_cache_bucket_name}/*"
      ],
      "Sid": "readFromRawCache"
    }
  ],
  "Version": "2012-10-17"
}
