{
  "Statement": [
    {
      "Action": [
        "dynamodb:BatchWriteItem",
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
      "Resource": ["${judgment_input_queue}", "${copy_files_from_tdr_queue}"],
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
        "${copy_files_from_tdr_log_group}"
      ]
    },
    {
      "Action": [
        "s3:PutObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${input_bucket_name}",
        "arn:aws:s3:::${input_bucket_name}/*"
      ],
      "Sid": "writeToRawCache"
    }
  ],
  "Version": "2012-10-17"
}
