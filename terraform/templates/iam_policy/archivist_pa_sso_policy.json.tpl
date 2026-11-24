{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "WriteBucketSendMessage",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "sqs:SendMessage"
      ],
      "Resource": [
        "arn:aws:s3:::${pa_bucket_name}/*",
        "${pa_importer_queue}"
      ]
    }
  ]
}