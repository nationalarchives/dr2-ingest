{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3SQSAccess",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "sqs:SendMessage",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:sqs:eu-west-2:${account_id}:${environment}-dr2-preingest-dri-importer",
        "arn:aws:s3:::${environment}-dr2-ingest-dri-migration-cache",
        "arn:aws:s3:::${environment}-dr2-ingest-dri-migration-cache/*"
      ]
    }
  ]
}