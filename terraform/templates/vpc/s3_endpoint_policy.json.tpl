{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "allow-callers-from-specific-account",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "*",
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "aws:ResourceAccount": "${account_id}"
        }
      }
    },
    {
      "Sid": "allow-calls-to-preservica-bucket",
      "Effect": "Allow",
      "Principal": "*",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::${preservica_ingest_bucket}",
        "arn:aws:s3:::${preservica_ingest_bucket}/*"
      ]
    }
  ]
}
