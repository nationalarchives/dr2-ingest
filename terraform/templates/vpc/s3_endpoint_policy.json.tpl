{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "allowCallersFromSpecificAccount",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "aws:ResourceAccount": "${account_id}"
        }
      }
    },
    {
      "Sid": "allowCallsToPreservicaBucket",
      "Effect": "Allow",
      "Principal": "*",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::${preservica_ingest_bucket}",
        "arn:aws:s3:::${preservica_ingest_bucket}/*",
        "arn:aws:s3:::${tdr_export_bucket}",
        "arn:aws:s3:::${tdr_export_bucket}/*",
        "${tre_export_bucket_arn}",
        "${tre_export_bucket_arn}/*"
      ]
    }
  ]
}
