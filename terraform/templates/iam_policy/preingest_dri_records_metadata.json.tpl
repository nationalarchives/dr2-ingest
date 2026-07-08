{
  "Statement": [
    {
      "Action": [
        "s3:ListBucket",
        "s3:GetObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${records_metadata_bucket}",
        "arn:aws:s3:::${records_metadata_bucket}/*"
      ]
    },
    {
      "Action": [
        "s3:PutObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${source_bucket}",
        "arn:aws:s3:::${source_bucket}/*"
      ]
    }
  ],
  "Version": "2012-10-17"
}
