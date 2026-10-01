{
  "Statement": [
    {
      "Action": [
        "s3:GetObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${pa_metadata_bucket}",
        "arn:aws:s3:::${pa_metadata_bucket}/*"
      ]
    }
  ],
  "Version": "2012-10-17"
}
