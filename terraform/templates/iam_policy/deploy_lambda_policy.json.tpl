{
  "Statement": [
    {
      "Action" : [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${bucket_name}/*",
        "arn:aws:s3:::${bucket_name}"
      ]
    },
    {
      "Action": [
        "lambda:GetFunctionConfiguration",
        "lambda:GetFunction",
        "lambda:PublishVersion",
        "lambda:UpdateFunctionCode"
      ],
      "Effect": "Allow",
      "Resource": ${lambda_arns}
    },
    {
      "Action": "events:PutEvents",
      "Effect": "Allow",
      "Resource": "arn:aws:events:eu-west-2:${account_id}:event-bus/default",
      "Sid": "putEventbridgeEvents"
    }
  ],
  "Version": "2012-10-17"
}
