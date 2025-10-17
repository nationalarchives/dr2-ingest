{
  "Statement": [
    {
      "Action": [
        "ssm:GetParameter"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:ssm:eu-west-2:${account_id}:parameter/${environment}/preservica/bulk1/s3/user",
        "arn:aws:ssm:eu-west-2:${account_id}:parameter/${environment}/preservica/bulk2/s3/user",
        "arn:aws:ssm:eu-west-2:${account_id}:parameter/${environment}/preservica/bulk3/s3/user",
        "arn:aws:ssm:eu-west-2:${account_id}:parameter/${environment}/preservica/bulk4/s3/user"
      ],
      "Sid": "readSsmParameter"
    },
    {
      "Action": [
        "s3:GetObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::${bucket_name}",
        "arn:aws:s3:::${bucket_name}/*"
      ],
      "Sid": "readStagingCache"
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
