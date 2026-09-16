{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "InvokeFunction",
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "${lambda_arn}"
    }
  ]
}