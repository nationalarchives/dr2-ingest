{
  "Statement": [
    {
      "Action": [
        "lambda:ListFunctions",
        "lambda:ListVersionsByFunction",
        "lambda:DeleteFunction"
      ],
      "Effect": "Allow",
      "Resource": ["arn:aws:lambda:eu-west-2:${account_id}:function:*", "arn:aws:lambda:eu-west-2:${account_id}:function:*:*"],
      "Sid": "updateLambdaVersions"
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
