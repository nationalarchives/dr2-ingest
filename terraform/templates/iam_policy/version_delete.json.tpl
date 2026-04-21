{
  "Statement": [
    {
      "Action": [
        "lambda:ListVersionsByFunction",
        "lambda:DeleteFunction"
      ],
      "Effect": "Allow",
      "Resource": ${lambdas_to_delete},
      "Sid": "updateLambdaVersions"
    },
    {
      "Action": [
        "lambda:ListFunctions"
      ],
      "Effect": "Allow",
      "Resource": "*",
      "Sid": "listFunctions"
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
