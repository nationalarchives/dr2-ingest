{
  "Statement": [
    {
      "Sid": "listDynamoTables",
      "Effect": "Allow",
      "Action": "dynamodb:ListTables",
      "Resource": "*"
    },
    {
      "Action": [
        "logs:GetLogEvents",
        "states:ListExecutions",
        "dynamodb:DescribeTable"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/${environment}-external-notifications:*:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/${environment}-external-notifications:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-reconciler:*:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-reconciler:*",
        "arn:aws:states:eu-west-2:${account_id}:stateMachine:*",
        "arn:aws:dynamodb:*:${account_id}:table/*"
      ],
      "Sid": "readNotificationLogsAndStepFunctionStates"
    }
  ],
  "Version": "2012-10-17"
}