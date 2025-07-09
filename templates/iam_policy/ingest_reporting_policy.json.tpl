{
  "Statement": [
    {
      "Action": [
        "logs:GetLogEvents",
        "states:ListExecutions"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/${environment}-external-notifications:*:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/${environment}-external-notifications:*",
        "arn:aws:states:eu-west-2:${account_id}:stateMachine:*"
      ],
      "Sid": "readNotificationLogsAndStepFunctionStates"
    }
  ],
  "Version": "2012-10-17"
}