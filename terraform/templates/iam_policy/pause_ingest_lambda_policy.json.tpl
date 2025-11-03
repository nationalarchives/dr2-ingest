{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EventBridgePutEvents",
      "Effect": "Allow",
      "Action": "events:PutEvents",
      "Resource": "arn:aws:events:eu-west-2:${account_number}:event-bus/default"
    },
    {
      "Sid": "ListEventSourceMappings",
      "Effect": "Allow",
      "Action": [
        "lambda:ListEventSourceMappings"
      ],
      "Resource": "*"
    },
    {
      "Sid": "UpdateEventSourceMappings",
      "Effect": "Allow",
      "Action": [
        "lambda:UpdateEventSourceMapping"
      ],
      "Resource": "arn:aws:lambda:eu-west-2:${account_number}:event-source-mapping:*"
    },
    {
      "Sid": "SSMGetAndPutParameter",
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter",
        "ssm:PutParameter"
      ],
      "Resource": "arn:aws:ssm:eu-west-2:${account_number}:parameter/${environment}/flow-control-config"
    },
    {
      "Effect": "Allow",
      "Action": "logs:CreateLogGroup",
      "Resource": "arn:aws:logs:eu-west-2:${account_number}:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:eu-west-2:${account_number}:log-group:/aws/lambda/${lambda_name}:*"
      ]
    }
  ]
}