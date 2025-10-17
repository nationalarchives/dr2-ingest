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
      "Sid": "ModifyEventBridgeRule",
      "Effect": "Allow",
      "Action": [
        "events:DescribeRule",
        "events:DisableRule",
        "events:EnableRule"
      ],
      "Resource": "${eventbridge_rule_arn}"
    },
    {
      "Sid": "UpdateSecretRotation",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:DescribeSecret",
        "secretsmanager:RotateSecret",
        "secretsmanager:CancelRotateSecret"
      ],
      "Resource": ${secret_arns}
    },
    {
      "Sid": "InvokeRotationLambdas",
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "${secret_rotation_arn}"
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