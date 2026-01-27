{
  "Statement": [
    {
      "Action": "events:PutEvents",
      "Effect": "Allow",
      "Resource": "arn:aws:events:eu-west-2:${account_id}:event-bus/default",
      "Sid": "putEventbridgeEvents",
      "Condition":  {
        "StringEquals": {
          "aws:sourceVpc": "${vpc_id}"
        }
      }
    },
    {
      "Action": "dynamodb:BatchGetItem",
      "Effect": "Allow",
      "Resource": "${dynamo_db_file_table_arn}",
      "Sid": "getDynamoDB",
      "Condition":  {
        "StringEquals": {
          "aws:sourceVpc": "${vpc_id}"
        }
      }
    },
    {
      "Action": "secretsmanager:GetSecretValue",
      "Effect": "Allow",
      "Resource": "${secrets_manager_secret_arn}",
      "Sid": "readSecretsManager",
      "Condition":  {
        "StringEquals": {
          "aws:sourceVpc": "${vpc_id}"
        }
      }
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
      "Sid": "readWriteLogs",
      "Condition":  {
        "StringEquals": {
          "aws:sourceVpc": "${vpc_id}"
        }
      }
    }
  ],
  "Version": "2012-10-17"
}
