{
  "Statement": [
    {
      "Action": [
        "dynamodb:UpdateItem",
        "dynamodb:BatchGetItem"
      ],
      "Effect": "Allow",
      "Resource": "${dynamo_db_file_table_arn}",
      "Sid": "readWriteDynamoDB",
      "Condition":  {
        "StringEquals": {
          "aws:SourceVpc": "${vpc_id}"
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
          "aws:SourceVpc": "${vpc_id}"
        }
      }      
    },
    {
      "Action": "sns:Publish",
      "Effect": "Allow",
      "Resource": "${sns_arn}",
      "Sid": "writeSNS",
      "Condition":  {
        "StringEquals": {
          "aws:SourceVpc": "${vpc_id}"
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
      "Sid": "writeLogs"
    }
  ],
  "Version": "2012-10-17"
}
