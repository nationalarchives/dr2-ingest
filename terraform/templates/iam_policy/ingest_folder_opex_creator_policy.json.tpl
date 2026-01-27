{
  "Statement": [
    {
      "Action" : "sts:AssumeRole",
      "Effect": "Allow",
      "Resource": "${copy_to_preservica_role_arn}",
      "Sid": "assumeCopyToPreservicaRole",
      "Condition":  {
        "StringEquals": {
          "aws:sourceVpc": "${vpc_id}"
        }
      }      
    },
    {
      "Action": [
        "dynamodb:BatchGetItem",
        "dynamodb:Query"
      ],
      "Effect": "Allow",
      "Resource": [
        "${dynamo_db_file_table_arn}",
        "${dynamo_db_file_table_arn}/index/${gsi_name}"
      ],
      "Sid": "getAndQueryDynamoDB",
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
      "Sid": "readWriteLogs"
    }
  ],
  "Version": "2012-10-17"
}
