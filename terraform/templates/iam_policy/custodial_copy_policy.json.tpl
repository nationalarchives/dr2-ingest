{
  "Statement": [
    {
      "Action": "secretsmanager:GetSecretValue",
      "Effect": "Allow",
      "Resource": "${secrets_manager_secret_arn}",
      "Sid": "readSecretsManager"
    },
    {
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:GetQueueAttributes",
        "sqs:DeleteMessage"
      ],
      "Effect": "Allow",
      "Resource": [
        "${custodial_copy_queue}",
        "${database_builder_queue}",
        "${custodial_copy_confirmer_queue}"
      ],
      "Sid": "readSqs"
    },
    {
      "Action": [
        "dynamodb:UpdateItem"
      ],
      "Effect": "Allow",
      "Resource": "${postingest_table}"
    },
    {
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Effect": "Allow",
      "Resource": "*",
      "Sid": "putMetrics"
    },
    {
      "Action": "events:PutEvents",
      "Effect": "Allow",
      "Resource": "arn:aws:events:eu-west-2:${account_id}:event-bus/default"
    },
    {
      "Action": [
        "logs:PutLogEvents",
        "logs:PutRetentionPolicy",
        "logs:DescribeLogStreams",
        "logs:DescribeLogGroups",
        "logs:CreateLogStream",
        "logs:CreateLogGroup"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-backend:*:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-backend:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-backend",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-webapp:*:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-webapp:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-webapp",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-re-indexer:*:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-re-indexer:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-re-indexer",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-db-builder:*:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-db-builder:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-confirmer:*:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-confirmer:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-builder",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-reconciler:*:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-reconciler:*",
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/custodial-copy-reconciler"
      ],
      "Sid": "writeLogs"
    },
    {
      "Action" : "ecr:GetAuthorizationToken",
      "Effect": "Allow",
      "Resource": "*"
    },
    {
      "Action" : [
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:ecr:eu-west-2:${management_account_id}:repository/dr2-custodial-copy-backend",
        "arn:aws:ecr:eu-west-2:${management_account_id}:repository/dr2-custodial-copy-db-builder",
        "arn:aws:ecr:eu-west-2:${management_account_id}:repository/dr2-custodial-copy-webapp",
        "arn:aws:ecr:eu-west-2:${management_account_id}:repository/dr2-custodial-copy-re-indexer",
        "arn:aws:ecr:eu-west-2:${management_account_id}:repository/dr2-custodial-copy-confirmer",
        "arn:aws:ecr:eu-west-2:${management_account_id}:repository/dr2-custodial-copy-reconciler"
      ]
    }
  ],
  "Version": "2012-10-17"
}
