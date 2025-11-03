{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "logs:PutLogEvents",
        "ecr:BatchCheckLayerAvailability"
      ],
      "Resource": [
        "arn:aws:logs:eu-west-2:${account_id}:log-group:/aws/ecs/e2e-tests:log-stream:*",
        "arn:aws:ecr:eu-west-2:${management_account_id}:repository/e2e-tests"
      ]
    },
    {
      "Sid": "",
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken"
      ],
      "Resource": "*"
    }
  ]
}
