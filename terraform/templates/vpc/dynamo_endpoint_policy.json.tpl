{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "allowCallersFromSpecificAccount",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "dynamodb:*",
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "aws:ResourceAccount": "${account_id}"
        }
      }
    }
  ]
}
