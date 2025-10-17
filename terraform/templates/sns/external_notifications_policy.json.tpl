{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "TDRPublishSubscribe",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${tdr_account_id}:root"
      },
      "Action": "sns:Subscribe",
      "Resource": "arn:aws:sns:eu-west-2:${account_id}:${topic_name}",
      "Condition": {
        "ArnEquals": {
          "aws:PrincipalArn": "${tdr_terraform_role}"
        }
      }
    }
  ]
}