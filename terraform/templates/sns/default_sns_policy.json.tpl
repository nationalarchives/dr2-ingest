{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "SameAccountPublishSubscribe",
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": [
        "SNS:Publish",
        "SNS:Subscribe"
      ],
      "Resource": "arn:aws:sns:eu-west-2:${account_id}:${topic_name}",
      "Condition": {
        "StringEquals": {
          "AWS:SourceOwner": "${account_id}"
        }
      }
    }
  ]
}
