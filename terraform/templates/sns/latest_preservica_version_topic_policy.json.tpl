{
  "Version": "2008-10-17",
  "Id": "entity_event_topic",
  "Statement": [
    {
      "Sid": "SNSAllowLambdaToPublish",
      "Effect": "Allow",
      "Principal": {
        "AWS": "${lambda_role_arn}"
      },
      "Action": "SNS:Publish",
      "Resource": "${sns_topic}"
    }
  ]
}
