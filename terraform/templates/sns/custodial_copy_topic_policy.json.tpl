{
  "Version": "2008-10-17",
  "Id": "custodial_copy_topic",
  "Statement": [
    {
      "Sid": "SNSAllowLambdaToPublish",
      "Effect": "Allow",
      "Principal": {
        "AWS": "${cc_user_arn}"
      },
      "Action": "SNS:Publish",
      "Resource": "${sns_topic}"
    }
  ]
}