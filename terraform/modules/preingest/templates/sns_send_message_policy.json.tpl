{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "sns.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:eu-west-2:${account_id}:${queue_name}",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "${topic_arn}"
        }
      }
    }
  ]
}
