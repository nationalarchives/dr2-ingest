{
  "Statement": [
    {
      "Action": [
        "sqs:SendMessage"
      ],
      "Effect": "Allow",
      "Resource": "${state_change_handler_queue_arn}",
      "Sid": "sendSqsMessage"
    }
  ],
  "Version": "2012-10-17"
}
