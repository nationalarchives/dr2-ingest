{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowPARoleToSendMessages",
      "Effect": "Allow",
      "Principal": {
        "AWS": "${pa_role_arn}"
      },
      "Action": "SQS:SendMessage",
      "Resource": "${queue_arn}"
    }
  ]
}