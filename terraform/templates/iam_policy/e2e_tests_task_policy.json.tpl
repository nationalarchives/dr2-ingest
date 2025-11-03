{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "secretsmanager:GetSecretValue",
        "sqs:SendMessage"
      ],
      "Resource": [
        "arn:aws:s3:::${court_document_test_input_bucket}/*",
        "${secret_arn}",
        "${sqs_arn}"
      ]
    }
  ]
}
