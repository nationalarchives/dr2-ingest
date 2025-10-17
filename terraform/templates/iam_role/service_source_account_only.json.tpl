{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "${service}.amazonaws.com"
      },
      "Effect": "Allow",
      "Condition": {
        "StringEquals": {
          "aws:SourceAccount": "${account_id}"
        }
      }
    }
  ]
}
