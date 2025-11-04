{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "allow-callers-from-specific-account",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "*",
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "aws:ResourceAccount": "${account_id}"
        }
      }
    }
  ]
}
