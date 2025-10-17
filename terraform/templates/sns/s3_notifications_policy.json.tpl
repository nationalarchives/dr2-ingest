{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": [
        "sns:Publish"
      ],
      "Resource": "arn:aws:sns:eu-west-2:${account_id}:${topic_name}",
      "Condition": {
        "ArnLike": {
          "aws:SourceArn": "arn:aws:s3:*:*:${bucket_name}"
        },
        "StringEquals": {
          "aws:SourceAccount": "${account_id}"
        }
      }
    }
  ]
}
