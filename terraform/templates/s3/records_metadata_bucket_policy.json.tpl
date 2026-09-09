{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "crossAccountReadObject",
      "Effect": "Allow",
      "Principal": {
        "AWS": ["${ayr_data_migration_worker_role}", "${ayr_data_migration_coordinator_role}"]
      },
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::${bucket_name}/live/*"
    },
    {
      "Sid": "crossAccountReadBucket",
      "Effect": "Allow",
      "Principal": {
        "AWS": "${ayr_data_migration_coordinator_role}"
      },
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::${bucket_name}",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "live/",
            "live/*"
          ]
        }
      }
    }
  ]
}