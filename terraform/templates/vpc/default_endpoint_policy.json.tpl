{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "allowAccessOnlyFromSameOrgId",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "${service_name}:*",
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "aws:resourceOrgId": "${org_id}"
        }
      }
    }
  ]
}
