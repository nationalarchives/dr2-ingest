{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DeleteNaclEntry",
      "Effect": "Allow",
      "Action": "ec2:DeleteNetworkAclEntry",
      "Resource": ["${public_nacl_arn}", "${private_nacl_arn}"]
    },
    {
      "Sid": "DescribeNacls",
      "Effect": "Allow",
      "Action": "ec2:DescribeNetworkAcls",
      "Resource": "*"
    }
  ]
}