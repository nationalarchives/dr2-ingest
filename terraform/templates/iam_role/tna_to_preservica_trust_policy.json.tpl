{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": [
          "${terraform_role_arn}",
          "${parent_folder_opex_creator_role_arn}",
          "${folder_opex_creator_role_arn}",
          "${asset_opex_creator_role_arn}"
        ]
      },
      "Action": "sts:AssumeRole"
    },
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow"
    }
  ]
}
