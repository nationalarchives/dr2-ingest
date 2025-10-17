{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "invokePackageBuilderLambda",
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": [
        "arn:aws:lambda:eu-west-2:${account_id}:function:${package_builder_lambda_name}:*",
        "arn:aws:lambda:eu-west-2:${account_id}:function:${package_builder_lambda_name}"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "states:StartExecution"
      ],
      "Resource": [
        "${ingest_step_function_arn}"
      ]
    }
  ]
}