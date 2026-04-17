locals {
  delete_lambda_version_name = "${local.environment}-dr2-delete-lambda-version"
}
module "dr2_delete_lambda_version_schedule" {
  source                  = "git::https://github.com/nationalarchives/da-terraform-modules//cloudwatch_events"
  rule_name               = "${local.delete_lambda_version_name}-schedule"
  schedule                = "rate(1 day)"
  lambda_event_target_arn = "arn:aws:lambda:eu-west-2:${data.aws_caller_identity.current.account_id}:function:${local.entity_event_lambda_name}"
}

data "archive_file" "delete_lambda_version_code" {
  type        = "zip"
  source_file = "${path.module}/templates/lambda/delete_lambda_versions.py"
  output_path = "${path.module}/lambda/function.zip"
}

module "dr2_delete_lambda_version_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.delete_lambda_version_name
  handler         = "delete_lambda_versions.lambda_handler"
  filename        = data.archive_file.delete_lambda_version_code.output_path
  timeout_seconds = 60
  memory_size     = local.python_lambda_memory_size
  runtime         = local.python_runtime
  tags            = {}
  policies = {
    delete_version_policy = templatefile("${path.module}/templates/iam_policy/version_delete.json.tpl", {
      account_id  = data.aws_caller_identity.current.account_id,
      lambda_name = local.delete_lambda_version_name
    })
  }
}