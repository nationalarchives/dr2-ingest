locals {
  pause_preservica_activity = "${local.environment}-dr2-pause-preservica-activity"
  secrets = [
    aws_secretsmanager_secret_rotation.secret_rotation,
    aws_secretsmanager_secret_rotation.secret_rotation_read_metadata,
    aws_secretsmanager_secret_rotation.secret_rotation_read_metadata_read_content,
    aws_secretsmanager_secret_rotation.secret_rotation_read_update_metadata_insert_content
  ]
}
module "pause_preservica_activity_lambda" {
  source        = "git::https://github.com/nationalarchives/da-terraform-modules//lambda?ref=DR2-2511-do-not-ignore-filename-if-set"
  function_name = local.pause_preservica_activity
  s3_bucket     = local.code_deploy_bucket
  s3_key        = replace("${var.deploy_version}/${local.pause_preservica_activity}", "${local.environment}-dr2-", "")
  handler       = "pause_preservica_activity.lambda_handler"
  policies = {
    "${local.pause_preservica_activity}-policy" : templatefile("${path.root}/templates/iam_policy/pause_preservica_activity_lambda_policy.json.tpl", {
      account_number       = data.aws_caller_identity.current.account_id
      environment          = local.environment
      lambda_name          = local.pause_preservica_activity,
      eventbridge_rule_arn = module.dr2_entity_event_cloudwatch_event.event_arn
      secret_arns          = jsonencode([for secret in local.secrets : secret.id])
      secret_rotation_arn  = module.dr2_rotate_preservation_system_password_lambda.lambda_arn
    })
  }
  timeout_seconds = 10
  memory_size     = local.python_lambda_memory_size
  runtime         = local.python_runtime
  tags            = {}
  lambda_invoke_permissions = {
    "events.amazonaws.com" = module.pause_preservica_activity_checker_cloudwatch_event.event_arn
  }
  plaintext_env_vars = {
    ENVIRONMENT = local.environment,
    SECRETS_MANAGER_DETAILS = jsonencode([
      for _, rotation in local.secrets : {
        id                  = rotation.secret_id,
        lambda_arn          = rotation.rotation_lambda_arn
        schedule_expression = rotation.rotation_rules[0].schedule_expression
      }
    ])
  }
}

module "pause_preservica_activity_checker_cloudwatch_event" {
  source                  = "git::https://github.com/nationalarchives/da-terraform-modules//cloudwatch_events"
  rule_name               = "${local.environment}-dr2-pause-preservica-activity-event-schedule"
  schedule                = "cron(0 7-18 ? * MON-FRI *)"
  lambda_event_target_arn = "arn:aws:lambda:eu-west-2:${data.aws_caller_identity.current.account_id}:function:${local.pause_preservica_activity}"
}

