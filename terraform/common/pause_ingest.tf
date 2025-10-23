locals {
  pause_ingest = "${local.environment}-dr2-pause-ingest"
}
module "pause_ingest_lambda" {
  source        = "git::https://github.com/nationalarchives/da-terraform-modules//lambda?ref=DR2-2511-do-not-ignore-filename-if-set"
  function_name = local.pause_ingest
  s3_bucket     = local.code_deploy_bucket
  s3_key        = replace("${var.deploy_version}/${local.pause_ingest}", "${local.environment}-dr2-", "")
  handler       = "pause_ingest.lambda_handler"
  policies = {
    "${local.pause_ingest}-policy" : templatefile("${path.root}/templates/iam_policy/pause_ingest_lambda_policy.json.tpl", {
      account_number = data.aws_caller_identity.current.account_id
      environment    = local.environment
      lambda_name    = local.pause_ingest
    })
  }
  timeout_seconds = 10
  memory_size     = local.python_lambda_memory_size
  runtime         = local.python_runtime
  tags            = {}
  lambda_invoke_permissions = {
    "events.amazonaws.com" = module.pause_ingest_checker_cloudwatch_event.event_arn
  }
  plaintext_env_vars = {
    TRIGGER_ARNS = jsonencode([
      var.ingest.tdr_preingest.aggregator_sqs.sqs_arn,
      var.ingest.dri_preingest.aggregator_sqs.sqs_arn,
      var.ingest.court_document_event_handler_sqs.sqs_arn
    ])
    ENVIRONMENT = local.environment
  }
}

module "pause_ingest_checker_cloudwatch_event" {
  source                  = "git::https://github.com/nationalarchives/da-terraform-modules//cloudwatch_events"
  rule_name               = "${local.environment}-dr2-pause-ingest-event-schedule"
  schedule                = "cron(0 7-18 ? * MON-FRI *)"
  lambda_event_target_arn = "arn:aws:lambda:eu-west-2:${data.aws_caller_identity.current.account_id}:function:${local.pause_ingest}"
}

