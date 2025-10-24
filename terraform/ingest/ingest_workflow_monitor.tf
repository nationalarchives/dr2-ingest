module "dr2_ingest_workflow_monitor_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda?ref=DR2-2511-do-not-ignore-filename-if-set"
  function_name   = var.lambda_names.workflow_monitor
  s3_bucket       = local.code_deploy_bucket
  s3_key          = replace("${var.deploy_version}/${var.lambda_names.workflow_monitor}", "${local.environment}-dr2-", "")
  handler         = "uk.gov.nationalarchives.ingestworkflowmonitor.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${var.lambda_names.workflow_monitor}-policy" = templatefile("./templates/iam_policy/ingest_workflow_monitor_policy.json.tpl", {
      account_id                 = data.aws_caller_identity.current.account_id
      lambda_name                = var.lambda_names.workflow_monitor
      secrets_manager_secret_arn = var.preservica_secret.arn
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    PRESERVICA_SECRET_NAME = var.preservica_secret.name
  }
  vpc_config = {
    subnet_ids         = var.private_subnets
    security_group_ids = local.outbound_security_group_ids
  }
  tags = {
    Name = var.lambda_names.workflow_monitor
  }
}
