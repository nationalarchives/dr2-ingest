locals {
  ingest_failure_notifications_lambda_name = "${local.environment}-dr2-ingest-failure-notifications"
}

module "dr2_ingest_failure_notifications_lambda" {
  source        = "git::https://github.com/nationalarchives/da-terraform-modules//lambda?ref=DR2-2511-do-not-ignore-filename-if-set"
  function_name = local.ingest_failure_notifications_lambda_name
  handler       = "uk.gov.nationalarchives.ingestfailurenotifications.Lambda::handleRequest"
  policies = {
    "${local.ingest_failure_notifications_lambda_name}-policy" = templatefile("${path.root}/templates/iam_policy/failure_notifications_policy.json.tpl", {
      account_id               = data.aws_caller_identity.current.account_id
      lambda_name              = local.ingest_failure_notifications_lambda_name
      dynamo_db_file_table_arn = var.ingest_lock_table_arn
      gsi_name                 = local.ingest_lock_table_group_id_gsi_name
      sns_arn                  = var.notifications_topic.arn
    })
  }
  timeout_seconds = local.java_timeout_seconds
  memory_size     = local.java_lambda_memory_size
  runtime         = local.java_runtime
  tags            = {}
  plaintext_env_vars = {
    LOCK_DDB_TABLE                  = local.ingest_lock_dynamo_table_name
    LOCK_DDB_TABLE_GROUPID_GSI_NAME = local.ingest_lock_table_group_id_gsi_name
    OUTPUT_TOPIC_ARN                = var.notifications_topic.arn
  }
  lambda_invoke_permissions = {
    "events.amazonaws.com" = var.failed_ingest_step_function_event_bridge_rule_arn
  }
}
