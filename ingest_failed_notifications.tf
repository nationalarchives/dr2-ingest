locals {
  ingest_failure_notifications_lambda_name = "${local.environment}-dr2-ingest-failure-notifications"
}

module "dr2_ingest_failure_notifications_lambda" {
  source        = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name = local.ingest_failure_notifications_lambda_name
  handler       = "uk.gov.nationalarchives.ingestfailurenotifications.Lambda::handleRequest"
  policies = {
    dr2_ingest_failure_notifications_policy = templatefile("${path.module}/templates/iam_policy/failure_notifications_policy.json.tpl", {
      account_id               = data.aws_caller_identity.current.account_id
      lambda_name              = local.ingest_failure_notifications_lambda_name
      dynamo_db_file_table_arn = module.ingest_lock_table.table_arn
      gsi_name                 = local.ingest_lock_table_group_id_gsi_name
      sns_arn                  = module.dr2_notifications_sns.sns_arn
    })
  }
  timeout_seconds = local.java_timeout_seconds
  memory_size     = local.java_lambda_memory_size
  runtime         = local.java_runtime
  tags            = {}
  plaintext_env_vars = {
    DDB_LOCK_TABLE                  = local.ingest_lock_dynamo_table_name       # Remove in DR2-1626/2
    LOCK_DDB_TABLE_GROUP_ID_IDX     = local.ingest_lock_table_group_id_gsi_name # Remove in DR2-1626/2
    TOPIC_ARN                       = module.dr2_notifications_sns.sns_arn      # Remove in DR2-1626/2
    LOCK_DDB_TABLE                  = local.ingest_lock_dynamo_table_name
    LOCK_DDB_TABLE_GROUPID_GSI_NAME = local.ingest_lock_table_group_id_gsi_name
    OUTPUT_TOPIC_ARN                = module.dr2_notifications_sns.sns_arn
  }
  lambda_invoke_permissions = {
    "events.amazonaws.com" = module.failed_ingest_step_function_event_bridge_rule.rule_arn
  }
}
