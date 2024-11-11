locals {
  ingest_asset_reconciler_lambda_name = "${local.environment}-dr2-ingest-asset-reconciler"
}

module "dr2_ingest_asset_reconciler_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.ingest_asset_reconciler_lambda_name
  handler         = "uk.gov.nationalarchives.ingestassetreconciler.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${local.ingest_asset_reconciler_lambda_name}-policy" = templatefile("./templates/iam_policy/ingest_asset_reconciler_policy.json.tpl", {
      account_id                 = var.account_number
      lambda_name                = local.ingest_asset_reconciler_lambda_name
      dynamo_db_file_table_arn   = module.files_table.table_arn
      gsi_name                   = local.files_table_batch_parent_global_secondary_index_name
      dynamo_db_lock_table_arn   = module.ingest_lock_table.table_arn
      secrets_manager_secret_arn = aws_secretsmanager_secret.preservica_read_metadata.arn
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    FILES_DDB_TABLE                      = local.files_dynamo_table_name
    FILES_DDB_TABLE_BATCHPARENT_GSI_NAME = local.files_table_batch_parent_global_secondary_index_name
    LOCK_DDB_TABLE                       = local.ingest_lock_dynamo_table_name
    PRESERVICA_API_URL                   = data.aws_ssm_parameter.preservica_url.value
    PRESERVICA_SECRET_NAME               = aws_secretsmanager_secret.preservica_read_metadata.name
  }
  vpc_config = {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [module.outbound_https_access_only.security_group_id]
  }
  tags = {
    Name = local.ingest_asset_reconciler_lambda_name
  }
}

