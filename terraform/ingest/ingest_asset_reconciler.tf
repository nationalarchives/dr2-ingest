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
      account_id                 = data.aws_caller_identity.current.account_id
      lambda_name                = local.ingest_asset_reconciler_lambda_name
      dynamo_db_file_table_arn   = module.files_table.table_arn
      gsi_name                   = local.files_table_batch_parent_global_secondary_index_name
      dynamo_db_lock_table_arn   = var.ingest_lock_table.arn
      secrets_manager_secret_arn = var.secrets.preservica_read_metadata.arn
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    FILES_DDB_TABLE                      = local.files_dynamo_table_name
    FILES_DDB_TABLE_BATCHPARENT_GSI_NAME = local.files_table_batch_parent_global_secondary_index_name
    LOCK_DDB_TABLE                       = var.ingest_lock_table.name
    PRESERVICA_SECRET_NAME               = var.secrets.preservica_read_metadata.name
  }
  vpc_config = {
    subnet_ids         = var.private_subnets
    security_group_ids = local.outbound_security_group_ids
  }
  tags = {
    Name = local.ingest_asset_reconciler_lambda_name
  }
}

