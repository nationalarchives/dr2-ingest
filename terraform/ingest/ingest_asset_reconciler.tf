module "dr2_ingest_asset_reconciler_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda?ref=DR2-2511-do-not-ignore-filename-if-set"
  function_name   = var.lambda_names.ingest_reconciler
  s3_bucket       = local.code_deploy_bucket
  s3_key          = replace("${var.deploy_version}/${var.lambda_names.ingest_reconciler}", "${local.environment}-dr2-", "")
  handler         = "uk.gov.nationalarchives.ingestassetreconciler.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${var.lambda_names.ingest_reconciler}-policy" = templatefile("./templates/iam_policy/ingest_asset_reconciler_policy.json.tpl", {
      account_id                 = data.aws_caller_identity.current.account_id
      lambda_name                = var.lambda_names.ingest_reconciler
      dynamo_db_file_table_arn   = var.files_table_arn
      gsi_name                   = var.files_table_gsi_name
      dynamo_db_lock_table_arn   = var.ingest_lock_table_arn
      secrets_manager_secret_arn = var.preservica_read_metadata_secret.arn
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    FILES_DDB_TABLE                      = local.files_dynamo_table_name
    FILES_DDB_TABLE_BATCHPARENT_GSI_NAME = var.files_table_gsi_name
    LOCK_DDB_TABLE                       = local.ingest_lock_dynamo_table_name
    PRESERVICA_SECRET_NAME               = var.preservica_read_metadata_secret.name
  }
  vpc_config = {
    subnet_ids         = var.private_subnets
    security_group_ids = local.outbound_security_group_ids
  }
  tags = {
    Name = var.lambda_names.ingest_reconciler
  }
}

