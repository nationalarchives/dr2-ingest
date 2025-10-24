locals {
  ingest_find_existing_asset_name = "${local.environment}-dr2-ingest-find-existing-asset"
}

module "ingest_find_existing_asset" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.ingest_find_existing_asset_name
  handler         = "uk.gov.nationalarchives.ingestfindexistingasset.Lambda::handleRequest"
  timeout_seconds = 60
  policies = {
    "${local.ingest_find_existing_asset_name}-policy" = templatefile(
      "${path.root}/templates/iam_policy/ingest_find_existing_asset_policy.json.tpl", {
        account_id                 = data.aws_caller_identity.current.account_id
        lambda_name                = local.ingest_find_existing_asset_name
        dynamo_db_file_table_arn   = module.files_table.table_arn
        secrets_manager_secret_arn = var.secrets.preservica_read_metadata.arn
      }
    )
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    FILES_DDB_TABLE        = local.files_dynamo_table_name
    PRESERVICA_SECRET_NAME = var.secrets.preservica_read_metadata.name
  }
  vpc_config = {
    subnet_ids         = var.private_subnets
    security_group_ids = local.outbound_security_group_ids
  }
  tags = {
    Name = local.ingest_find_existing_asset_name
  }
}
