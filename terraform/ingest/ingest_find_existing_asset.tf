module "ingest_find_existing_asset" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda?ref=DR2-2511-do-not-ignore-filename-if-set"
  function_name   = var.lambda_names.find_existing_asset
  s3_bucket       = local.code_deploy_bucket
  s3_key          = replace("${var.deploy_version}/${var.lambda_names.find_existing_asset}", "${local.environment}-dr2-", "")
  handler         = "uk.gov.nationalarchives.ingestfindexistingasset.Lambda::handleRequest"
  timeout_seconds = 60
  policies = {
    "${var.lambda_names.find_existing_asset}-policy" = templatefile(
      "${path.root}/templates/iam_policy/ingest_find_existing_asset_policy.json.tpl", {
        account_id                 = data.aws_caller_identity.current.account_id
        lambda_name                = var.lambda_names.find_existing_asset
        dynamo_db_file_table_arn   = var.files_table_arn
        secrets_manager_secret_arn = var.preservica_read_metadata_secret.arn
      }
    )
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    FILES_DDB_TABLE        = local.files_dynamo_table_name
    PRESERVICA_SECRET_NAME = var.preservica_read_metadata_secret.name
  }
  vpc_config = {
    subnet_ids         = var.private_subnets
    security_group_ids = local.outbound_security_group_ids
  }
  tags = {
    Name = var.lambda_names.find_existing_asset
  }
}
