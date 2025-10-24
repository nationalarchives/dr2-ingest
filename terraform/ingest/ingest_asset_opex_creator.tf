module "dr2_ingest_asset_opex_creator_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda?ref=DR2-2511-do-not-ignore-filename-if-set"
  function_name   = var.lambda_names.ingest_asset_opex_creator
  s3_bucket       = local.code_deploy_bucket
  s3_key          = replace("${var.deploy_version}/${var.lambda_names.ingest_asset_opex_creator}", "${local.environment}-dr2-", "")
  handler         = "uk.gov.nationalarchives.ingestassetopexcreator.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${var.lambda_names.ingest_asset_opex_creator}-policy" = templatefile("./templates/iam_policy/ingest_asset_opex_creator_policy.json.tpl", {
      account_id                  = data.aws_caller_identity.current.account_id
      source_bucket_name          = var.ingest_raw_cache_bucket_name
      destination_bucket_name     = local.preservica_ingest_bucket
      account_id                  = data.aws_caller_identity.current.account_id
      lambda_name                 = var.lambda_names.ingest_asset_opex_creator
      dynamo_db_file_table_arn    = var.files_table_arn
      gsi_name                    = var.files_table_gsi_name
      copy_to_preservica_role_arn = module.copy_tna_to_preservica_role.role_arn

    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    FILES_DDB_TABLE                      = local.files_dynamo_table_name
    FILES_DDB_TABLE_BATCHPARENT_GSI_NAME = var.files_table_gsi_name
    OUTPUT_BUCKET_NAME                   = local.preservica_ingest_bucket
    S3_ROLE_ARN                          = module.copy_tna_to_preservica_role.role_arn

  }
  tags = {
    Name = var.lambda_names.ingest_asset_opex_creator
  }
}
