locals {
  ingest_folder_opex_creator_lambda_name = "${local.environment}-dr2-ingest-folder-opex-creator"
}

module "dr2_ingest_folder_opex_creator_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.ingest_folder_opex_creator_lambda_name
  handler         = "uk.gov.nationalarchives.ingestfolderopexcreator.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${local.ingest_folder_opex_creator_lambda_name}-policy" = templatefile("./templates/iam_policy/ingest_folder_opex_creator_policy.json.tpl", {
      account_id                  = data.aws_caller_identity.current.account_id
      bucket_name                 = local.preservica_ingest_bucket
      account_id                  = data.aws_caller_identity.current.account_id
      lambda_name                 = local.ingest_folder_opex_creator_lambda_name
      dynamo_db_file_table_arn    = module.files_table.table_arn
      gsi_name                    = local.files_table_batch_parent_global_secondary_index_name
      copy_to_preservica_role_arn = module.copy_tna_to_preservica_role.role_arn
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    FILES_DDB_TABLE                      = local.files_dynamo_table_name
    FILES_DDB_TABLE_BATCHPARENT_GSI_NAME = local.files_table_batch_parent_global_secondary_index_name
    OUTPUT_BUCKET_NAME                   = local.preservica_ingest_bucket
    S3_ROLE_ARN                          = module.copy_tna_to_preservica_role.role_arn
  }
  vpc_config = {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [module.outbound_https_access_only.security_group_id, module.outbound_https_access_for_S3.security_group_id]
  }
  tags = {
    Name = local.ingest_folder_opex_creator_lambda_name
  }
}
