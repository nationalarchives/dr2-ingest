module "tdr_preingest" {
  source                              = "./preingest"
  environment                         = local.environment
  ingest_lock_dynamo_table_name       = local.ingest_lock_dynamo_table_name
  ingest_lock_table_arn               = module.ingest_lock_table.table_arn
  ingest_lock_table_group_id_gsi_name = local.ingest_lock_table_group_id_gsi_name
  ingest_raw_cache_bucket_name        = local.ingest_raw_cache_bucket_name
  ingest_step_function_name           = local.ingest_step_function_name
  sns_topic_arn                       = "arn:aws:sns:eu-west-2:${module.tdr_config.account_numbers[local.environment]}:tdr-external-notifications-${local.environment}"
  source_name                         = "tdr"
  bucket_kms_arn                      = module.tdr_config.terraform_config["${local.environment}_s3_export_bucket_kms_key_arn"]
  copy_source_bucket_name             = local.tdr_export_bucket
  private_security_group_ids          = [module.outbound_https_access_only.security_group_id, module.outbound_https_access_for_s3.security_group_id]
  private_subnet_ids                  = module.vpc.private_subnets
}

module "dri_preingest" {
  source                              = "./preingest"
  environment                         = local.environment
  ingest_lock_dynamo_table_name       = local.ingest_lock_dynamo_table_name
  ingest_lock_table_arn               = module.ingest_lock_table.table_arn
  ingest_lock_table_group_id_gsi_name = local.ingest_lock_table_group_id_gsi_name
  ingest_raw_cache_bucket_name        = local.ingest_raw_cache_bucket_name
  ingest_step_function_name           = local.ingest_step_function_name
  source_name                         = "dri"
  copy_source_bucket_name             = local.ingest_raw_cache_bucket_name
  private_security_group_ids          = [module.outbound_https_access_only.security_group_id, module.outbound_https_access_for_s3.security_group_id]
  private_subnet_ids                  = module.vpc.private_subnets
}

module "pa_preingest" {
  source                              = "./preingest"
  environment                         = local.environment
  ingest_lock_dynamo_table_name       = local.ingest_lock_dynamo_table_name
  ingest_lock_table_arn               = module.ingest_lock_table.table_arn
  ingest_lock_table_group_id_gsi_name = local.ingest_lock_table_group_id_gsi_name
  ingest_raw_cache_bucket_name        = local.ingest_raw_cache_bucket_name
  ingest_step_function_name           = local.ingest_step_function_name
  source_name                         = "pa"
  copy_source_bucket_name             = "hop-production"
  additional_importer_lambda_policies = {
    "${local.environment}-dr2-preingest-pa-importer-assume-role" = templatefile("${path.module}/templates/iam_policy/preingest_pa_importer_additional_permissions.json.tpl", {
      pa_migration_role = module.config.terraform_config["parliament_files_role"]
    })
  }
  additional_importer_lambda_env_vars = {
    ROLE_TO_ASSUME = module.config.terraform_config["parliament_files_role"]
    FILES_BUCKET   = "hop-production"
  }
}

