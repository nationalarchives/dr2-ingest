module "tdr_preingest" {
  source                              = "./preingest"
  environment                         = local.environment
  ingest_lock_dynamo_table_name       = local.ingest_lock_dynamo_table_name
  ingest_lock_table_arn               = module.ingest_lock_table.table_arn
  ingest_lock_table_group_id_gsi_name = local.ingest_lock_table_group_id_gsi_name
  ingest_raw_cache_bucket_name        = local.ingest_raw_cache_bucket_name
  ingest_step_function_name           = module.common.step_function_names.ingest
  sns_topic_arn                       = "arn:aws:sns:eu-west-2:${module.tdr_config.account_numbers[local.environment]}:tdr-external-notifications-${local.environment}"
  source_name                         = "tdr"
  bucket_kms_arn                      = module.tdr_config.terraform_config["${local.environment}_s3_export_bucket_kms_key_arn"]
  copy_source_bucket_name             = "tdr-export-${local.environment}"
  deploy_version                      = var.deploy_version
  lambda_names                        = module.common.lambda_names.preingest.tdr
  step_function_names = module.common.step_function_names
}

module "dri_preingest" {
  source                              = "./preingest"
  environment                         = local.environment
  ingest_lock_dynamo_table_name       = local.ingest_lock_dynamo_table_name
  ingest_lock_table_arn               = module.ingest_lock_table.table_arn
  ingest_lock_table_group_id_gsi_name = local.ingest_lock_table_group_id_gsi_name
  ingest_raw_cache_bucket_name        = local.ingest_raw_cache_bucket_name
  ingest_step_function_name           = module.common.step_function_names.ingest
  source_name                         = "dri"
  copy_source_bucket_name             = local.ingest_raw_cache_bucket_name
  deploy_version                      = var.deploy_version
  lambda_names                        = module.common.lambda_names.preingest.dri
  step_function_names = module.common.step_function_names
}

