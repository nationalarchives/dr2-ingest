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
  copy_source_bucket_name             = "tdr-export-${local.environment}"
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
}

module "adhoc_preingest" {
  source                              = "./preingest"
  environment                         = local.environment
  ingest_lock_dynamo_table_name       = local.ingest_lock_dynamo_table_name
  ingest_lock_table_arn               = module.ingest_lock_table.table_arn
  ingest_lock_table_group_id_gsi_name = local.ingest_lock_table_group_id_gsi_name
  ingest_raw_cache_bucket_name        = local.ingest_raw_cache_bucket_name
  ingest_step_function_name           = local.ingest_step_function_name
  source_name                         = "adhoc"
  copy_source_bucket_name             = local.ingest_raw_cache_bucket_name
}

