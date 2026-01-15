locals {
  pa_source_bucket = "pa-migration-files-bucket"
}
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
  private_security_group_ids          = [module.outbound_https_access_for_s3.security_group_id, module.https_to_vpc_endpoints_security_group.security_group_id, module.outbound_https_access_for_dynamo_db.security_group_id]
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
  private_security_group_ids          = [module.outbound_https_access_for_s3.security_group_id, module.https_to_vpc_endpoints_security_group.security_group_id, module.outbound_https_access_for_dynamo_db.security_group_id]
  private_subnet_ids                  = module.vpc.private_subnets
}

module "ad_hoc_preingest" {
  source                              = "./preingest"
  environment                         = local.environment
  ingest_lock_dynamo_table_name       = local.ingest_lock_dynamo_table_name
  ingest_lock_table_arn               = module.ingest_lock_table.table_arn
  ingest_lock_table_group_id_gsi_name = local.ingest_lock_table_group_id_gsi_name
  ingest_raw_cache_bucket_name        = local.ingest_raw_cache_bucket_name
  ingest_step_function_name           = local.ingest_step_function_name
  source_name                         = "adhoc"
  copy_source_bucket_name             = local.adhoc_bucket_name
  private_security_group_ids          = [module.outbound_https_access_for_s3.security_group_id, module.https_to_vpc_endpoints_security_group.security_group_id, module.outbound_https_access_for_dynamo_db.security_group_id]
  private_subnet_ids                  = module.vpc.private_subnets
}

// Subnets and security groups aren't specified as we don't want this lambda in the VPC
// The PA bucket is in a different region which we can't access through the gateway endpoint.
module "pa_preingest" {
  source                              = "./preingest"
  environment                         = local.environment
  ingest_lock_dynamo_table_name       = local.ingest_lock_dynamo_table_name
  ingest_lock_table_arn               = module.ingest_lock_table.table_arn
  ingest_lock_table_group_id_gsi_name = local.ingest_lock_table_group_id_gsi_name
  ingest_raw_cache_bucket_name        = local.ingest_raw_cache_bucket_name
  ingest_step_function_name           = local.ingest_step_function_name
  source_name                         = "pa"
  copy_source_bucket_name             = local.pa_source_bucket
  additional_importer_lambda_policies = {
    "${local.environment}-dr2-preingest-pa-importer-assume-role" = templatefile("${path.module}/templates/iam_policy/preingest_pa_importer_additional_permissions.json.tpl", {
      pa_migration_role = local.parliament_ingest_role
    })
  }
  additional_importer_lambda_env_vars = {
    ROLE_TO_ASSUME = local.parliament_ingest_role
    FILES_BUCKET   = local.pa_source_bucket
  }
  importer_lambda = {
    visibility_timeout = 900
    timeout            = 900
    handler            = "uk.gov.nationalarchives.preingestpaimporter.Lambda::handleRequest"
    runtime            = local.java_runtime
    memory_size        = 2048
  }
}

module "court_document_preingest" {
  source                              = "./preingest"
  environment                         = local.environment
  ingest_lock_dynamo_table_name       = local.ingest_lock_dynamo_table_name
  ingest_lock_table_arn               = module.ingest_lock_table.table_arn
  ingest_lock_table_group_id_gsi_name = local.ingest_lock_table_group_id_gsi_name
  ingest_raw_cache_bucket_name        = local.ingest_raw_cache_bucket_name
  ingest_step_function_name           = local.ingest_step_function_name
  sns_topic_arn                       = local.environment == "prod" ? local.tre_prod_event_bus : null
  source_name                         = "courtdoc"
  bucket_kms_arn                      = module.tdr_config.terraform_config["${local.environment}_s3_export_bucket_kms_key_arn"]
  copy_source_bucket_name             = local.environment == "prod" ? local.tre_terraform_prod_config["s3_court_document_pack_out_arn"] : local.ingest_parsed_court_document_event_handler_test_bucket_name
  private_security_group_ids          = [module.outbound_https_access_for_s3.security_group_id, module.https_to_vpc_endpoints_security_group.security_group_id]
  private_subnet_ids                  = module.vpc.private_subnets
  importer_lambda = {
    visibility_timeout = 900
    timeout            = 900
    handler            = "uk.gov.nationalarchives.preingestcourtdocimporter.Lambda::handleRequest"
    runtime            = local.java_runtime
    memory_size        = 2048
  }
  package_builder_lambda = {
    handler = "uk.gov.nationalarchives.preingestcourtdocpackagebuilder.Lambda::handleRequest"
  }
}