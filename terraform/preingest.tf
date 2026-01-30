module "tdr_preingest" {
  source                              = "./preingest"
  environment                         = local.environment
  ingest_lock_dynamo_table_name       = local.ingest_lock_dynamo_table_name
  ingest_lock_table_arn               = module.ingest_lock_table.table_arn
  ingest_lock_table_group_id_gsi_name = local.ingest_lock_table_group_id_gsi_name
  ingest_raw_cache_bucket_name        = local.ingest_raw_cache_bucket_name
  ingest_step_function_name           = local.ingest_step_function_name
  sns_topic_subscription = {
    topic_arn     = "arn:aws:sns:eu-west-2:${module.tdr_config.account_numbers[local.environment]}:tdr-external-notifications-${local.environment}"
    filter_policy = templatefile("${path.module}/templates/sns/bucket_filter_policy.json.tpl", { bucket = local.tdr_export_bucket })
  }
  source_name                = "tdr"
  bucket_kms_arn             = module.tdr_config.terraform_config["${local.environment}_s3_export_bucket_kms_key_arn"]
  copy_source_bucket_arn     = "arn:aws:s3:::${local.tdr_export_bucket}"
  private_security_group_ids = [module.outbound_https_access_for_s3.security_group_id, module.https_to_vpc_endpoints_security_group.security_group_id, module.outbound_https_access_for_dynamo_db.security_group_id]
  private_subnet_ids         = module.vpc.private_subnets
  vpc_id                     = module.vpc.vpc_id
  vpc_arn                    = module.vpc.vpc_arn
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
  copy_source_bucket_arn              = "arn:aws:s3:::${local.ingest_raw_cache_bucket_name}"
  private_security_group_ids          = [module.outbound_https_access_for_s3.security_group_id, module.https_to_vpc_endpoints_security_group.security_group_id, module.outbound_https_access_for_dynamo_db.security_group_id]
  private_subnet_ids                  = module.vpc.private_subnets
  vpc_id                              = module.vpc.vpc_id
  vpc_arn                    = module.vpc.vpc_arn
}

module "ad_hoc_preingest" {
  source                                       = "./preingest"
  environment                                  = local.environment
  ingest_lock_dynamo_table_name                = local.ingest_lock_dynamo_table_name
  ingest_lock_table_arn                        = module.ingest_lock_table.table_arn
  ingest_lock_table_group_id_gsi_name          = local.ingest_lock_table_group_id_gsi_name
  ingest_raw_cache_bucket_name                 = local.ingest_raw_cache_bucket_name
  ingest_step_function_name                    = local.ingest_step_function_name
  source_name                                  = "adhoc"
  copy_source_bucket_arn                       = "arn:aws:s3:::${local.adhoc_bucket_name}"
  private_security_group_ids                   = [module.outbound_https_access_for_s3.security_group_id, module.https_to_vpc_endpoints_security_group.security_group_id, module.outbound_https_access_for_dynamo_db.security_group_id]
  private_subnet_ids                           = module.vpc.private_subnets
  aggregator_secondary_grouping_window_seconds = 900
  vpc_id                                       = module.vpc.vpc_id
  vpc_arn                    = module.vpc.vpc_arn
}

module "court_document_preingest" {
  source                              = "./preingest"
  environment                         = local.environment
  ingest_lock_dynamo_table_name       = local.ingest_lock_dynamo_table_name
  ingest_lock_table_arn               = module.ingest_lock_table.table_arn
  ingest_lock_table_group_id_gsi_name = local.ingest_lock_table_group_id_gsi_name
  ingest_raw_cache_bucket_name        = local.ingest_raw_cache_bucket_name
  ingest_step_function_name           = local.ingest_step_function_name
  sns_topic_subscription = local.environment == "prod" ? {
    topic_arn     = local.tre_prod_event_bus,
    filter_policy = templatefile("${path.module}/templates/sns/tre_live_stream_filter_policy.json.tpl", {})
  } : null
  source_name                = "courtdoc"
  bucket_kms_arn             = module.tre_config.terraform_config["prod_s3_court_document_pack_out_kms_arn"]
  copy_source_bucket_arn     = local.environment == "prod" ? local.tre_terraform_prod_config["s3_court_document_pack_out_arn"] : "arn:aws:s3:::${local.ingest_parsed_court_document_event_handler_test_bucket_name}"
  private_security_group_ids = [module.outbound_https_access_for_s3.security_group_id, module.https_to_vpc_endpoints_security_group.security_group_id, module.outbound_https_access_for_dynamo_db.security_group_id]
  private_subnet_ids         = module.vpc.private_subnets
  importer_lambda = {
    visibility_timeout = 900
    timeout            = 900
    handler            = "uk.gov.nationalarchives.preingestcourtdocimporter.Lambda::handleRequest"
    runtime            = local.java_runtime
    memory_size        = 512
  }
  package_builder_lambda = {
    handler = "uk.gov.nationalarchives.preingestcourtdocpackagebuilder.Lambda::handleRequest"
  }
  vpc_id = module.vpc.vpc_id
}