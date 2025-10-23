module "common" {
  source         = "./common"
  deploy_version = var.deploy_version
  environment    = startswith(local.environment, "DR2-") ? "intg" : local.environment
  ingest         = module.ingest
}

module "ingest" {
  source                                            = "./ingest"
  discovery_security_group_id                       = module.common.discovery_inbound_security_group_id
  environment                                       = local.environment
  files_table_arn                                   = module.common.files_table_arn
  files_table_gsi_name                              = module.common.files_table_batch_parent_global_secondary_index_name
  flow_control_config                               = module.common.flow_control_config
  ingest_lock_table_arn                             = module.common.ingest_lock_table_arn
  ingest_queue_table_arn                            = module.common.ingest_queue_table_arn
  ingest_raw_cache_bucket_name                      = module.common.raw_cache_bucket_name
  notifications_topic                               = module.common.notifications_sns
  outbound_cloudflare_https_access_id               = module.common.outbound_cloudflare_https_access_security_group_id
  outbound_https_access_only_id                     = module.common.outbound_https_access_only_security_group_id
  preservica_read_metadata_secret                   = module.common.read_metadata_secret
  preservica_read_update_metadata_insert_content    = module.common.read_update_metadata_insert_content_secret
  preservica_secret                                 = module.common.preservica_secret
  private_subnets                                   = module.common.private_subnets
  external_notification_log_group_arn               = module.common.external_notification_log_group_arn
  failed_ingest_step_function_event_bridge_rule_arn = module.common.failed_ingest_step_function_event_bridge_rule_arn
  deploy_version                                    = var.deploy_version
}