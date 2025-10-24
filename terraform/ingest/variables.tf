variable "environment" {}

variable "outbound_https_access_only_id" {}

variable "outbound_cloudflare_https_access_id" {}

variable "notifications_topic" {}

variable "ingest_lock_table_arn" {}

variable "ingest_raw_cache_bucket_name" {}

variable "files_table_arn" {}

variable "files_table_gsi_name" {}

variable "preservica_read_metadata_secret" {}

variable "preservica_secret" {}

variable "preservica_read_update_metadata_insert_content" {}

variable "private_subnets" {}

variable "ingest_queue_table_arn" {}

variable "flow_control_config" {}

variable "discovery_security_group_id" {}

variable "external_notification_log_group_arn" {}

variable "failed_ingest_step_function_event_bridge_rule_arn" {}

variable "deploy_version" {}

variable "step_function_names" {
  type = object({
    ingest = string
    preingest = object({
      tdr = string
      dri = string
    })
  })
}

variable "table_names" {
  type = object({
    postingest = string
  })
}

variable "lambda_names" {
  type = object({
    ingest_asset_opex_creator  = string
    find_existing_asset        = string
    validate_ingest_inputs     = string
    ingest_reconciler          = string
    folder_opex_creator        = string
    parent_folder_opex_creator = string
    start_workflow             = string
    workflow_monitor           = string
    preingest = object({
      tdr = object({
        importer        = string
        aggregator      = string
        package_builder = string
      })
      dri = object({
        importer        = string
        aggregator      = string
        package_builder = string
      })
    })
    ingest_mapper          = string
    court_document_handler = string
    upsert_folders         = string
    failed_notification    = string
  })
}