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