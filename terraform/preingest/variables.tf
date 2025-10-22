variable "environment" {}

variable "ingest_step_function_name" {}

variable "source_name" {}

variable "sns_topic_arn" {
  default = null
}

variable "ingest_lock_table_arn" {}

variable "ingest_lock_dynamo_table_name" {}

variable "ingest_lock_table_group_id_gsi_name" {}

variable "ingest_raw_cache_bucket_name" {}

variable "bucket_kms_arn" {
  default = null
}

variable "copy_source_bucket_name" {}

variable "deploy_version" {}