variable "environment" {}

variable "ingest_step_function_name" {}

variable "source_name" {}

variable "sns_topic_subscription" {
  type = object({
    topic_arn     = string
    filter_policy = string
  })
  default = null
}

variable "ingest_lock_table_arn" {}

variable "ingest_lock_dynamo_table_name" {}

variable "ingest_lock_table_group_id_gsi_name" {}

variable "ingest_raw_cache_bucket_name" {}

variable "bucket_kms_arn" {
  default = null
}

variable "copy_source_bucket_arn" {}

variable "additional_importer_lambda_policies" {
  default = {}
}

variable "additional_importer_lambda_env_vars" {
  default = {}
}

variable "private_security_group_ids" {
  default = []
}

variable "private_subnet_ids" {
  default = []
}

variable "importer_lambda" {
  type = object({
    timeout            = number
    visibility_timeout = number
    handler            = string
    runtime            = string
    memory_size        = number
  })
  default = {
    timeout            = 180
    visibility_timeout = 300
    handler            = "lambda_function.lambda_handler"
    runtime            = "python3.12"
    memory_size        = 128

  }
}

variable "package_builder_lambda" {
  type = object({
    handler = string
  })
  default = {
    handler = "uk.gov.nationalarchives.preingesttdrpackagebuilder.Lambda::handleRequest"
  }
}

variable "aggregator_primary_grouping_window_seconds" {
  default = 300
}


variable "aggregator_secondary_grouping_window_seconds" {
  default     = 180
  description = "Additional time we wait before starting preingest to allow multiple invocations to form a single group, this is added to the aggregator_lambda_timeout_seconds when we start a group."
}

variable "vpc_id" {}