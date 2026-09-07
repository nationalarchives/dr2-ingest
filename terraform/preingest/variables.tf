variable "environment" {}

variable "ingest_step_function_name" {}

variable "source_name" {
  validation {
    condition     = var.source_name == lower(var.source_name)
    error_message = "The source_name should be lowercased"
  }
}

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

variable "aggregator_lambda" {
  type = object({
    timeout = number
  })
  default = {
    timeout = 60
  }
}

variable "importer_lambda" {
  type = object({
    timeout            = number
    visibility_timeout = number
    handler            = string
    runtime            = string
    architecture       = string
    memory_size        = number
  })
  default = {
    timeout            = 180
    visibility_timeout = 300
    handler            = "lambda_function.lambda_handler"
    runtime            = "python3.12"
    architecture       = "x86_64"
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

variable "vpc_arn" {}

variable "delete_from_source" {
  type        = bool
  description = "Whether to delete the files and metadata from the source bucket"
  default     = false
}

variable "lambda_code_version" {}

variable "notifications_topic_arn" {}

variable "code_deploy_bucket" {}

variable "slack_api_destination_arn" {}

variable "general_notifications_channel_id" {}