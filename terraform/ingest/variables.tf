variable "preingest_lambda_names" {}

variable "entity_event_lambda_names" {}

variable "security_groups" {
  type = object({
    outbound_https_access_only_security_group_id       = string
    outbound_cloudflare_https_access_security_group_id = string
  })
}

variable "custodial_copy" {
  type = object({
    custodial_copy_queue = object({
      sqs_queue_url = string
      event_alarms  = any
      alarms        = any
    })
    custodial_copy_builder_queue = object({
      sqs_queue_url = string
      event_alarms  = any
      alarms        = any
    })
    custodial_copy_queue_creator_queue = object({
      sqs_queue_url = string
      event_alarms  = any
      alarms        = any
    })
  })
}

variable "secrets" {
  type = object({
    preservica_secret = object({
      id   = string
      arn  = string
      name = string
    })
    preservica_read_metadata_read_content = object({
      id   = string
      arn  = string
      name = string
    })
    preservica_read_metadata = object({
      id   = string
      arn  = string
      name = string
    })
    preservica_read_update_metadata_insert_content = object({
      id   = string
      arn  = string
      name = string
    })
    demo_preservica_secret = object({
      id   = string
      arn  = string
      name = string
    })
  })
}

variable "bucket_names" {
  type = object({
    ingest_raw_cache_bucket_name                                = string
    ingest_state_bucket_name                                    = string
    ingest_parsed_court_document_event_handler_test_bucket_name = string
  })
}

variable "postingest" {
  type = object({
    table_name                                  = string
    cc_confirmer_queue_oldest_message_alarm_arn = string
  })
}
variable "ingest_lock_table" {
  type = object({
    name              = string
    arn               = string
    group_id_gsi_name = string
    hash_key          = string
  })
}

variable "kms_key_arn" {}

variable "preingest" {
  type = object({
    tdr = object({
      sfn_arn              = string
      importer_sqs_arn     = string
      aggregator_sqs_arn   = string
      importer_lambda_name = string
    })
    dri = object({
      sfn_arn              = string
      importer_sqs_arn     = string
      aggregator_sqs_arn   = string
      importer_lambda_name = string
    })
  })
}

variable "private_subnets" {}

variable "notifications_topic" {
  type = object({
    name = string
    arn  = string
  })
}

variable "external_notifications" {
  type = object({
    queue = object({
      event_alarms = any
      alarms       = any
    })
    log_group_arn = string
  })
}