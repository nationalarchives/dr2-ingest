locals {
  messages_visible_threshold = 1000000
  sse_encryption             = "sse"
}

variable "kms_key_arn" {}

data "aws_ssm_parameter" "environments" {
  name = "/${local.environment}/environments"
}

module "ingest" {
  source   = "./ingest"
  for_each = local.environments
  preingest_lambda_names = [
    module.tdr_preingest[each.key].aggregator_lambda.function_name,
    module.tdr_preingest[each.key].package_builder_lambda.function_name,
    module.tdr_preingest[each.key].importer_lambda.function_name,
    module.dri_preingest[each.key].aggregator_lambda.function_name,
    module.dri_preingest[each.key].package_builder_lambda.function_name,
    module.dri_preingest[each.key].importer_lambda.function_name
  ]
  custodial_copy = {
    custodial_copy_builder_queue       = module.dr2_custodial_copy_db_builder_queue
    custodial_copy_queue               = module.dr2_custodial_copy_queue
    custodial_copy_queue_creator_queue = module.dr2_custodial_copy_queue_creator_queue
  }
  postingest = {
    table_name                                  = module.postingest[each.key].postingest_table_name
    cc_confirmer_queue_oldest_message_alarm_arn = module.postingest[each.key].cc_confirmer_queue_oldest_message_alarm_arn
  }
  secrets = {
    preservica_secret                              = aws_secretsmanager_secret.preservica_secret
    preservica_read_metadata                       = aws_secretsmanager_secret.preservica_read_metadata
    preservica_read_metadata_read_content          = aws_secretsmanager_secret.preservica_read_metadata_read_content
    preservica_read_update_metadata_insert_content = aws_secretsmanager_secret.preservica_read_update_metadata_insert_content
    demo_preservica_secret                         = aws_secretsmanager_secret.demo_preservica_secret
  }
  security_groups = {
    outbound_https_access_only_security_group_id       = module.outbound_https_access_only.security_group_id
    outbound_cloudflare_https_access_security_group_id = module.outbound_cloudflare_https_access.security_group_id
  }
  ingest_lock_table = {
    name              = local.ingest_lock_dynamo_table_name
    arn               = module.ingest_lock_table.table_arn
    group_id_gsi_name = local.ingest_lock_table_group_id_gsi_name
    hash_key          = local.ingest_lock_table_hash_key
  }
  preingest = {
    tdr = {
      sfn_arn              = module.tdr_preingest[each.key].preingest_sfn_arn
      importer_sqs_arn     = module.tdr_preingest[each.key].importer_sqs.sqs_arn
      aggregator_sqs_arn   = module.tdr_preingest[each.key].aggregator_sqs.sqs_arn
      importer_lambda_name = module.tdr_preingest[each.key].importer_lambda.function_name
    }
    dri = {
      sfn_arn              = module.dri_preingest[each.key].preingest_sfn_arn
      importer_sqs_arn     = module.dri_preingest[each.key].importer_sqs.sqs_arn
      aggregator_sqs_arn   = module.dri_preingest[each.key].aggregator_sqs.sqs_arn
      importer_lambda_name = module.dri_preingest[each.key].importer_lambda.function_name
    }
  }
  kms_key_arn     = var.kms_key_arn
  private_subnets = module.vpc.private_subnets
  bucket_names = {
    ingest_parsed_court_document_event_handler_test_bucket_name = local.ingest_parsed_court_document_event_handler_test_bucket_name
    ingest_state_bucket_name                                    = local.ingest_state_bucket_name
    ingest_raw_cache_bucket_name                                = local.ingest_raw_cache_bucket_name
  }
  external_notifications = {
    queue = {
      event_alarms = module.dr2_external_notifications_queue.event_alarms,
      alarms       = module.dr2_external_notifications_queue.alarms
    }
    log_group_arn = aws_cloudwatch_log_group.external_notification_log_group.arn
  }
  notifications_topic = {
    name = local.notifications_topic_name
    arn  = module.dr2_notifications_sns.sns_arn
  }
  entity_event_lambda_names = [local.entity_event_lambda_name, local.ingest_queue_creator_name]
}