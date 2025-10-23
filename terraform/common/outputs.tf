output "discovery_inbound_security_group_id" {
  value = module.discovery_inbound_https.security_group_id
}

output "files_table_arn" {
  value = "arn:aws:dynamodb:eu-west-2:${data.aws_caller_identity.current.account_id}:table/${local.files_dynamo_table_name}"
}

output "files_table_batch_parent_global_secondary_index_name" {
  value = local.files_table_batch_parent_global_secondary_index_name
}

output "flow_control_config" {
  value = aws_ssm_parameter.flow_control_config
}

output "ingest_lock_table_arn" {
  value = "arn:aws:dynamodb:eu-west-2:${data.aws_caller_identity.current.account_id}:table/${local.ingest_lock_dynamo_table_name}"
}

output "ingest_queue_table_arn" {
  value = "arn:aws:dynamodb:eu-west-2:${data.aws_caller_identity.current.account_id}:table/${local.ingest_queue_dynamo_table_name}"
}

output "raw_cache_bucket_name" {
  value = local.ingest_raw_cache_bucket_name
}

output "notifications_sns" {
  value = module.dr2_notifications_sns.sns
}

output "outbound_cloudflare_https_access_security_group_id" {
  value = module.outbound_cloudflare_https_access.security_group_id
}

output "outbound_https_access_only_security_group_id" {
  value = module.outbound_cloudflare_https_access.security_group_id
}

output "read_metadata_secret" {
  value = aws_secretsmanager_secret.preservica_read_metadata
}

output "read_update_metadata_insert_content_secret" {
  value = aws_secretsmanager_secret.preservica_read_update_metadata_insert_content
}

output "preservica_secret" {
  value = aws_secretsmanager_secret.preservica_secret
}

output "private_subnets" {
  value = module.vpc.private_subnets
}

output "external_notification_log_group_arn" {
  value = aws_cloudwatch_log_group.external_notification_log_group.arn
}

output "failed_ingest_step_function_event_bridge_rule_arn" {
  value = "arn:aws:events:eu-west-2:${data.aws_caller_identity.current.account_id}:rule/${local.step_function_failure_eventbridge_rule}"
}