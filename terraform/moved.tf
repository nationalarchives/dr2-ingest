moved {
  from = module.copy_from_tre_bucket_policy[0]
  to   = module.ingest.module.copy_from_tre_bucket_policy[0]
}

moved {
  from = module.copy_from_tre_bucket_role[0]
  to   = module.ingest.module.copy_from_tre_bucket_role[0]
}

moved {
  from = module.copy_tna_to_preservica_policy
  to   = module.ingest.module.copy_tna_to_preservica_policy
}

moved {
  from = module.copy_tna_to_preservica_role
  to   = module.ingest.module.copy_tna_to_preservica_role
}

moved {
  from = module.dr2_e2e_tests_policy[0]
  to   = module.ingest.module.dr2_e2e_tests_policy[0]
}

moved {
  from = module.dr2_ingest_asset_opex_creator_lambda
  to   = module.ingest.module.dr2_ingest_asset_opex_creator_lambda
}

moved {
  from = module.dr2_ingest_asset_reconciler_lambda
  to   = module.ingest.module.dr2_ingest_asset_reconciler_lambda
}

moved {
  from = module.dr2_ingest_failure_notifications_lambda
  to   = module.ingest.module.dr2_ingest_failure_notifications_lambda
}

moved {
  from = module.dr2_ingest_flow_control_lambda
  to   = module.ingest.module.dr2_ingest_flow_control_lambda
}

moved {
  from = module.dr2_ingest_folder_opex_creator_lambda
  to   = module.ingest.module.dr2_ingest_folder_opex_creator_lambda
}

moved {
  from = module.dr2_ingest_mapper_lambda
  to   = module.ingest.module.dr2_ingest_mapper_lambda
}

moved {
  from = module.dr2_ingest_parent_folder_opex_creator_lambda
  to   = module.ingest.module.dr2_ingest_parent_folder_opex_creator_lambda
}

moved {
  from = module.dr2_ingest_parsed_court_document_event_handler_lambda
  to   = module.ingest.module.dr2_ingest_parsed_court_document_event_handler_lambda
}

moved {
  from = module.dr2_ingest_parsed_court_document_event_handler_sqs
  to   = module.ingest.module.dr2_ingest_parsed_court_document_event_handler_sqs
}

moved {
  from = module.dr2_ingest_run_workflow_step_function
  to   = module.ingest.module.dr2_ingest_run_workflow_step_function
}

moved {
  from = module.dr2_ingest_run_workflow_step_function_policy
  to   = module.ingest.module.dr2_ingest_run_workflow_step_function_policy
}

moved {
  from = module.dr2_ingest_start_workflow_lambda
  to   = module.ingest.module.dr2_ingest_start_workflow_lambda
}

moved {
  from = module.dr2_ingest_step_function
  to   = module.ingest.module.dr2_ingest_step_function
}

moved {
  from = module.dr2_ingest_step_function_policy
  to   = module.ingest.module.dr2_ingest_step_function_policy
}

moved {
  from = module.dr2_ingest_upsert_archive_folders_lambda
  to   = module.ingest.module.dr2_ingest_upsert_archive_folders_lambda
}

moved {
  from = module.dr2_ingest_validate_generic_ingest_inputs_lambda
  to   = module.ingest.module.dr2_ingest_validate_generic_ingest_inputs_lambda
}

moved {
  from = module.dr2_ingest_workflow_monitor_lambda
  to   = module.ingest.module.dr2_ingest_workflow_monitor_lambda
}

moved {
  from = module.dr2_run_e2e_tests_role[0]
  to   = module.ingest.module.dr2_run_e2e_tests_role[0]
}

moved {
  from = module.dri_preingest
  to   = module.ingest.module.dri_preingest
}

moved {
  from = module.ingest_find_existing_asset
  to   = module.ingest.module.ingest_find_existing_asset
}

moved {
  from = module.postingest
  to   = module.ingest.module.postingest
}

moved {
  from = module.tdr_preingest
  to   = module.ingest.module.tdr_preingest
}

moved {
  from = module.ingest_queue_table
  to   = module.ingest.module.ingest_queue_table
}

moved {
  from = aws_cloudwatch_dashboard.ingest_dashboard
  to   = module.common.aws_cloudwatch_dashboard.ingest_dashboard
}

moved {
  from = aws_cloudwatch_event_rule.fire_event_every_minute
  to   = module.common.aws_cloudwatch_event_rule.fire_event_every_minute
}

moved {
  from = aws_cloudwatch_event_target.lambda_trigger
  to   = module.common.aws_cloudwatch_event_target.lambda_trigger
}

moved {
  from = aws_cloudwatch_log_group.external_notification_log_group
  to   = module.common.aws_cloudwatch_log_group.external_notification_log_group
}

moved {
  from = aws_cloudwatch_log_resource_policy.eventbridge_resource_policy
  to   = module.common.aws_cloudwatch_log_resource_policy.eventbridge_resource_policy
}

moved {
  from = aws_dynamodb_table_item.dr2_preservica_version
  to   = module.common.aws_dynamodb_table_item.dr2_preservica_version
}

moved {
  from = aws_dynamodb_table_item.initial_start_datetime
  to   = module.common.aws_dynamodb_table_item.initial_start_datetime
}

moved {
  from = aws_ec2_managed_prefix_list.cloudflare_prefix_list
  to   = module.common.aws_ec2_managed_prefix_list.cloudflare_prefix_list
}

moved {
  from = aws_iam_access_key.custodial_copy_user_access_key
  to   = module.common.aws_iam_access_key.custodial_copy_user_access_key
}

moved {
  from = aws_iam_group.custodial_copy_group
  to   = module.common.aws_iam_group.custodial_copy_group
}

moved {
  from = aws_iam_group_policy.custodial_copy_group_policy
  to   = module.common.aws_iam_group_policy.custodial_copy_group_policy
}

moved {
  from = aws_iam_user.custodial_copy_user
  to   = module.common.aws_iam_user.custodial_copy_user
}

moved {
  from = aws_iam_user_group_membership.custodial_copy_group_membership
  to   = module.common.aws_iam_user_group_membership.custodial_copy_group_membership
}

moved {
  from = aws_lambda_permission.allow_eventbridge
  to   = module.common.aws_lambda_permission.allow_eventbridge
}

moved {
  from = aws_pipes_pipe.dr2_external_notifications_log_pipe
  to   = module.common.aws_pipes_pipe.dr2_external_notifications_log_pipe
}

moved {
  from = aws_secretsmanager_secret.demo_preservica_secret
  to   = module.common.aws_secretsmanager_secret.demo_preservica_secret
}

moved {
  from = aws_secretsmanager_secret.preservica_read_metadata
  to   = module.common.aws_secretsmanager_secret.preservica_read_metadata
}

moved {
  from = aws_secretsmanager_secret.preservica_read_metadata_read_content
  to   = module.common.aws_secretsmanager_secret.preservica_read_metadata_read_content
}

moved {
  from = aws_secretsmanager_secret.preservica_read_update_metadata_insert_content
  to   = module.common.aws_secretsmanager_secret.preservica_read_update_metadata_insert_content
}

moved {
  from = aws_secretsmanager_secret.preservica_secret
  to   = module.common.aws_secretsmanager_secret.preservica_secret
}

moved {
  from = aws_sns_topic_subscription.tre_topic_subscription[0]
  to   = module.common.aws_sns_topic_subscription.tre_topic_subscription[0]
}

moved {
  from = aws_ssm_parameter.flow_control_config
  to   = module.common.aws_ssm_parameter.flow_control_config
}

moved {
  from = aws_vpc_endpoint.discovery
  to   = module.common.aws_vpc_endpoint.discovery
}

moved {
  from = module.cloudwatch_alarm_event_bridge_rule["ALARM"]
  to   = module.common.module.cloudwatch_alarm_event_bridge_rule["ALARM"]
}

moved {
  from = module.cloudwatch_alarm_event_bridge_rule["OK"]
  to   = module.common.module.cloudwatch_alarm_event_bridge_rule["OK"]
}

moved {
  from = module.cloudwatch_event_alarm_event_bridge_rule_alarm_only
  to   = module.common.module.cloudwatch_event_alarm_event_bridge_rule_alarm_only
}

moved {
  from = module.deploy_lambda_policy
  to   = module.common.module.deploy_lambda_policy
}

moved {
  from = module.deploy_lambda_role
  to   = module.common.module.deploy_lambda_role
}

moved {
  from = module.dev_slack_message_eventbridge_rule
  to   = module.common.module.dev_slack_message_eventbridge_rule
}

moved {
  from = module.discovery_inbound_https
  to   = module.common.module.discovery_inbound_https
}

moved {
  from = module.dr2_court_document_package_anonymiser_lambda[0]
  to   = module.common.module.dr2_court_document_package_anonymiser_lambda[0]
}

moved {
  from = module.dr2_court_document_package_anonymiser_sqs[0]
  to   = module.common.module.dr2_court_document_package_anonymiser_sqs[0]
}

moved {
  from = module.dr2_custodial_copy_db_builder_queue
  to   = module.common.module.dr2_custodial_copy_db_builder_queue
}

moved {
  from = module.dr2_custodial_copy_queue
  to   = module.common.module.dr2_custodial_copy_queue
}

moved {
  from = module.dr2_custodial_copy_queue_creator_lambda
  to   = module.common.module.dr2_custodial_copy_queue_creator_lambda
}

moved {
  from = module.dr2_custodial_copy_queue_creator_queue
  to   = module.common.module.dr2_custodial_copy_queue_creator_queue
}

moved {
  from = module.dr2_custodial_copy_topic
  to   = module.common.module.dr2_custodial_copy_topic
}

moved {
  from = module.dr2_developer_key
  to   = module.common.module.dr2_developer_key
}

moved {
  from = module.dr2_entity_event_cloudwatch_event
  to   = module.common.module.dr2_entity_event_cloudwatch_event
}

moved {
  from = module.dr2_entity_event_generator_lambda
  to   = module.common.module.dr2_entity_event_generator_lambda
}

moved {
  from = module.dr2_entity_event_generator_topic
  to   = module.common.module.dr2_entity_event_generator_topic
}

moved {
  from = module.dr2_entity_event_lambda_updated_since_query_start_datetime_table
  to   = module.common.module.dr2_entity_event_lambda_updated_since_query_start_datetime_table
}

moved {
  from = module.dr2_external_notifications_pipes_policy
  to   = module.common.module.dr2_external_notifications_pipes_policy
}

moved {
  from = module.dr2_external_notifications_pipes_role
  to   = module.common.module.dr2_external_notifications_pipes_role
}

moved {
  from = module.dr2_external_notifications_queue
  to   = module.common.module.dr2_external_notifications_queue
}

moved {
  from = module.dr2_get_latest_preservica_version_cloudwatch_event
  to   = module.common.module.dr2_get_latest_preservica_version_cloudwatch_event
}

moved {
  from = module.dr2_get_latest_preservica_version_lambda
  to   = module.common.module.dr2_get_latest_preservica_version_lambda
}

moved {
  from = module.dr2_ingest_metric_collector_lambda
  to   = module.common.module.dr2_ingest_metric_collector_lambda
}

moved {
  from = module.dr2_ingest_parsed_court_document_event_handler_test_input_bucket[0]
  to   = module.common.module.dr2_ingest_parsed_court_document_event_handler_test_input_bucket[0]
}

moved {
  from = module.dr2_ip_lock_checker_cloudwatch_event
  to   = module.common.module.dr2_ip_lock_checker_cloudwatch_event
}

moved {
  from = module.dr2_ip_lock_checker_lambda
  to   = module.common.module.dr2_ip_lock_checker_lambda
}

moved {
  from = module.dr2_kms_key
  to   = module.common.module.dr2_kms_key
}

moved {
  from = module.dr2_latest_preservica_version_topic
  to   = module.common.module.dr2_latest_preservica_version_topic
}

moved {
  from = module.dr2_notifications_sns
  to   = module.common.module.dr2_notifications_sns
}

moved {
  from = module.dr2_rotate_preservation_system_password_lambda
  to   = module.common.module.dr2_rotate_preservation_system_password_lambda
}

moved {
  from = module.eventbridge_alarm_notifications_destination
  to   = module.common.module.eventbridge_alarm_notifications_destination
}

moved {
  from = module.failed_ingest_step_function_event_bridge_rule
  to   = module.common.module.failed_ingest_step_function_event_bridge_rule
}

moved {
  from = module.files_table
  to   = module.common.module.files_table
}

moved {
  from = module.general_slack_message_eventbridge_rule
  to   = module.common.module.general_slack_message_eventbridge_rule
}

moved {
  from = module.get_latest_preservica_version_lambda_dr2_preservica_version_table
  to   = module.common.module.get_latest_preservica_version_lambda_dr2_preservica_version_table
}

moved {
  from = module.guard_duty_findings_eventbridge_rule
  to   = module.common.module.guard_duty_findings_eventbridge_rule
}

moved {
  from = module.ingest_lock_table
  to   = module.common.module.ingest_lock_table
}

moved {
  from = module.ingest_raw_cache_bucket
  to   = module.common.module.ingest_raw_cache_bucket
}

moved {
  from = module.ingest_state_bucket
  to   = module.common.module.ingest_state_bucket
}

moved {
  from = module.outbound_cloudflare_https_access
  to   = module.common.module.outbound_cloudflare_https_access
}

moved {
  from = module.outbound_https_access_only
  to   = module.common.module.outbound_https_access_only
}

moved {
  from = module.pause_ingest_checker_cloudwatch_event
  to   = module.common.module.pause_ingest_checker_cloudwatch_event
}

moved {
  from = module.pause_ingest_lambda
  to   = module.common.module.pause_ingest_lambda
}

moved {
  from = module.pause_ingest_policy
  to   = module.common.module.pause_ingest_policy
}

moved {
  from = module.pause_ingest_role
  to   = module.common.module.pause_ingest_role
}

moved {
  from = module.pause_preservica_activity_checker_cloudwatch_event
  to   = module.common.module.pause_preservica_activity_checker_cloudwatch_event
}

moved {
  from = module.pause_preservica_activity_lambda
  to   = module.common.module.pause_preservica_activity_lambda
}

moved {
  from = module.pause_preservica_activity_policy
  to   = module.common.module.pause_preservica_activity_policy
}

moved {
  from = module.pause_preservica_activity_role
  to   = module.common.module.pause_preservica_activity_role
}

moved {
  from = module.remove_all_nacl_rules_policy
  to   = module.common.module.remove_all_nacl_rules_policy
}

moved {
  from = module.remove_all_nacl_rules_role
  to   = module.common.module.remove_all_nacl_rules_role
}

moved {
  from = module.sample_files_bucket
  to   = module.common.module.sample_files_bucket
}

moved {
  from = module.secret_rotation_eventbridge_rule
  to   = module.common.module.secret_rotation_eventbridge_rule
}

moved {
  from = module.vpc
  to   = module.common.module.vpc
}

moved {
  from = random_password.preservica_password
  to   = module.common.random_password.preservica_password
}

moved {
  from = random_string.preservica_user
  to   = module.common.random_string.preservica_user
}

