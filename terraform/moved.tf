moved {
  from = module.dri_preingest
  to   = module.dri_preingest["staging"]
}

moved {
  from = module.tdr_preingest
  to   = module.tdr_preingest["staging"]
}

moved {
  from = module.postingest
  to   = module.postingest["staging"]
}
moved {
  from = aws_cloudwatch_dashboard.ingest_dashboard
  to   = module.ingest["staging"].aws_cloudwatch_dashboard.ingest_dashboard
}

moved {
  from = aws_cloudwatch_event_rule.fire_event_every_minute
  to   = module.ingest["staging"].aws_cloudwatch_event_rule.fire_event_every_minute
}

moved {
  from = aws_cloudwatch_event_target.lambda_trigger
  to   = module.ingest["staging"].aws_cloudwatch_event_target.lambda_trigger
}

moved {
  from = aws_cloudwatch_log_resource_policy.eventbridge_resource_policy
  to   = module.ingest["staging"].aws_cloudwatch_log_resource_policy.eventbridge_resource_policy
}

moved {
  from = aws_dynamodb_table_item.dr2_preservica_version
  to   = module.ingest["staging"].aws_dynamodb_table_item.dr2_preservica_version
}

moved {
  from = aws_lambda_permission.allow_eventbridge
  to   = module.ingest["staging"].aws_lambda_permission.allow_eventbridge
}

moved {
  from = aws_ssm_parameter.flow_control_config
  to   = module.ingest["staging"].aws_ssm_parameter.flow_control_config
}

moved {
  from = module.cloudwatch_alarm_event_bridge_rule["ALARM"]
  to   = module.ingest["staging"].module.cloudwatch_alarm_event_bridge_rule["ALARM"]
}

moved {
  from = module.cloudwatch_alarm_event_bridge_rule["OK"]
  to   = module.ingest["staging"].module.cloudwatch_alarm_event_bridge_rule["OK"]
}

moved {
  from = module.cloudwatch_event_alarm_event_bridge_rule_alarm_only
  to   = module.ingest["staging"].module.cloudwatch_event_alarm_event_bridge_rule_alarm_only
}

moved {
  from = module.copy_from_tre_bucket_policy[0]
  to   = module.ingest["staging"].module.copy_from_tre_bucket_policy[0]
}

moved {
  from = module.copy_from_tre_bucket_role[0]
  to   = module.ingest["staging"].module.copy_from_tre_bucket_role[0]
}

moved {
  from = module.copy_tna_to_preservica_policy
  to   = module.ingest["staging"].module.copy_tna_to_preservica_policy
}

moved {
  from = module.copy_tna_to_preservica_role
  to   = module.ingest["staging"].module.copy_tna_to_preservica_role
}

moved {
  from = module.deploy_lambda_policy
  to   = module.ingest["staging"].module.deploy_lambda_policy
}

moved {
  from = module.deploy_lambda_role
  to   = module.ingest["staging"].module.deploy_lambda_role
}

moved {
  from = module.dev_slack_message_eventbridge_rule
  to   = module.ingest["staging"].module.dev_slack_message_eventbridge_rule
}

moved {
  from = module.dr2_get_latest_preservica_version_cloudwatch_event
  to   = module.ingest["staging"].module.dr2_get_latest_preservica_version_cloudwatch_event
}

moved {
  from = module.dr2_get_latest_preservica_version_lambda
  to   = module.ingest["staging"].module.dr2_get_latest_preservica_version_lambda
}

moved {
  from = module.dr2_ingest_asset_opex_creator_lambda
  to   = module.ingest["staging"].module.dr2_ingest_asset_opex_creator_lambda
}

moved {
  from = module.dr2_ingest_asset_reconciler_lambda
  to   = module.ingest["staging"].module.dr2_ingest_asset_reconciler_lambda
}

moved {
  from = module.dr2_ingest_failure_notifications_lambda
  to   = module.ingest["staging"].module.dr2_ingest_failure_notifications_lambda
}

moved {
  from = module.dr2_ingest_flow_control_lambda
  to   = module.ingest["staging"].module.dr2_ingest_flow_control_lambda
}

moved {
  from = module.dr2_ingest_folder_opex_creator_lambda
  to   = module.ingest["staging"].module.dr2_ingest_folder_opex_creator_lambda
}

moved {
  from = module.dr2_ingest_mapper_lambda
  to   = module.ingest["staging"].module.dr2_ingest_mapper_lambda
}

moved {
  from = module.dr2_ingest_metric_collector_lambda
  to   = module.ingest["staging"].module.dr2_ingest_metric_collector_lambda
}

moved {
  from = module.dr2_ingest_parent_folder_opex_creator_lambda
  to   = module.ingest["staging"].module.dr2_ingest_parent_folder_opex_creator_lambda
}

moved {
  from = module.dr2_ingest_parsed_court_document_event_handler_lambda
  to   = module.ingest["staging"].module.dr2_ingest_parsed_court_document_event_handler_lambda
}

moved {
  from = module.dr2_ingest_parsed_court_document_event_handler_sqs
  to   = module.ingest["staging"].module.dr2_ingest_parsed_court_document_event_handler_sqs
}

moved {
  from = module.dr2_ingest_run_workflow_step_function
  to   = module.ingest["staging"].module.dr2_ingest_run_workflow_step_function
}

moved {
  from = module.dr2_ingest_run_workflow_step_function_policy
  to   = module.ingest["staging"].module.dr2_ingest_run_workflow_step_function_policy
}

moved {
  from = module.dr2_ingest_start_workflow_lambda
  to   = module.ingest["staging"].module.dr2_ingest_start_workflow_lambda
}

moved {
  from = module.dr2_ingest_step_function
  to   = module.ingest["staging"].module.dr2_ingest_step_function
}

moved {
  from = module.dr2_ingest_step_function_policy
  to   = module.ingest["staging"].module.dr2_ingest_step_function_policy
}

moved {
  from = module.dr2_ingest_upsert_archive_folders_lambda
  to   = module.ingest["staging"].module.dr2_ingest_upsert_archive_folders_lambda
}

moved {
  from = module.dr2_ingest_validate_generic_ingest_inputs_lambda
  to   = module.ingest["staging"].module.dr2_ingest_validate_generic_ingest_inputs_lambda
}

moved {
  from = module.dr2_ingest_workflow_monitor_lambda
  to   = module.ingest["staging"].module.dr2_ingest_workflow_monitor_lambda
}

moved {
  from = module.dr2_ip_lock_checker_cloudwatch_event
  to   = module.ingest["staging"].module.dr2_ip_lock_checker_cloudwatch_event
}

moved {
  from = module.dr2_ip_lock_checker_lambda
  to   = module.ingest["staging"].module.dr2_ip_lock_checker_lambda
}

moved {
  from = module.dr2_latest_preservica_version_topic
  to   = module.ingest["staging"].module.dr2_latest_preservica_version_topic
}

moved {
  from = module.dr2_rotate_preservation_system_password_lambda
  to   = module.ingest["staging"].module.dr2_rotate_preservation_system_password_lambda
}

moved {
  from = module.eventbridge_alarm_notifications_destination
  to   = module.ingest["staging"].module.eventbridge_alarm_notifications_destination
}

moved {
  from = module.failed_ingest_step_function_event_bridge_rule
  to   = module.ingest["staging"].module.failed_ingest_step_function_event_bridge_rule
}

moved {
  from = module.files_table
  to   = module.ingest["staging"].module.files_table
}

moved {
  from = module.general_slack_message_eventbridge_rule
  to   = module.ingest["staging"].module.general_slack_message_eventbridge_rule
}

moved {
  from = module.get_latest_preservica_version_lambda_dr2_preservica_version_table
  to   = module.ingest["staging"].module.get_latest_preservica_version_lambda_dr2_preservica_version_table
}

moved {
  from = module.guard_duty_findings_eventbridge_rule
  to   = module.ingest["staging"].module.guard_duty_findings_eventbridge_rule
}

moved {
  from = module.ingest_find_existing_asset
  to   = module.ingest["staging"].module.ingest_find_existing_asset
}

moved {
  from = module.ingest_queue_table
  to   = module.ingest["staging"].module.ingest_queue_table
}

moved {
  from = module.secret_rotation_eventbridge_rule
  to   = module.ingest["staging"].module.secret_rotation_eventbridge_rule
}

