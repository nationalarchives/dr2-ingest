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

