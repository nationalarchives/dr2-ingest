output "ingest_step_function" {
  value = module.dr2_ingest_step_function
}

output "lambdas" {
  value = local.lambdas_by_name
}

output "court_document_event_handler_sqs" {
  value = module.dr2_ingest_parsed_court_document_event_handler_sqs
}

output "importer_sqs_queues" {
  value = [module.tdr_preingest.importer_sqs, module.dri_preingest.importer_sqs]
}

output "e2e_tests_role" {
  value = var.environment == "intg" ? [module.dr2_run_e2e_tests_role[0].role_arn] : []
}

output "step_function_arns" {
  value = [
    module.dr2_ingest_step_function.step_function_arn,
    module.tdr_preingest.preingest_sfn_arn,
    module.dri_preingest.preingest_sfn_arn,
    module.dr2_ingest_run_workflow_step_function.step_function_arn
  ]
}

output "run_workflow_step_function_arn" {
  value = module.dr2_ingest_run_workflow_step_function.step_function_arn
}

output "lambda_names" {
  value = {
    ingest_asset_opex_creator  = local.ingest_asset_opex_creator_lambda_name
    find_existing_asset        = local.ingest_find_existing_asset_name
    validate_ingest_inputs     = local.ingest_validate_generic_ingest_inputs_lambda_name
    ingest_reconciler          = local.ingest_asset_reconciler_lambda_name
    folder_opex_creator        = local.ingest_folder_opex_creator_lambda_name
    parent_folder_opex_creator = local.ingest_parent_folder_opex_creator_lambda_name
    tdr_aggregator             = module.tdr_preingest.aggregator_lambda.function_name
    ingest_mapper              = local.ingest_mapper_lambda_name
    tdr_package_builder        = module.tdr_preingest.package_builder_lambda.function_name
    court_document_handler     = local.ingest_parsed_court_document_event_handler_lambda_name
    upsert_folders             = local.ingest_upsert_archive_folders_lambda_name
    dri_importer               = module.dri_preingest.importer_lambda.function_name
    tdr_importer               = module.tdr_preingest.importer_lambda.function_name
    dri_package_builder        = module.dri_preingest.package_builder_lambda.function_name
    folder_opex_creator        = local.ingest_folder_opex_creator_lambda_name
    dri_aggregator             = module.dri_preingest.aggregator_lambda.function_name,
    failed_notification        = local.ingest_failure_notifications_lambda_name
  }
}

output "postingest" {
  value = module.postingest
}

output "tdr_preingest" {
  value = module.tdr_preingest
}

output "dri_preingest" {
  value = module.dri_preingest
}