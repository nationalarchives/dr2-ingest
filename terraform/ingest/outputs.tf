output "ingest_step_function" {
  value = {
    step_function_arn      = module.dr2_ingest_step_function.step_function_arn
    step_function_role_arn = ""//module.dr2_ingest_step_function.step_function_role_arn
    name                   = local.ingest_step_function_name
  }
}

output "court_document_event_handler_sqs" {
  value = {
    sqs_arn = module.dr2_ingest_parsed_court_document_event_handler_sqs.sqs_arn
  }
}

output "e2e_tests_role" {
  value = var.environment == "intg" ? [module.dr2_run_e2e_tests_role[0].role_arn] : []
}

output "step_function_arns" {
  value = [
    module.dr2_ingest_step_function.step_function_arn,
    module.dr2_ingest_run_workflow_step_function.step_function_arn
  ]
}

output "run_workflow_step_function_arn" {
  value = module.dr2_ingest_run_workflow_step_function.step_function_arn
}

