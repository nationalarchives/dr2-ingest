output "aggregator_lambda" {
  value = module.dr2_preingest_aggregator_lambda.lambda_function
}

output "aggregator_sqs" {
  value = module.dr2_preingest_aggregator_queue
}

output "package_builder_lambda" {
  value = module.dr2_preingest_package_builder_lambda.lambda_function
}

output "importer_lambda" {
  value = module.dr2_importer_lambda.lambda_function
}

output "importer_sqs" {
  value = module.dr2_importer_sqs
}

output "preingest_sfn_arn" {
  value = module.dr2_preingest_step_function.step_function_arn
}

output "importer_lambda_role_arn" {
  value = module.dr2_importer_lambda.lambda_role_arn
}