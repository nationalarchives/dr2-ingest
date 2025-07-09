output "postingest_table_name" {
  value = local.postingest_state_table_name
}

output "postingest_table_arn" {
  value = module.postingest_state_table.table_arn
}

output "postingest_confirmer_queue_arn" {
  value = module.dr2_custodial_copy_confirmer_queue.sqs_arn
}

output "postingest_state_change_lambda_arn" {
  value = module.dr2_state_change_lambda.lambda_arn
}

output "postingest_resender_lambda_arn" {
  value = module.dr2_message_resender_lambda.lambda_arn
}

