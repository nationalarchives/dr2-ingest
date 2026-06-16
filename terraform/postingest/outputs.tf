output "postingest_table_name" {
  value = local.postingest_state_table_name
}

output "postingest_table_arn" {
  value = module.postingest_state_table.table_arn
}

output "postingest_confirmer_queue_arns" {
  value = {
    for k, v in module.confirmer_queues : k => v.sqs_arn
  }
}

output "postingest_state_change_lambda_arn" {
  value = module.dr2_state_change_lambda.lambda_arn
}

output "postingest_resender_lambda_arn" {
  value = module.dr2_message_resender_lambda.lambda_arn
}

output "confirmer_queue_oldest_message_alarm_arns" {
  value = {
    for k, v in module.confirmer_message_older_than_one_week_alarm : k => v.cloudwatch_alarm_arn
  }
}
