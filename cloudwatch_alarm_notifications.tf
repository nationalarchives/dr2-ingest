locals {
  cloudwatch_alarm_notifications_name = "${local.environment}-cloudwatch-alarms-notifications"
}
module "cloudwatch_alarms_notifications_sns" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//sns"
  sns_policy = templatefile("${path.module}/templates/sns/cloudwatch_alarm_policy.json.tpl", {
    topic_name           = local.cloudwatch_alarm_notifications_name
    account_id           = data.aws_caller_identity.current.account_id
    cloudwatch_alarm_arn = module.download_files_sqs.dlq_cloudwatch_alarm_arn
  })
  tags = {
    Name = "Preservica Config SNS"
  }
  topic_name  = local.cloudwatch_alarm_notifications_name
  kms_key_arn = module.dr2_kms_key.kms_key_arn
  sqs_subscriptions = {
    cloudwatch_alarms_notifications_queue = module.cloudwatch_alarms_notifications_queue.sqs_arn
  }
}

module "cloudwatch_alarms_notifications_queue" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name = local.cloudwatch_alarm_notifications_name
  sqs_policy = templatefile("${path.module}/templates/sqs/sns_send_message_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.cloudwatch_alarm_notifications_name,
    topic_arn  = "arn:aws:sns:eu-west-2:${data.aws_caller_identity.current.account_id}:${local.cloudwatch_alarm_notifications_name}"
  })
  kms_key_id = module.dr2_developer_key.kms_key_arn
}
