locals {
  notifications_topic_name = "${local.environment}-dr2-notifications"
}

module "dr2_notifications_sns" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//sns"
  sns_policy = templatefile("${path.module}/templates/sns/external_notifications_policy.json.tpl", {
    topic_name         = local.notifications_topic_name
    account_id         = data.aws_caller_identity.current.account_id
    tdr_terraform_role = module.tdr_config.terraform_config[local.environment]["terraform_account_role"]
    tdr_account_id     = module.tdr_config.account_numbers[local.environment]
  })
  tags = {
    Name = local.notifications_topic_name
  }
  topic_name = local.notifications_topic_name
  sqs_subscriptions = {
    log_external_notifications_queue = module.dr2_external_notifications_queue.sqs_arn
  }
}
