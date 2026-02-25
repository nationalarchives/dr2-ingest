locals {
  external_notifications_name = "${local.environment}-external-notifications"
}
resource "aws_cloudwatch_log_group" "external_notification_log_group" {
  name              = "/${local.external_notifications_name}"
  retention_in_days = 30
}

module "dr2_external_notifications_pipes_role" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_role"
  assume_role_policy = templatefile("${path.module}/templates/iam_role/service_source_account_only.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    service    = "pipes"
  })
  name = "${local.environment}-dr2-log-external-notifications"
  policy_attachments = {
    external_notifications_log = module.dr2_external_notifications_pipes_policy.policy_arn
  }
  tags = {}
}

module "dr2_external_notifications_queue" {
  source                                            = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name                                        = local.external_notifications_name
  queue_cloudwatch_alarm_visible_messages_threshold = local.messages_visible_threshold
  sqs_policy = templatefile("./templates/sqs/sns_send_message_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.external_notifications_name
    topic_arn  = module.dr2_notifications_sns.sns_arn
  })
  encryption_type = local.sse_encryption
}

module "dr2_external_notifications_pipes_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.environment}-dr2-log-external-notifications-policy"
  policy_string = templatefile("${path.module}/templates/iam_policy/external_notification_log_pipe_policy.json.tpl", {
    queue_arn      = module.dr2_external_notifications_queue.sqs_arn
    account_id     = data.aws_caller_identity.current.account_id
    log_group_name = aws_cloudwatch_log_group.external_notification_log_group.name
  })
}

resource "aws_pipes_pipe" "dr2_external_notifications_log_pipe" {
  depends_on = [module.dr2_external_notifications_pipes_policy.policy_arn]
  name       = local.external_notifications_name
  role_arn   = module.dr2_external_notifications_pipes_role.role_arn
  source     = module.dr2_external_notifications_queue.sqs_arn
  target     = aws_cloudwatch_log_group.external_notification_log_group.arn
  target_parameters {
    input_template = templatefile("${path.module}/templates/pipes/sqs_to_cloudwatch_target_transformer.json.tpl", {
      topic_arn = module.dr2_notifications_sns.sns_arn
    })
  }
}