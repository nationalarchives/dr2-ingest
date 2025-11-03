locals {
  custodial_copy_topic_name = "${local.environment}-dr2-cc-notifications"
  custodial_copy_topic_arn  = "arn:aws:sns:eu-west-2:${data.aws_caller_identity.current.account_id}:${local.custodial_copy_topic_name}"
}

module "dr2_custodial_copy_topic" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//sns"
  sns_policy = templatefile("./templates/sns/custodial_copy_topic_policy.json.tpl", {
    cc_user_arn = aws_iam_user.custodial_copy_user.arn
    sns_topic   = local.custodial_copy_topic_arn
  })
  tags       = {}
  topic_name = local.custodial_copy_topic_name
  sqs_subscriptions = {
    database_builder_queue = module.dr2_custodial_copy_db_builder_queue.sqs_arn
  }
}
