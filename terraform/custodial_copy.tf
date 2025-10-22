locals {
  custodial_copy_name                  = "${local.environment}-dr2-custodial-copy"
  custodial_copy_db_builder_queue_name = "${local.custodial_copy_name}-db-builder"
}

resource "aws_iam_user" "custodial_copy_user" {
  name = local.custodial_copy_name
}

resource "aws_iam_access_key" "custodial_copy_user_access_key" {
  user = local.custodial_copy_name
}

resource "aws_iam_user_group_membership" "custodial_copy_group_membership" {
  groups = [aws_iam_group.custodial_copy_group.name]
  user   = aws_iam_user.custodial_copy_user.name
}

resource "aws_iam_group" "custodial_copy_group" {
  name = local.custodial_copy_name
}

resource "aws_iam_group_policy" "custodial_copy_group_policy" {
  group = aws_iam_group.custodial_copy_group.name
  name  = "${local.environment}-dr2-custodial-copy-policy"
  policy = templatefile("${path.module}/templates/iam_policy/custodial_copy_policy.json.tpl", {
    account_id                     = data.aws_caller_identity.current.account_id
    secrets_manager_secret_arn     = aws_secretsmanager_secret.preservica_read_metadata_read_content.arn
    custodial_copy_queue           = module.dr2_custodial_copy_queue.sqs_arn
    custodial_copy_confirmer_queue = module.ingest.postingest.postingest_confirmer_queue_arn
    postingest_table               = module.ingest.postingest.postingest_table_arn
    database_builder_queue         = module.dr2_custodial_copy_db_builder_queue.sqs_arn
    management_account_id          = module.config.account_numbers["mgmt"]
  })
}

module "dr2_custodial_copy_db_builder_queue" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name = local.custodial_copy_db_builder_queue_name
  sqs_policy = templatefile("./templates/sqs/sns_send_message_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.custodial_copy_db_builder_queue_name
    topic_arn  = local.custodial_copy_topic_arn
  })
  queue_cloudwatch_alarm_visible_messages_threshold = local.messages_visible_threshold
  encryption_type                                   = local.sse_encryption
}

module "dr2_custodial_copy_queue" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name = local.custodial_copy_name
  fifo_queue = true
  sqs_policy = templatefile("./templates/sqs/sqs_access_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.custodial_copy_name
  })
  queue_cloudwatch_alarm_visible_messages_threshold = local.messages_visible_threshold
  encryption_type                                   = local.sse_encryption
  visibility_timeout                                = 300
}
