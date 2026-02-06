locals {
  postingest_state_table_name         = "${var.environment}-dr2-postingest-state"
  postingest_gsi_firstqueued_name     = "QueueFirstQueuedIdx"
  postingest_gsi_lastqueued_name      = "QueueLastQueuedIdx"
  custodial_copy_confirmer_queue_name = "${var.environment}-dr2-custodial-copy-confirmer"
  state_change_lambda_name            = "${var.environment}-dr2-postingest-state-change-handler"
  state_change_lambda_dlq             = "${var.environment}-dr2-postingest-state-change-dlq"
  resender_lambda_name                = "${var.environment}-dr2-postingest-message-resender"
  java_runtime                        = "java21"
  java_lambda_memory_size             = 512
  postingest_queue_config = [ // Before adding a new queue here, update the state change handler to expect it
    { "queueAlias" : "CC", "queueOrder" : 1, "queueUrl" : module.dr2_custodial_copy_confirmer_queue.sqs_queue_url }
  ]
  six_hours                  = 60 * 60 * 6
  seven_days                 = 60 * 60 * 24 * 7
  messages_visible_threshold = 1000000
}

data "aws_caller_identity" "current" {}

module "postingest_state_table" {
  source                         = "git::https://github.com/nationalarchives/da-terraform-modules//dynamo"
  hash_key                       = { name = "assetId", type = "S" }
  range_key                      = { name = "batchId", type = "S" }
  table_name                     = local.postingest_state_table_name
  server_side_encryption_enabled = false
  ttl_attribute_name             = "ttl"
  stream_enabled                 = true
  stream_view_type               = "NEW_AND_OLD_IMAGES"
  deletion_protection_enabled    = true
  additional_attributes = [
    { name = "queue", type = "S" },
    { name = "lastQueued", type = "S" },
    { name = "firstQueued", type = "S" }
  ]
  global_secondary_indexes = [
    {
      name            = local.postingest_gsi_lastqueued_name
      hash_key        = "queue"
      range_key       = "lastQueued"
      projection_type = "ALL"
    },
    {
      name            = local.postingest_gsi_firstqueued_name
      hash_key        = "queue"
      range_key       = "firstQueued"
      projection_type = "ALL"
    }
  ]
  point_in_time_recovery_enabled = true
}

module "dr2_custodial_copy_confirmer_queue" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name = local.custodial_copy_confirmer_queue_name
  sqs_policy = templatefile("./templates/sqs/sqs_access_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.custodial_copy_confirmer_queue_name
  })
  create_dlq                                        = false
  queue_cloudwatch_alarm_visible_messages_threshold = local.messages_visible_threshold
  visibility_timeout                                = 600
  encryption_type                                   = "sse"
  delay_seconds                                     = 900
}

module "cc_confirmer_message_older_than_one_week_alarm" {
  source              = "git::https://github.com/nationalarchives/da-terraform-modules//cloudwatch_alarms"
  name                = "${local.custodial_copy_confirmer_queue_name}-messages-older-than-one-week-alarm"
  comparison_operator = "GreaterThanThreshold"
  metric_name         = "ApproximateAgeOfOldestMessage"
  namespace           = "AWS/SQS"
  statistic           = "Maximum"
  treat_missing_data  = "ignore"
  datapoints_to_alarm = 1
  dimensions = {
    QueueName = local.custodial_copy_confirmer_queue_name
  }
  period    = local.six_hours
  threshold = local.seven_days
}

module "dr2_state_change_lambda_dlq" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name = local.state_change_lambda_dlq
  sqs_policy = templatefile("./templates/sqs/lambda_sqs_dlq_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.state_change_lambda_dlq
  })
  create_dlq                                        = false
  queue_cloudwatch_alarm_visible_messages_threshold = 1
  visibility_timeout                                = 300
  encryption_type                                   = "sse"
}


module "dr2_state_change_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.state_change_lambda_name
  handler         = "uk.gov.nationalarchives.postingeststatechangehandler.Lambda::handleRequest"
  timeout_seconds = 900

  policies = {
    "${local.state_change_lambda_name}-policy" = templatefile("${path.module}/templates/policies/state_change_lambda_policy.json.tpl", {
      custodial_copy_checker_queue_arn = module.dr2_custodial_copy_confirmer_queue.sqs_arn
      dynamo_db_postingest_arn         = module.postingest_state_table.table_arn
      sns_external_notifications_arn   = var.notifications_topic_arn
      account_id                       = data.aws_caller_identity.current.account_id
      lambda_name                      = local.state_change_lambda_name
      dynamo_db_postingest_stream_arn  = module.postingest_state_table.stream_arn
      state_change_dlq_arn             = module.dr2_state_change_lambda_dlq.sqs_arn
      vpc_id                           = var.vpc_id
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  dynamo_stream_config = {
    stream_arn             = module.postingest_state_table.stream_arn
    dead_letter_target_arn = module.dr2_state_change_lambda_dlq.sqs_arn
  }
  vpc_config = {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = var.private_security_group_ids
  }
  plaintext_env_vars = {
    POSTINGEST_STATE_DDB_TABLE                = local.postingest_state_table_name
    POSTINGEST_DDB_TABLE_BATCHPARENT_GSI_NAME = local.postingest_gsi_lastqueued_name
    OUTPUT_TOPIC_ARN                          = var.notifications_topic_arn
    POSTINGEST_QUEUES                         = jsonencode(local.postingest_queue_config)
  }
  tags = {
    Name = local.state_change_lambda_name
  }
}

module "dr2_message_resender_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.resender_lambda_name
  handler         = "uk.gov.nationalarchives.postingestresender.Lambda::handleRequest"
  timeout_seconds = 900

  policies = {
    "${local.resender_lambda_name}-policy" = templatefile("${path.module}/templates/policies/message_resender_lambda_policy.json.tpl", {
      custodial_copy_checker_queue_arn = module.dr2_custodial_copy_confirmer_queue.sqs_arn
      postingest_state_arn             = module.postingest_state_table.table_arn
      account_id                       = data.aws_caller_identity.current.account_id
      lambda_name                      = local.resender_lambda_name
      gsi_name                         = local.postingest_gsi_lastqueued_name
      vpc_id                           = var.vpc_id
    })
  }
  lambda_invoke_permissions = {
    "events.amazonaws.com" = module.dr2_message_resender_cloudwatch_event.event_arn
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    POSTINGEST_STATE_DDB_TABLE                = local.postingest_state_table_name
    POSTINGEST_DDB_TABLE_BATCHPARENT_GSI_NAME = local.postingest_gsi_lastqueued_name
    POSTINGEST_QUEUES                         = jsonencode(local.postingest_queue_config)
  }
  vpc_config = {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = var.private_security_group_ids
  }
  tags = {
    Name = local.resender_lambda_name
  }
}

module "dr2_message_resender_cloudwatch_event" {
  source                  = "git::https://github.com/nationalarchives/da-terraform-modules//cloudwatch_events"
  rule_name               = "${var.environment}-dr2-postingest-resender-schedule"
  schedule                = "rate(1 hour)"
  lambda_event_target_arn = "arn:aws:lambda:eu-west-2:${data.aws_caller_identity.current.account_id}:function:${local.resender_lambda_name}"
}
