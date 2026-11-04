locals {
  entity_event_lambda_key_name = "entity-event-generator-lambda"
  entity_event_lambda_name     = "${local.environment}-dr2-${local.entity_event_lambda_key_name}"
  entity_event_topic_name      = "${local.environment}-dr2-entity-event-generator-topic"
  last_polled_table_name       = "${local.environment}-dr2-entity-event-lambda-updated-since-query-start-datetime"
  entity_event_topic_arn       = "arn:aws:sns:eu-west-2:${data.aws_caller_identity.current.account_id}:${local.entity_event_topic_name}"
}

module "dr2_entity_event_cloudwatch_event" {
  source                  = "git::https://github.com/nationalarchives/da-terraform-modules//cloudwatch_events"
  rule_name               = "${local.environment}-dr2-entity-event-schedule"
  schedule                = "rate(5 minutes)"
  lambda_event_target_arn = "arn:aws:lambda:eu-west-2:${data.aws_caller_identity.current.account_id}:function:${local.entity_event_lambda_name}"
}

module "dr2_entity_event_generator_lambda" {
  source        = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name = local.entity_event_lambda_name
  handler       = "uk.gov.nationalarchives.entityeventgenerator.Lambda::handleRequest"
  policies = {
    "${local.entity_event_lambda_name}-policy" = templatefile("${path.module}/templates/iam_policy/entity_event_lambda_policy.json.tpl", {
      account_id                 = data.aws_caller_identity.current.account_id
      lambda_name                = local.entity_event_lambda_name
      dynamo_db_file_table_arn   = module.dr2_entity_event_lambda_updated_since_query_start_datetime_table.table_arn
      secrets_manager_secret_arn = aws_secretsmanager_secret.preservica_read_metadata.arn
      sns_arn                    = local.entity_event_topic_arn
      vpc_id                     = module.vpc.vpc.id
    })
  }
  s3_bucket            = local.code_deploy_bucket
  s3_key               = "${var.lambda_code_version}/${local.entity_event_lambda_key_name}"
  timeout_seconds      = 60
  memory_size          = local.java_lambda_memory_size
  runtime              = local.java_runtime
  architecture         = local.architecture_arm64
  tags                 = {}
  reserved_concurrency = 1
  lambda_invoke_permissions = {
    "events.amazonaws.com" = module.dr2_entity_event_cloudwatch_event.event_arn
  }
  vpc_config = {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = flatten([local.clouflare_and_vpc_endpoints_security_groups, [module.outbound_https_access_for_dynamo_db.security_group_id]])
  }
  plaintext_env_vars = {
    LAMBDA_STATE_DDB_TABLE = local.last_polled_table_name
    PRESERVICA_SECRET_NAME = aws_secretsmanager_secret.preservica_read_metadata.name
    OUTPUT_TOPIC_ARN       = local.entity_event_topic_arn
  }
}

module "dr2_entity_event_lambda_updated_since_query_start_datetime_table" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//dynamo"
  hash_key   = { name = "id", type = "S" }
  table_name = local.last_polled_table_name
}

resource "aws_dynamodb_table_item" "initial_start_datetime" {
  hash_key   = "id"
  item       = templatefile("${path.module}/templates/dynamo/initial_last_updated_item.json.tpl", {})
  table_name = local.last_polled_table_name
  lifecycle {
    ignore_changes = [item]
  }
}

module "dr2_entity_event_generator_topic" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//sns"
  sns_policy = templatefile("${path.module}/templates/sns/entity_event_topic_policy.json.tpl", {
    lambda_role_arn = module.dr2_entity_event_generator_lambda.lambda_role_arn
    sns_topic       = local.entity_event_topic_arn
  })
  tags       = {}
  topic_name = local.entity_event_topic_name
  sqs_subscriptions = {
    custodial_copy_queue_creator_queue = module.dr2_custodial_copy_queue_creator_queue.sqs_arn
  }
}

resource "awscc_cloudwatch_log_alarm" "entity_event" {
  alarm_name      = "${local.environment}-entity-event-failure-alarm"
  tags            = []
  actions_enabled = true

  threshold                 = 0
  comparison_operator       = "GreaterThanThreshold"
  query_results_to_evaluate = 1
  query_results_to_alarm    = 1
  treat_missing_data        = "notBreaching"

  scheduled_query_configuration = {
    query_string           = "filter log.level == \"ERROR\""
    aggregation_expression = "COUNT(*)"

    log_group_identifiers = [
      "/aws/lambda/${local.environment}-dr2-entity-event-generator-lambda"
    ]

    scheduled_query_role_arn = module.entity_event_failure_alarm_execution_role.role_arn

    schedule_configuration = {
      schedule_expression = "rate(15 minutes)"
      start_time_offset   = 900
      end_time_offset     = 0
    }
  }
}

module "entity_event_failure_alarm_execution_role" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_role"
  assume_role_policy = templatefile("${path.module}/templates/iam_role/service_assume_role.json.tpl", {
    service        = "logs"
    account_number = data.aws_caller_identity.current.account_id
  })
  name = "${local.environment}-dr2-entity-event-failure-alarm"
  policy_attachments = {
    entity_event_failure = module.entity_event_failure_alarm_execution_policy.policy_arn
  }
  tags = {}
}

module "entity_event_failure_alarm_execution_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.environment}-dr2-entity-event-failure-alarm"
  policy_string = templatefile("${path.module}/templates/iam_policy/scheduled_log_alarm_policy.json.tpl", {
    log_group_arn = "arn:aws:logs:eu-west-2:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.entity_event_lambda_name}"
  })
}

