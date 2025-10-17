data "aws_caller_identity" "current" {}

locals {
  environment                                  = var.environment
  preingest_name                               = "${local.environment}-dr2-preingest-${var.source_name}"
  aggregator_name                              = "${local.preingest_name}-aggregator"
  aggregator_queue_arn                         = "arn:aws:sqs:eu-west-2:${data.aws_caller_identity.current.account_id}:${local.aggregator_name}"
  package_builder_lambda_name                  = "${local.preingest_name}-package-builder"
  preingest_sfn_arn                            = "arn:aws:states:eu-west-2:${data.aws_caller_identity.current.account_id}:stateMachine:${local.preingest_name}"
  ingest_sfn_arn                               = "arn:aws:states:eu-west-2:${data.aws_caller_identity.current.account_id}:stateMachine:${var.ingest_step_function_name}"
  java_runtime                                 = "java21"
  java_lambda_memory_size                      = 512
  java_timeout_seconds                         = 180
  aggregator_primary_grouping_window_seconds   = 300                                                                                        # How long the SQS Poller waits before invoking the Lambda after receiving the first message. <=300 for Lambda.
  aggregator_lambda_timeout_seconds            = 60                                                                                         # <=900 for Lambda.
  aggregator_secondary_grouping_window_seconds = 180                                                                                        # Additional time we wait before starting preingest to allow multiple invocations to form a single group, this is added to the aggregator_lambda_timeout_seconds when we start a group.
  aggregator_invocation_batch_size             = 10000                                                                                      # Max number of messages to invoke the Lambda with, but all messages need to be processed before the Lambda times out. <=10000 for Lambda.
  aggregator_group_size                        = 10000                                                                                      # Max size of an aggregation group.
  aggregator_queue_visibility_timeout          = local.aggregator_primary_grouping_window_seconds + local.aggregator_lambda_timeout_seconds # <=43200 for SQS.
  messages_visible_threshold                   = 1000000
}

module "dr2_preingest_aggregator_queue" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name = local.aggregator_name
  sqs_policy = var.sns_topic_arn == null ? templatefile("${path.module}/templates/sqs_access_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.aggregator_name
    }) : templatefile("${path.module}/templates/sns_send_message_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.aggregator_name
    topic_arn  = var.sns_topic_arn
  })
  queue_cloudwatch_alarm_visible_messages_threshold = local.messages_visible_threshold
  visibility_timeout                                = local.aggregator_queue_visibility_timeout
  encryption_type                                   = "sse"
}

module "dr2_preingest_aggregator_lambda" {
  source                         = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name                  = local.aggregator_name
  handler                        = "uk.gov.nationalarchives.preingesttdraggregator.Lambda::handleRequest"
  sqs_queue_batching_window      = local.aggregator_primary_grouping_window_seconds
  sqs_queue_mapping_batch_size   = local.aggregator_invocation_batch_size
  sqs_report_batch_item_failures = true
  lambda_sqs_queue_mappings = [{
    sqs_queue_arn         = local.aggregator_queue_arn
    sqs_queue_concurrency = 2
    ignore_enabled_status = true
  }]
  timeout_seconds = local.aggregator_lambda_timeout_seconds
  policies = {
    "${local.aggregator_name}-policy" = templatefile("${path.module}/templates/preingest_aggregator_policy.json.tpl", {
      account_id                 = data.aws_caller_identity.current.account_id
      lambda_name                = local.aggregator_name
      dynamo_db_lock_table_arn   = var.ingest_lock_table_arn
      preingest_sfn_arn          = local.preingest_sfn_arn
      preingest_aggregator_queue = local.aggregator_queue_arn
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    LOCK_DDB_TABLE                = var.ingest_lock_dynamo_table_name
    MAX_BATCH_SIZE                = local.aggregator_group_size
    MAX_SECONDARY_BATCHING_WINDOW = local.aggregator_secondary_grouping_window_seconds
    PREINGEST_SFN_ARN             = local.preingest_sfn_arn
    SOURCE_SYSTEM                 = upper(var.source_name)
  }
  tags = {
    Name = local.aggregator_name
  }
}

module "dr2_preingest_step_function" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//sfn"
  step_function_definition = templatefile("${path.module}/templates/preingest_sfn_definition.json.tpl", {
    ingest_step_function_arn    = local.ingest_sfn_arn
    account_id                  = data.aws_caller_identity.current.account_id
    package_builder_lambda_name = local.package_builder_lambda_name
    retry_statement             = jsonencode([{ ErrorEquals = ["States.ALL"], IntervalSeconds = 2, MaxAttempts = 6, BackoffRate = 2, JitterStrategy = "FULL" }])
  })
  step_function_name = local.preingest_name
  step_function_role_policy_attachments = {
    preingest_step_function_policy = module.dr2_preingest_step_function_policy.policy_arn
  }
}

module "dr2_preingest_step_function_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.preingest_name}-step-function-policy"
  policy_string = templatefile("${path.module}/templates/preingest_step_function_policy.json.tpl", {
    ingest_step_function_arn    = local.ingest_sfn_arn
    account_id                  = data.aws_caller_identity.current.account_id
    package_builder_lambda_name = local.package_builder_lambda_name
  })
}

module "dr2_preingest_package_builder_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.package_builder_lambda_name
  handler         = "uk.gov.nationalarchives.preingesttdrpackagebuilder.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${local.package_builder_lambda_name}-policy" = templatefile("${path.module}/templates/preingest_package_builder_policy.json.tpl", {
      account_id               = data.aws_caller_identity.current.account_id
      lambda_name              = local.package_builder_lambda_name
      dynamo_db_lock_table_arn = var.ingest_lock_table_arn
      gsi_name                 = var.ingest_lock_table_group_id_gsi_name
      raw_cache_bucket_name    = var.ingest_raw_cache_bucket_name
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    LOCK_DDB_TABLE                  = var.ingest_lock_dynamo_table_name
    LOCK_DDB_TABLE_GROUPID_GSI_NAME = var.ingest_lock_table_group_id_gsi_name
    OUTPUT_BUCKET_NAME              = var.ingest_raw_cache_bucket_name
    SOURCE_SYSTEM                   = upper(var.source_name)
  }
  tags = {
    Name = local.package_builder_lambda_name
  }
}
