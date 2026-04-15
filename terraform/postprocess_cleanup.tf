locals {
  cleanup_trigger_queue_name         = "${local.environment}-dr2-cleanup-trigger-queue"
  notifications_topic_arn            = "arn:aws:sns:${local.aws_region_name}:${data.aws_caller_identity.current.account_id}:${local.notifications_topic_name}"
  cleanup_lambda_name                = "${local.environment}-dr2-postprocess-cleanup-handler"
  sqs_queue_arn                      = "arn:aws:sqs:${local.aws_region_name}:${data.aws_caller_identity.current.account_id}:${local.cleanup_trigger_queue_name}"
  cleanup_messages_visible_threshold = 100
}

module "cleanup_trigger_queue" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name = local.cleanup_trigger_queue_name
  sqs_policy = templatefile("./templates/sqs/sns_send_message_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.cleanup_trigger_queue_name
    topic_arn  = local.notifications_topic_arn
  })
  queue_cloudwatch_alarm_visible_messages_threshold = local.cleanup_messages_visible_threshold
  encryption_type                                   = local.sse_encryption
  visibility_timeout                                = local.visibility_timeout
}

resource "aws_sns_topic_subscription" "cleanup_trigger_queue_subscription" {
  endpoint             = local.sqs_queue_arn
  protocol             = "sqs"
  topic_arn            = local.notifications_topic_arn
  raw_message_delivery = true
  filter_policy_scope  = "MessageBody"
  filter_policy        = templatefile("./templates/sns/cleanup_filter_policy.json.tpl", {})
}

module "cleanup_handler_lambda" {
  source        = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name = local.cleanup_lambda_name
  handler       = "uk.gov.nationalarchives.postprocesscleanup.Lambda::handleRequest"
  policies = {
    "${local.cleanup_lambda_name}-policy" = templatefile("./templates/iam_policy/cleanup_lambda_policy.json.tpl", {
      account_id         = data.aws_caller_identity.current.account_id
      lambda_name        = local.cleanup_lambda_name
      bucket_name        = module.ingest_raw_cache_bucket.s3_bucket_name
      region_name        = local.aws_region_name
      dynamodb_table_arn = module.files_table.table_arn
      index_name         = local.files_table_batch_parent_global_secondary_index_name
      sqs_queue_arn      = local.sqs_queue_arn
    })
  }
  timeout_seconds = 900
  memory_size     = 1024
  runtime         = local.java_runtime
  tags            = {}
  lambda_sqs_queue_mappings = [{
    sqs_queue_arn = local.sqs_queue_arn
  }]
  vpc_config = {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [module.outbound_https_access_for_s3.security_group_id, module.outbound_https_access_for_dynamo_db.security_group_id]
  }
  plaintext_env_vars = {
    FILES_DDB_TABLE                      = local.files_dynamo_table_name
    FILES_DDB_TABLE_BATCHPARENT_GSI_NAME = local.files_table_batch_parent_global_secondary_index_name
    RAW_CACHE_BUCKET_NAME                = local.ingest_raw_cache_bucket_name
  }
}

