locals {
  importer_name             = "${local.environment}-dr2-preingest-${var.source_name}-importer"
  importer_queue_arn        = "arn:aws:sqs:eu-west-2:${data.aws_caller_identity.current.account_id}:${local.importer_name}"
  python_runtime            = "python3.12"
  python_lambda_memory_size = 128
  python_timeout_seconds    = 30
  sse_encryption            = "sse"
  visibility_timeout        = 180
  redrive_maximum_receives  = 5
}
module "dr2_importer_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  description     = "A lambda to validate incoming metadata and copy the files to the DR2 S3 bucket for ${upper(var.source_name)}"
  function_name   = local.importer_name
  handler         = "lambda_function.lambda_handler"
  timeout_seconds = local.python_timeout_seconds
  lambda_sqs_queue_mappings = [
    { sqs_queue_arn = local.importer_queue_arn, ignore_enabled_status = true }
  ]
  policies = {
    "${local.importer_name}-policy" = var.bucket_kms_arn == null ? templatefile("${path.module}/templates/copy_files_no_kms_policy.json.tpl", {
      copy_files_queue_arn  = local.importer_queue_arn
      raw_cache_bucket_name = var.ingest_raw_cache_bucket_name
      bucket_name           = var.copy_source_bucket_name
      aggregator_queue_arn  = module.dr2_preingest_aggregator_queue.sqs_arn
      account_id            = data.aws_caller_identity.current.account_id
      lambda_name           = local.importer_name
      }) : templatefile("${path.module}/templates/copy_files_with_kms_policy.json.tpl", {
      copy_files_queue_arn  = local.importer_queue_arn
      raw_cache_bucket_name = var.ingest_raw_cache_bucket_name
      bucket_name           = var.copy_source_bucket_name
      aggregator_queue_arn  = module.dr2_preingest_aggregator_queue.sqs_arn
      account_id            = data.aws_caller_identity.current.account_id
      lambda_name           = local.importer_name
      kms_arn               = var.bucket_kms_arn
    })
  }
  memory_size = local.python_lambda_memory_size
  runtime     = local.python_runtime
  plaintext_env_vars = {
    OUTPUT_BUCKET_NAME = var.ingest_raw_cache_bucket_name
    OUTPUT_QUEUE_URL   = module.dr2_preingest_aggregator_queue.sqs_queue_url
    SOURCE_SYSTEM      = var.source_name
  }
  tags = {
    Name = local.importer_name
  }
}


module "dr2_importer_sqs" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name = local.importer_name
  sqs_policy = var.sns_topic_arn == null ? templatefile("${path.module}/templates/sqs_access_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.importer_name
    }) : templatefile("${path.module}/templates/sns_send_message_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.importer_name
    topic_arn  = var.sns_topic_arn
  })
  redrive_maximum_receives = local.redrive_maximum_receives
  visibility_timeout       = local.visibility_timeout
  encryption_type          = local.sse_encryption
}

resource "aws_sns_topic_subscription" "dr2_importer_subscription" {
  count                = var.sns_topic_arn != null ? 1 : 0
  endpoint             = module.dr2_importer_sqs.sqs_arn
  protocol             = "sqs"
  topic_arn            = var.sns_topic_arn
  raw_message_delivery = true
  filter_policy_scope  = "MessageBody"
  filter_policy        = templatefile("${path.module}/templates/bucket_filter_policy.json.tpl", { bucket = var.copy_source_bucket_name })
}
