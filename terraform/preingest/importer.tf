terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.36.0"
    }
  }
}
locals {
  importer_key              = "preingest-${var.source_name}-importer"
  importer_name             = "${local.environment}-dr2-${local.importer_key}"
  importer_queue_arn        = "arn:aws:sqs:eu-west-2:${data.aws_caller_identity.current.account_id}:${local.importer_name}"
  sse_encryption            = "sse"
  visibility_timeout        = 180
  redrive_maximum_receives  = 5
  source_bucket_permissions = var.delete_from_source ? ["s3:GetObject", "s3:GetObjectTagging", "s3:DeleteObject"] : ["s3:GetObject", "s3:GetObjectTagging"]
  vpc_arns                  = var.vpc_arn == null || var.vpc_arn == "" ? [] : [var.vpc_arn]
}
data "aws_iam_policy_document" "importer_policy" {
  dynamic "statement" {
    for_each = var.bucket_kms_arn == null ? [] : [var.bucket_kms_arn]
    content {
      sid       = "DecryptWithKey"
      effect    = "Allow"
      actions   = ["kms:Decrypt"]
      resources = [statement.value]
    }
  }

  statement {
    sid       = "readSqs"
    effect    = "Allow"
    actions   = ["sqs:ReceiveMessage", "sqs:GetQueueAttributes", "sqs:DeleteMessage"]
    resources = [local.importer_queue_arn]
  }

  statement {
    sid     = "readWriteIngestRawCache"
    effect  = "Allow"
    actions = ["s3:PutObject*", "s3:GetObject", "s3:DeleteObject"]
    resources = [
      "arn:aws:s3:::${var.ingest_raw_cache_bucket_name}",
      "arn:aws:s3:::${var.ingest_raw_cache_bucket_name}/*"
    ]
    dynamic "condition" {
      for_each = local.vpc_arns
      content {
        test     = "ArnEquals"
        variable = "aws:SourceVpcArn"
        values   = [condition.value]
      }
    }
  }

  statement {
    sid     = "sourceBucketPermissions"
    effect  = "Allow"
    actions = local.source_bucket_permissions
    resources = [
      var.copy_source_bucket_arn,
      "${var.copy_source_bucket_arn}/*"
    ]
    dynamic "condition" {
      for_each = local.vpc_arns
      content {
        test     = "ArnEquals"
        variable = "aws:SourceVpcArn"
        values   = [condition.value]
      }
    }
  }

  statement {
    sid       = "sendSqsMessage"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [module.dr2_preingest_aggregator_queue.sqs_arn]
  }

  statement {
    sid     = "readWriteLogs"
    effect  = "Allow"
    actions = ["logs:PutLogEvents", "logs:CreateLogStream", "logs:CreateLogGroup"]
    resources = [
      "arn:aws:logs:eu-west-2:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.importer_name}:*:*",
      "arn:aws:logs:eu-west-2:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.importer_name}:*"
    ]
  }
}

module "dr2_importer_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  description     = "A lambda to validate incoming metadata and copy the files to the DR2 S3 bucket for ${upper(var.source_name)}"
  function_name   = local.importer_name
  handler         = var.importer_lambda.handler
  timeout_seconds = var.importer_lambda.timeout
  lambda_sqs_queue_mappings = [
    { sqs_queue_arn = local.importer_queue_arn, ignore_enabled_status = true }
  ]
  policies = merge({
    "${local.importer_name}-policy" = data.aws_iam_policy_document.importer_policy.json
  }, var.additional_importer_lambda_policies)
  memory_size  = var.importer_lambda.memory_size
  runtime      = var.importer_lambda.runtime
  architecture = var.importer_lambda.architecture
  s3_bucket    = local.code_deploy_bucket
  s3_key       = "${var.lambda_code_version}/${local.importer_key}"
  plaintext_env_vars = merge(
    {
      OUTPUT_BUCKET_NAME = var.ingest_raw_cache_bucket_name
      OUTPUT_QUEUE_URL   = module.dr2_preingest_aggregator_queue.sqs_queue_url
      SOURCE_SYSTEM      = var.source_name
    },
    var.additional_importer_lambda_env_vars,
    var.delete_from_source ? { "DELETE_FROM_SOURCE" = "true" } : {}
  )
  tags = {
    Name = local.importer_name
  }
  vpc_config = {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = var.private_security_group_ids
  }
}


module "dr2_importer_sqs" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name = local.importer_name
  sqs_policy = var.sns_topic_subscription == null ? templatefile("${path.module}/templates/sqs_access_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.importer_name
    }) : templatefile("${path.module}/templates/sns_send_message_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.importer_name
    topic_arn  = var.sns_topic_subscription.topic_arn
  })
  queue_cloudwatch_alarm_visible_messages_threshold = local.messages_visible_threshold
  redrive_maximum_receives                          = local.redrive_maximum_receives
  visibility_timeout                                = var.importer_lambda.visibility_timeout
  encryption_type                                   = local.sse_encryption
}

resource "aws_sns_topic_subscription" "dr2_importer_subscription" {
  count                = var.sns_topic_subscription != null ? 1 : 0
  endpoint             = module.dr2_importer_sqs.sqs_arn
  protocol             = "sqs"
  topic_arn            = var.sns_topic_subscription.topic_arn
  raw_message_delivery = true
  filter_policy_scope  = "MessageBody"
  filter_policy        = var.sns_topic_subscription.filter_policy
}
