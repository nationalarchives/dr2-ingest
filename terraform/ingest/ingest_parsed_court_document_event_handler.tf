locals {
  ingest_parsed_court_document_event_handler_queue_name  = "${local.environment}-dr2-ingest-parsed-court-document-event-handler"
  ingest_parsed_court_document_event_handler_queue_arn   = "arn:aws:sqs:eu-west-2:${data.aws_caller_identity.current.account_id}:${local.ingest_parsed_court_document_event_handler_queue_name}"
  ingest_parsed_court_document_event_handler_queue_url   = "https://sqs.eu-west-2.amazonaws.com/${data.aws_caller_identity.current.account_id}/${local.ingest_parsed_court_document_event_handler_queue_name}"
  ingest_parsed_court_document_event_handler_lambda_name = "${local.environment}-dr2-ingest-parsed-court-document-event-handler"
  court_document_lambda_policy_template_suffix           = local.environment == "prod" ? "_prod" : ""
  court_document_queue_sqs_policy                        = local.environment == "prod" ? "sns_send_message_policy" : "sqs_access_policy"
  tre_prod_event_bus                                     = local.tre_terraform_prod_config["da_eventbus"]
}

module "copy_from_tre_bucket_role" {
  count              = local.environment != "prod" ? 1 : 0
  source             = "git::https://github.com/nationalarchives/da-terraform-modules//iam_role"
  assume_role_policy = templatefile("${path.root}/templates/iam_role/aws_principal_assume_role.json.tpl", { aws_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" })
  name               = split("/", module.config.terraform_config[local.environment]["copy_from_tre_bucket_role"])[1]
  policy_attachments = {
    copy_from_tre_bucket_policy = module.copy_from_tre_bucket_policy[count.index].policy_arn
  }
  tags = {}
}

module "copy_from_tre_bucket_policy" {
  count         = local.environment != "prod" ? 1 : 0
  source        = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name          = "${local.environment}-copy-from-tre-bucket-policy"
  policy_string = templatefile("${path.root}/templates/iam_policy/assume_tre_role_policy.json.tpl", { tre_role = "arn:aws:iam::${module.tre_config.account_numbers["prod"]}:role/prod-tre-editorial-judgment-out-copier" })
}

module "dr2_ingest_parsed_court_document_event_handler_sqs" {
  source                                            = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_cloudwatch_alarm_visible_messages_threshold = local.messages_visible_threshold
  queue_name                                        = local.ingest_parsed_court_document_event_handler_queue_name
  sqs_policy = templatefile("./templates/sqs/${local.court_document_queue_sqs_policy}.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.ingest_parsed_court_document_event_handler_queue_name
    topic_arn  = local.tre_prod_event_bus
  })
  redrive_maximum_receives = local.redrive_maximum_receives
  visibility_timeout       = local.visibility_timeout
  encryption_type          = local.sse_encryption

}

module "dr2_ingest_parsed_court_document_event_handler_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.ingest_parsed_court_document_event_handler_lambda_name
  handler         = "uk.gov.nationalarchives.ingestparsedcourtdocumenteventhandler.Lambda::handleRequest"
  timeout_seconds = 60
  lambda_sqs_queue_mappings = [
    { sqs_queue_arn = "arn:aws:sqs:eu-west-2:${data.aws_caller_identity.current.account_id}:${local.ingest_parsed_court_document_event_handler_queue_name}", ignore_enabled_status = true }
  ]
  policies = {
    "${local.ingest_parsed_court_document_event_handler_lambda_name}-policy" = templatefile("./templates/iam_policy/ingest_parsed_court_document_event_handler_lambda_policy${local.court_document_lambda_policy_template_suffix}.json.tpl", {
      ingest_parsed_court_document_event_handler_queue_arn = module.dr2_ingest_parsed_court_document_event_handler_sqs.sqs_arn
      bucket_name                                          = var.bucket_names.ingest_raw_cache_bucket_name
      account_id                                           = data.aws_caller_identity.current.account_id
      lambda_name                                          = local.ingest_parsed_court_document_event_handler_lambda_name
      step_function_arn                                    = module.dr2_ingest_step_function.step_function_arn
      tre_kms_arn                                          = module.tre_config.terraform_config["prod_s3_court_document_pack_out_kms_arn"]
      tre_bucket_arn                                       = local.tre_terraform_prod_config["s3_court_document_pack_out_arn"]
      dynamo_db_lock_table_arn                             = var.ingest_lock_table.arn
    })
  }
  memory_size = 1024
  runtime     = local.java_runtime
  plaintext_env_vars = {
    INGEST_SFN_ARN     = module.dr2_ingest_step_function.step_function_arn
    LOCK_DDB_TABLE     = var.ingest_lock_table.name
    OUTPUT_BUCKET_NAME = var.bucket_names.ingest_raw_cache_bucket_name
  }
  tags = {
    Name = local.ingest_parsed_court_document_event_handler_lambda_name
  }
}

resource "aws_sns_topic_subscription" "dr2_tre_topic_court_document_subscription" {
  # Only do this for prod now. We might do staging if that ends up pointing to prod Preservica
  count                = local.environment == "prod" ? 1 : 0
  endpoint             = module.dr2_ingest_parsed_court_document_event_handler_sqs.sqs_arn
  protocol             = "sqs"
  topic_arn            = local.tre_prod_event_bus
  raw_message_delivery = true
  filter_policy_scope  = "MessageBody"
  filter_policy        = templatefile("${path.root}/templates/sns/tre_live_stream_filter_policy.json.tpl", {})
}
