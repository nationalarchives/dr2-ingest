locals {
  ingest_parsed_court_document_event_handler_queue_name       = "${local.environment}-dr2-ingest-parsed-court-document-event-handler"
  ingest_parsed_court_document_event_handler_queue_arn        = "arn:aws:sqs:eu-west-2:${data.aws_caller_identity.current.account_id}:${local.ingest_parsed_court_document_event_handler_queue_name}"
  ingest_parsed_court_document_event_handler_queue_url        = "https://sqs.eu-west-2.amazonaws.com/${data.aws_caller_identity.current.account_id}/${local.ingest_parsed_court_document_event_handler_queue_name}"
  ingest_parsed_court_document_event_handler_test_bucket_name = "${local.environment}-dr2-ingest-parsed-court-document-test-input"
  ingest_parsed_court_document_event_handler_lambda_name      = "${local.environment}-dr2-ingest-parsed-court-document-event-handler"
  court_document_lambda_policy_template_suffix                = local.environment == "prod" ? "_prod" : ""
  court_document_queue_sqs_policy                             = local.environment == "prod" ? "sns_send_message_policy" : "sqs_access_policy"
  tre_prod_event_bus                                          = local.tre_terraform_prod_config["da_eventbus"]
}

module "dr2_ingest_parsed_court_document_event_handler_test_input_bucket" {
  count       = local.environment != "prod" ? 1 : 0
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name = local.ingest_parsed_court_document_event_handler_test_bucket_name
  bucket_policy = templatefile("./templates/s3/lambda_access_bucket_policy.json.tpl", {
    lambda_role_arns = jsonencode([module.dr2_ingest_parsed_court_document_event_handler_lambda.lambda_role_arn]),
    bucket_name      = local.ingest_parsed_court_document_event_handler_test_bucket_name
  })
  kms_key_arn = module.dr2_kms_key.kms_key_arn
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
  policies = {
    "${local.ingest_parsed_court_document_event_handler_lambda_name}-policy" = templatefile("./templates/iam_policy/ingest_parsed_court_document_event_handler_lambda_policy${local.court_document_lambda_policy_template_suffix}.json.tpl", {
      ingest_parsed_court_document_event_handler_queue_arn = module.dr2_ingest_parsed_court_document_event_handler_sqs.sqs_arn
      bucket_name                                          = local.ingest_raw_cache_bucket_name
      account_id                                           = data.aws_caller_identity.current.account_id
      lambda_name                                          = local.ingest_parsed_court_document_event_handler_lambda_name
      step_function_arn                                    = module.dr2_ingest_step_function.step_function_arn
      tre_kms_arn                                          = module.tre_config.terraform_config["prod_s3_court_document_pack_out_kms_arn"]
      tre_bucket_arn                                       = local.tre_terraform_prod_config["s3_court_document_pack_out_arn"]
      dynamo_db_lock_table_arn                             = module.ingest_lock_table.table_arn
      vpc_id                                               = module.vpc.vpc_id
    })
  }
  memory_size = 1024
  runtime     = local.java_runtime
  plaintext_env_vars = {
    INGEST_SFN_ARN     = module.dr2_ingest_step_function.step_function_arn
    LOCK_DDB_TABLE     = local.ingest_lock_dynamo_table_name
    OUTPUT_BUCKET_NAME = local.ingest_raw_cache_bucket_name
  }
  vpc_config = {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [module.outbound_https_access_for_s3.security_group_id, module.https_to_vpc_endpoints_security_group.security_group_id, module.outbound_https_access_for_dynamo_db.security_group_id]
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
  filter_policy        = templatefile("${path.module}/templates/sns/tre_live_stream_filter_policy.json.tpl", {})
}
