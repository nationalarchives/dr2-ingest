locals {
  court_document_anonymiser_lambda_name = "${local.environment}-dr2-court-document-package-anonymiser"
  court_document_anonymiser_queue_name  = "${local.environment}-dr2-court-document-package-anonymiser"
  court_document_anonymiser_queue_arn   = "arn:aws:sqs:eu-west-2:${data.aws_caller_identity.current.account_id}:${local.court_document_anonymiser_queue_name}"
  court_document_anonymiser_count       = local.environment == "intg" ? 1 : 0
  tre_terraform_prod_config             = module.tre_config.terraform_config["prod"]

}

module "dr2_court_document_package_anonymiser_lambda" {
  count           = local.court_document_anonymiser_count
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.court_document_anonymiser_lambda_name
  handler         = "bootstrap"
  timeout_seconds = 30
  lambda_sqs_queue_mappings = [{
    sqs_queue_arn = "arn:aws:sqs:eu-west-2:${data.aws_caller_identity.current.account_id}:${local.court_document_anonymiser_queue_name}"
  }]
  policies = {
    "${local.court_document_anonymiser_lambda_name}-policy" = templatefile("./templates/iam_policy/anonymiser_lambda_policy.json.tpl", {
      anonymiser_test_input_queue         = local.court_document_anonymiser_queue_arn
      ingest_court_document_handler_queue = module.court_document_preingest.importer_sqs.sqs_arn
      output_bucket_name                  = local.courtdoc_test_bucket_name
      account_id                          = data.aws_caller_identity.current.account_id
      lambda_name                         = local.court_document_anonymiser_lambda_name
      tre_bucket_arn                      = local.tre_terraform_prod_config["s3_court_document_pack_out_arn"]
      tre_kms_arn                         = module.tre_config.terraform_config["prod_s3_court_document_pack_out_kms_arn"]
    })
  }
  memory_size = 128
  runtime     = "provided.al2023"
  plaintext_env_vars = {
    OUTPUT_BUCKET = local.courtdoc_test_bucket_name
    OUTPUT_QUEUE  = module.court_document_preingest.importer_sqs.sqs_queue_url
  }
  tags = {}
}

module "dr2_court_document_package_anonymiser_sqs" {
  count                                             = local.court_document_anonymiser_count
  source                                            = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_cloudwatch_alarm_visible_messages_threshold = local.messages_visible_threshold
  queue_name                                        = local.court_document_anonymiser_queue_name
  sqs_policy = templatefile("./templates/sqs/sns_send_message_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.court_document_anonymiser_queue_name
    topic_arn  = local.tre_terraform_prod_config["da_eventbus"]
  })
  redrive_maximum_receives = local.redrive_maximum_receives
  visibility_timeout       = local.visibility_timeout
  kms_key_id               = module.dr2_kms_key.kms_key_arn
}

resource "aws_sns_topic_subscription" "tre_topic_subscription" {
  count                = local.court_document_anonymiser_count
  endpoint             = local.court_document_anonymiser_queue_arn
  protocol             = "sqs"
  topic_arn            = local.tre_terraform_prod_config["da_eventbus"]
  raw_message_delivery = true
  filter_policy_scope  = "MessageBody"
  filter_policy        = templatefile("${path.module}/templates/sns/tre_live_stream_filter_policy.json.tpl", {})
}
