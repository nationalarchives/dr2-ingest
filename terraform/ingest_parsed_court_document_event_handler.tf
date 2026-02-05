locals {
  ingest_parsed_court_document_event_handler_test_bucket_name = "${local.environment}-dr2-ingest-parsed-court-document-test-input"
  tre_prod_event_bus                                          = local.tre_terraform_prod_config["da_eventbus"]
}

module "dr2_ingest_parsed_court_document_event_handler_test_input_bucket" {
  count       = local.environment != "prod" ? 1 : 0
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name = local.ingest_parsed_court_document_event_handler_test_bucket_name
  bucket_policy = templatefile("./templates/s3/lambda_access_bucket_policy.json.tpl", {
    lambda_role_arns = jsonencode([module.court_document_preingest.importer_lambda_role_arn]),
    bucket_name      = local.ingest_parsed_court_document_event_handler_test_bucket_name
  })
  kms_key_arn = module.dr2_kms_key.kms_key_arn
}
