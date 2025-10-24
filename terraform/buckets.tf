module "ingest_state_bucket" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name = local.ingest_state_bucket_name
  kms_key_arn = module.dr2_developer_key.kms_key_arn
}

module "dr2_ingest_parsed_court_document_event_handler_test_input_bucket" {
  count       = local.environment != "prod" ? 1 : 0
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name = local.ingest_parsed_court_document_event_handler_test_bucket_name
  bucket_policy = templatefile("./templates/s3/lambda_access_bucket_policy.json.tpl", {
    lambda_role_arns = jsonencode([module.dr2_ingest_parsed_court_document_event_handler_lambda.lambda_role_arn, "arn:aws:iam::${module.tre_config.account_numbers["prod"]}:role/prod-tre-editorial-judgment-out-copier"]),
    bucket_name      = local.ingest_parsed_court_document_event_handler_test_bucket_name
  })
  kms_key_arn = module.dr2_kms_key.kms_key_arn
}

module "ingest_raw_cache_bucket" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name = local.ingest_raw_cache_bucket_name
  kms_key_arn = module.dr2_kms_key.kms_key_arn
}

module "sample_files_bucket" {
  source            = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name       = local.sample_files_bucket_name
  create_log_bucket = false
  kms_key_arn       = module.dr2_kms_key.kms_key_arn
}
