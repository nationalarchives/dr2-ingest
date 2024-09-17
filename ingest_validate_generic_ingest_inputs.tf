locals {
  ingest_validate_generic_ingest_inputs_lambda_name = "${local.environment}-dr2-ingest-validate-generic-ingest-inputs"
}

module "dr2_ingest_validate_generic_ingest_inputs_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.ingest_validate_generic_ingest_inputs_lambda_name
  handler         = "uk.gov.nationalarchives.ingestmapper.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${local.ingest_validate_generic_ingest_inputs_lambda_name}-policy" = templatefile("./templates/iam_policy/ingest_validate_generic_ingest_inputs_policy.json.tpl", {
      bucket_name = local.ingest_raw_cache_bucket_name
      account_id  = var.account_number
      lambda_name = local.ingest_validate_generic_ingest_inputs_lambda_name
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  tags = {
    Name = local.ingest_validate_generic_ingest_inputs_lambda_name
  }
}
