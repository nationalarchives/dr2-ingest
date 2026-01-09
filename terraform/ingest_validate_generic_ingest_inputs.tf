locals {
  ingest_validate_generic_ingest_inputs_lambda_name = "${local.environment}-dr2-ingest-validate-generic-ingest-inputs"
}

module "dr2_ingest_validate_generic_ingest_inputs_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.ingest_validate_generic_ingest_inputs_lambda_name
  handler         = "uk.gov.nationalarchives.ingestvalidategenericingestinputs.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${local.ingest_validate_generic_ingest_inputs_lambda_name}-policy" = templatefile("./templates/iam_policy/ingest_validate_generic_ingest_inputs_policy.json.tpl", {
      raw_cache_bucket_name   = local.ingest_raw_cache_bucket_name
      account_id              = data.aws_caller_identity.current.account_id
      lambda_name             = local.ingest_validate_generic_ingest_inputs_lambda_name
      adhoc_cache_bucket_name = local.adhoc_bucket_name
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  vpc_config = {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [module.outbound_https_access_only.security_group_id, module.outbound_https_access_for_s3.security_group_id]
  }
  tags = {
    Name = local.ingest_validate_generic_ingest_inputs_lambda_name
  }
}
