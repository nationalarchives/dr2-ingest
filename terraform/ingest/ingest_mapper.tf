locals {
  ingest_mapper_lambda_name = "${local.environment}-dr2-ingest-mapper"
}

module "dr2_ingest_mapper_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda?ref=DR2-2511-do-not-ignore-filename-if-set"
  function_name   = local.ingest_mapper_lambda_name
  s3_bucket       = local.code_deploy_bucket
  s3_key          = replace("${var.deploy_version}/${local.ingest_mapper_lambda_name}", "${local.environment}-dr2-", "")
  handler         = "uk.gov.nationalarchives.ingestmapper.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${local.ingest_mapper_lambda_name}-policy" = templatefile("./templates/iam_policy/ingest_mapper_policy.json.tpl", {
      raw_cache_bucket_name    = var.ingest_raw_cache_bucket_name
      ingest_state_bucket_name = local.ingest_state_bucket_name
      account_id               = data.aws_caller_identity.current.account_id
      lambda_name              = local.ingest_mapper_lambda_name
      dynamo_db_file_table_arn = var.files_table_arn
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    FILES_DDB_TABLE    = local.files_dynamo_table_name
    OUTPUT_BUCKET_NAME = local.ingest_state_bucket_name
  }
  vpc_config = {
    subnet_ids         = var.private_subnets
    security_group_ids = [var.outbound_https_access_only_id, var.discovery_security_group_id]
  }
  tags = {
    Name = local.ingest_mapper_lambda_name
  }
}

