module "dr2_ingest_upsert_archive_folders_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda?ref=DR2-2511-do-not-ignore-filename-if-set"
  function_name   = var.lambda_names.upsert_folders
  s3_bucket       = local.code_deploy_bucket
  s3_key          = replace("${var.deploy_version}/${var.lambda_names.upsert_folders}", "${local.environment}-dr2-", "")
  handler         = "uk.gov.nationalarchives.ingestupsertarchivefolders.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${var.lambda_names.upsert_folders}-policy" = templatefile("./templates/iam_policy/ingest_upsert_archive_folders_policy.json.tpl", {
      account_id                 = data.aws_caller_identity.current.account_id
      lambda_name                = var.lambda_names.upsert_folders
      dynamo_db_file_table_arn   = var.files_table_arn
      secrets_manager_secret_arn = var.preservica_read_update_metadata_insert_content.arn
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    FILES_DDB_TABLE        = local.files_dynamo_table_name
    PRESERVICA_SECRET_NAME = var.preservica_read_update_metadata_insert_content.name
  }
  vpc_config = {
    subnet_ids         = var.private_subnets
    security_group_ids = local.outbound_security_group_ids
  }
  reserved_concurrency = 1
  tags = {
    Name = var.lambda_names.upsert_folders
  }
}
