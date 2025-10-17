locals {
  ingest_upsert_archive_folders_lambda_name = "${local.environment}-dr2-ingest-upsert-archive-folders"
}

module "dr2_ingest_upsert_archive_folders_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.ingest_upsert_archive_folders_lambda_name
  handler         = "uk.gov.nationalarchives.ingestupsertarchivefolders.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${local.ingest_upsert_archive_folders_lambda_name}-policy" = templatefile("./templates/iam_policy/ingest_upsert_archive_folders_policy.json.tpl", {
      account_id                 = data.aws_caller_identity.current.account_id
      lambda_name                = local.ingest_upsert_archive_folders_lambda_name
      dynamo_db_file_table_arn   = module.files_table.table_arn
      secrets_manager_secret_arn = aws_secretsmanager_secret.preservica_read_update_metadata_insert_content.arn
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    FILES_DDB_TABLE        = local.files_dynamo_table_name
    PRESERVICA_SECRET_NAME = aws_secretsmanager_secret.preservica_read_update_metadata_insert_content.name
  }
  vpc_config = {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = local.outbound_security_group_ids
  }
  reserved_concurrency = 1
  tags = {
    Name = local.ingest_upsert_archive_folders_lambda_name
  }
}
