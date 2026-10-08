locals {
  ingest_find_existing_asset_key_name  = "ingest-find-existing-asset"
  ingest_find_existing_asset_name      = "${local.environment}-dr2-${local.ingest_find_existing_asset_key_name}"
  ingest_find_existing_asset_queue_arn = "arn:aws:sqs:eu-west-2:${data.aws_caller_identity.current.account_id}:${local.ingest_find_existing_asset_name}"
}

module "ingest_find_existing_asset" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.ingest_find_existing_asset_name
  handler         = "uk.gov.nationalarchives.ingestfindexistingasset.Lambda::handleRequest"
  timeout_seconds = 60
  policies = {
    "${local.ingest_find_existing_asset_name}-policy" = templatefile(
      "${path.module}/templates/iam_policy/ingest_find_existing_asset_policy.json.tpl", {
        account_id                 = data.aws_caller_identity.current.account_id
        queue_arn                  = module.ingest_find_existing_asset_queue.sqs_queue.arn
        lambda_name                = local.ingest_find_existing_asset_name
        dynamo_db_file_table_arn   = module.files_table.table_arn
        secrets_manager_secret_arn = aws_secretsmanager_secret.preservica_read_metadata.arn
        vpc_id                     = module.vpc.vpc.id
        ingest_sfn_arn             = module.dr2_ingest_step_function.step_function_arn
      }
    )
  }
  publish_version = true
  snap_start      = true
  s3_bucket       = local.code_deploy_bucket
  s3_key          = "${var.lambda_code_version}/${local.ingest_find_existing_asset_key_name}"
  memory_size     = local.java_lambda_memory_size
  runtime         = local.java_runtime
  architecture    = local.architecture_arm64
  plaintext_env_vars = {
    FILES_DDB_TABLE        = local.files_dynamo_table_name
    PRESERVICA_SECRET_NAME = aws_secretsmanager_secret.preservica_read_metadata.name
  }
  vpc_config = {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = flatten([local.clouflare_and_vpc_endpoints_security_groups, [module.outbound_https_access_for_dynamo_db.security_group_id]])
  }
  lambda_sqs_queue_mappings = [{
    sqs_queue_arn         = local.ingest_find_existing_asset_queue_arn
    sqs_queue_concurrency = 2
  }]
  tags = {
    Name        = local.ingest_find_existing_asset_name
    SfnFunction = "true"
  }
}

module "ingest_find_existing_asset_queue" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name = local.ingest_find_existing_asset_name
  sqs_policy = templatefile("./templates/sqs/sqs_access_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.ingest_find_existing_asset_name
  })
  message_retention_seconds                         = local.find_asset_heartbeat
  queue_cloudwatch_alarm_visible_messages_threshold = local.messages_visible_threshold
  visibility_timeout                                = 300
  encryption_type                                   = local.sse_encryption
  create_dlq                                        = false
}
