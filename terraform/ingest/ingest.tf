locals {
  environment                            = var.environment
  ingest_state_bucket_name               = "${local.environment}-dr2-ingest-state"
  ingest_step_function_name              = "${local.environment}-dr2-ingest"
  ingest_run_workflow_step_function_name = "${local.environment}-dr2-ingest-run-workflow"
  files_dynamo_table_name                = "${local.environment}-dr2-ingest-files"
  ingest_lock_dynamo_table_name          = "${local.environment}-dr2-ingest-lock"
  ingest_queue_dynamo_table_name         = "${local.environment}-dr2-ingest-queue"
  ingest_lock_table_group_id_gsi_name    = "IngestLockGroupIdx"
  ingest_lock_table_hash_key             = "assetId"
  preservica_tenant                      = local.environment == "prod" ? "tna" : "tnatest"
  preservica_ingest_bucket               = "com.preservica.${local.preservica_tenant}.bulk1"
  tna_to_preservica_role_arn             = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.environment}-tna-to-preservica-ingest-s3-${local.preservica_tenant}"
  ingest_run_workflow_sfn_arn            = "arn:aws:states:eu-west-2:${data.aws_caller_identity.current.account_id}:stateMachine:${local.ingest_run_workflow_step_function_name}"
  retry_statement                        = jsonencode([{ ErrorEquals = ["States.ALL"], IntervalSeconds = 2, MaxAttempts = 6, BackoffRate = 2, JitterStrategy = "FULL" }])
  messages_visible_threshold             = 1000000
  java_runtime                           = "java21"
  java_lambda_memory_size                = 512
  java_timeout_seconds                   = 180
  sse_encryption                         = "sse"
  visibility_timeout                     = 180
  redrive_maximum_receives               = 5
  # The list comes from https://www.cloudflare.com/en-gb/ips
  outbound_security_group_ids = [var.outbound_https_access_only_id, var.outbound_cloudflare_https_access_id]
  lambdas = [
    module.ingest_find_existing_asset.lambda_function,
    module.dr2_ingest_validate_generic_ingest_inputs_lambda.lambda_function,
    module.dr2_ingest_parsed_court_document_event_handler_lambda.lambda_function,
    module.dr2_ingest_mapper_lambda.lambda_function,
    module.dr2_ingest_asset_opex_creator_lambda.lambda_function,
    module.dr2_ingest_folder_opex_creator_lambda.lambda_function,
    module.dr2_ingest_upsert_archive_folders_lambda.lambda_function,
    module.dr2_ingest_parent_folder_opex_creator_lambda.lambda_function,
    module.dr2_ingest_asset_reconciler_lambda.lambda_function,
    module.tdr_preingest.aggregator_lambda,
    module.tdr_preingest.package_builder_lambda,
    module.tdr_preingest.importer_lambda,
    module.dri_preingest.aggregator_lambda,
    module.dri_preingest.package_builder_lambda,
    module.dri_preingest.importer_lambda,
    module.dr2_ingest_start_workflow_lambda.lambda_function,
    module.dr2_ingest_failure_notifications_lambda.lambda_function,
    module.dr2_ingest_workflow_monitor_lambda.lambda_function
  ]
  lambdas_by_name    = { for lambda in local.lambdas : (lambda.function_name) => lambda }
  code_deploy_bucket = "mgmt-dp-code-deploy"
}

data "aws_caller_identity" "current" {}

module "dr2_ingest_step_function" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//sfn"
  step_function_definition = templatefile("${path.root}/templates/sfn/ingest_sfn_definition.json.tpl", {
    step_function_name                                = local.ingest_step_function_name,
    account_id                                        = data.aws_caller_identity.current.account_id
    ingest_validate_generic_ingest_inputs_lambda_name = local.ingest_validate_generic_ingest_inputs_lambda_name
    ingest_mapper_lambda_name                         = local.ingest_mapper_lambda_name
    ingest_find_existing_asset_name_lambda_name       = local.ingest_find_existing_asset_name
    ingest_asset_opex_creator_lambda_name             = local.ingest_asset_opex_creator_lambda_name
    ingest_folder_opex_creator_lambda_name            = local.ingest_folder_opex_creator_lambda_name
    ingest_parent_folder_opex_creator_lambda_name     = local.ingest_parent_folder_opex_creator_lambda_name
    ingest_asset_reconciler_lambda_name               = local.ingest_asset_reconciler_lambda_name
    ingest_lock_table_name                            = local.ingest_lock_dynamo_table_name
    ingest_lock_table_group_id_gsi_name               = local.ingest_lock_table_group_id_gsi_name
    ingest_lock_table_hash_key                        = local.ingest_lock_table_hash_key
    ingest_run_workflow_sfn_name                      = local.ingest_run_workflow_step_function_name
    notifications_topic_name                          = var.notifications_topic.name
    ingest_state_bucket_name                          = local.ingest_state_bucket_name
    preservica_bucket_name                            = local.preservica_ingest_bucket
    ingest_files_table_name                           = local.files_dynamo_table_name
    ingest_queue_table_name                           = local.ingest_queue_dynamo_table_name
    ingest_flow_control_lambda_name                   = local.ingest_flow_control_lambda_name
    retry_statement                                   = local.retry_statement
    postingest_table_name                             = module.postingest.postingest_table_name
  })
  step_function_name = local.ingest_step_function_name
  step_function_role_policy_attachments = {
    step_function_policy = module.dr2_ingest_step_function_policy.policy_arn
  }
}

module "dr2_ingest_run_workflow_step_function" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//sfn"
  step_function_definition = templatefile("${path.root}/templates/sfn/ingest_run_workflow_sfn_definition.json.tpl", {
    step_function_name                        = local.ingest_run_workflow_step_function_name
    account_id                                = data.aws_caller_identity.current.account_id
    ingest_upsert_archive_folders_lambda_name = local.ingest_upsert_archive_folders_lambda_name
    ingest_start_workflow_lambda_name         = local.ingest_start_workflow_lambda_name
    ingest_workflow_monitor_lambda_name       = local.ingest_workflow_monitor_lambda_name
    retry_statement                           = local.retry_statement,
    upsert_lambda_retry_statement             = jsonencode([{ ErrorEquals = ["States.ALL"], IntervalSeconds = module.dr2_ingest_upsert_archive_folders_lambda.lambda_function.timeout, MaxAttempts = 10, BackoffRate = 1, JitterStrategy = "FULL" }])
  })
  step_function_name = local.ingest_run_workflow_step_function_name
  step_function_role_policy_attachments = {
    step_function_policy = module.dr2_ingest_run_workflow_step_function_policy.policy_arn
  }
}

module "dr2_ingest_step_function_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.environment}-dr2-ingest-step-function-policy"
  policy_string = templatefile("${path.root}/templates/iam_policy/ingest_step_function_policy.json.tpl", {
    account_id                                        = data.aws_caller_identity.current.account_id
    ingest_validate_generic_ingest_inputs_lambda_name = local.ingest_validate_generic_ingest_inputs_lambda_name
    ingest_mapper_lambda_name                         = local.ingest_mapper_lambda_name
    ingest_upsert_archive_folders_lambda_name         = local.ingest_upsert_archive_folders_lambda_name
    ingest_find_existing_asset_lambda_name            = local.ingest_find_existing_asset_name
    ingest_asset_opex_creator_lambda_name             = local.ingest_asset_opex_creator_lambda_name
    ingest_folder_opex_creator_lambda_name            = local.ingest_folder_opex_creator_lambda_name
    ingest_parent_folder_opex_creator_lambda_name     = local.ingest_parent_folder_opex_creator_lambda_name
    ingest_start_workflow_lambda_name                 = local.ingest_start_workflow_lambda_name
    ingest_workflow_monitor_lambda_name               = local.ingest_workflow_monitor_lambda_name
    ingest_asset_reconciler_lambda_name               = local.ingest_asset_reconciler_lambda_name
    ingest_flow_control_lambda_name                   = local.ingest_flow_control_lambda_name
    ingest_lock_table_name                            = local.ingest_lock_dynamo_table_name
    ingest_lock_table_group_id_gsi_name               = local.ingest_lock_table_group_id_gsi_name
    notifications_topic_name                          = var.notifications_topic.name
    ingest_queue_table_name                           = local.ingest_queue_dynamo_table_name
    ingest_state_bucket_name                          = local.ingest_state_bucket_name
    ingest_sfn_name                                   = local.ingest_step_function_name
    ingest_run_workflow_sfn_name                      = local.ingest_run_workflow_step_function_name
    ingest_files_table_name                           = local.files_dynamo_table_name
    tna_to_preservica_role_arn                        = local.tna_to_preservica_role_arn
    preingest_tdr_step_function_arn                   = module.tdr_preingest.preingest_sfn_arn
    preingest_dri_step_function_arn                   = module.dri_preingest.preingest_sfn_arn
    ingest_run_workflow_sfn_arn                       = local.ingest_run_workflow_sfn_arn
    postingest_table_name                             = module.postingest.postingest_table_name
  })
}

module "dr2_ingest_run_workflow_step_function_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.environment}-dr2-ingest-run-workflow-step-function-policy"
  policy_string = templatefile("${path.root}/templates/iam_policy/ingest_run_workflow_step_function_policy.json.tpl", {
    account_id                                = data.aws_caller_identity.current.account_id
    ingest_upsert_archive_folders_lambda_name = local.ingest_upsert_archive_folders_lambda_name
    ingest_start_workflow_lambda_name         = local.ingest_start_workflow_lambda_name
    ingest_workflow_monitor_lambda_name       = local.ingest_workflow_monitor_lambda_name,
    ingest_step_function_name                 = local.ingest_step_function_name
  })
}
