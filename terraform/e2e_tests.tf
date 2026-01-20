locals {
  e2e_tests_name  = "${local.environment}-dr2-e2e-tests"
  e2e_tests_count = local.environment == "intg" ? 1 : 0
}

module "dr2_run_e2e_tests_role" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_role"
  count  = local.e2e_tests_count
  assume_role_policy = templatefile("${path.module}/templates/iam_role/github_assume_role.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    repo_filters = jsonencode([
      "repo:nationalarchives/dr2-ingest:environment:${local.environment}"
    ])
  })
  name = "${local.environment}-dr2-run-e2e-tests-role"
  policy_attachments = {
    run_e2e_tests_policy = module.dr2_e2e_tests_policy[count.index].policy_arn
  }
  max_session_duration = 7200
  tags                 = {}
}

module "dr2_e2e_tests_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  count  = local.e2e_tests_count
  name   = "${local.e2e_tests_name}-policy"
  policy_string = templatefile("${path.module}/templates/iam_policy/e2e_tests_policy.json.tpl", {
    input_bucket_name                = local.ingest_raw_cache_bucket_name
    copy_files_from_tdr_queue        = module.tdr_preingest.importer_sqs.sqs_arn
    judgment_input_queue             = module.court_document_preingest.importer_sqs.sqs_arn
    preingest_sfn_arn                = module.tdr_preingest.preingest_sfn_arn
    dynamo_db_lock_table_arn         = module.ingest_lock_table.table_arn
    external_notifications_log_group = aws_cloudwatch_log_group.external_notification_log_group.arn
    copy_files_from_tdr_log_group    = "arn:aws:logs:eu-west-2:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${module.tdr_preingest.importer_lambda.function_name}"
  })
}
