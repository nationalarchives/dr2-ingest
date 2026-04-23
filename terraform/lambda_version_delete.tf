locals {
  delete_lambda_version_name = "${local.environment}-dr2-delete-lambda-version"
}
module "dr2_delete_lambda_version_schedule" {
  source                  = "git::https://github.com/nationalarchives/da-terraform-modules//cloudwatch_events?ref=main"
  rule_name               = "${local.delete_lambda_version_name}-schedule"
  schedule                = "rate(1 day)"
  lambda_event_target_arn = "arn:aws:lambda:eu-west-2:${data.aws_caller_identity.current.account_id}:function:${local.delete_lambda_version_name}"
}

data "archive_file" "delete_lambda_version_code" {
  type        = "zip"
  source_file = "${path.module}/templates/lambda/delete_lambda_versions.py"
  output_path = "${path.module}/templates/lambda/function.zip"
}

module "dr2_delete_lambda_version_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda?ref=main"
  function_name   = local.delete_lambda_version_name
  handler         = "delete_lambda_versions.lambda_handler"
  filename        = data.archive_file.delete_lambda_version_code.output_path
  timeout_seconds = 60
  memory_size     = local.python_lambda_memory_size
  runtime         = local.python_runtime
  tags            = {}
  policies = {
    "${local.delete_lambda_version_name}-policy" = templatefile("${path.module}/templates/iam_policy/version_delete.json.tpl", {
      account_id  = data.aws_caller_identity.current.account_id,
      lambda_name = local.delete_lambda_version_name,
      lambdas_to_delete = jsonencode(flatten([
        for l in [
          module.dr2_ingest_mapper_lambda.lambda_function,
          module.dr2_ingest_validate_generic_ingest_inputs_lambda.lambda_function,
          module.ingest_find_existing_asset.lambda_function,
          module.dr2_ingest_asset_opex_creator_lambda.lambda_function,
          module.dr2_ingest_folder_opex_creator_lambda.lambda_function,
          module.dr2_ingest_parent_folder_opex_creator_lambda.lambda_function,
          module.dr2_ingest_asset_reconciler_lambda.lambda_function,
          module.dr2_ingest_flow_control_lambda.lambda_function,
          module.dr2_ingest_upsert_archive_folders_lambda.lambda_function,
          module.dr2_ingest_start_workflow_lambda.lambda_function,
          module.dr2_ingest_workflow_monitor_lambda.lambda_function,
          module.cc_restore_preingest.package_builder_lambda,
          module.dri_preingest.package_builder_lambda,
          module.tdr_preingest.package_builder_lambda,
          module.ad_hoc_preingest.package_builder_lambda,
          module.court_document_preingest.package_builder_lambda
        ] : ["${l.arn}:*", l.arn]
      ]))
    })
  }
}