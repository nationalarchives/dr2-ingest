data "aws_ssm_parameter" "dev_admin_role" {
  name = "/${local.environment}/developer_role"
}

module "dr2_kms_key" {
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//kms"
  key_name = "${local.environment}-kms-dr2"
  default_policy_variables = {
    user_roles = concat([
      data.aws_iam_role.org_wiz_access_role.arn,
      module.ingest_find_existing_asset.lambda_role_arn,
      module.ingest_find_existing_asset.lambda_role_arn,
      module.dr2_ingest_validate_generic_ingest_inputs_lambda.lambda_role_arn,
      module.dr2_ingest_parsed_court_document_event_handler_lambda.lambda_role_arn,
      module.dr2_ingest_mapper_lambda.lambda_role_arn,
      module.dr2_ingest_asset_opex_creator_lambda.lambda_role_arn,
      module.dr2_ingest_folder_opex_creator_lambda.lambda_role_arn,
      module.dr2_ingest_upsert_archive_folders_lambda.lambda_role_arn,
      module.dr2_ingest_parent_folder_opex_creator_lambda.lambda_role_arn,
      module.dr2_ingest_asset_reconciler_lambda.lambda_role_arn,
      module.dr2_ingest_step_function.step_function_role_arn,
      module.tdr_preingest.aggregator_lambda.role,
      module.tdr_preingest.package_builder_lambda.role,
      module.tdr_preingest.importer_lambda.role,
      module.dri_preingest.aggregator_lambda.role,
      module.dri_preingest.package_builder_lambda.role,
      module.dri_preingest.importer_lambda.role,
      local.tna_to_preservica_role_arn,
      local.tre_prod_judgment_role,
    ], local.additional_user_roles, local.anonymiser_roles, local.e2e_test_roles)
    ci_roles = [local.terraform_role_arn]
    service_details = [
      { service_name = "cloudwatch" },
      { service_name = "sns", service_source_account = module.tre_config.account_numbers["prod"] },
      { service_name = "sns" },
    ]
  }
}

module "dr2_developer_key" {
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//kms"
  key_name = "${local.environment}-kms-dr2-dev"
  default_policy_variables = {
    user_roles = [
      data.aws_ssm_parameter.dev_admin_role.value,
      data.aws_iam_role.org_wiz_access_role.arn,
      module.dr2_ingest_mapper_lambda.lambda_role_arn,
      module.dr2_ingest_step_function.step_function_role_arn
    ]
    ci_roles = [local.terraform_role_arn]
    service_details = [
      { service_name = "s3" },
      { service_name = "sns" },
      { service_name = "logs.eu-west-2" },
      { service_name = "cloudwatch" }
    ]
  }
}
