output "kms_user_roles" {
  value = concat([
    data.aws_iam_role.org_wiz_access_role.arn,
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.ingest_find_existing_asset_name}-role",
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.ingest_validate_generic_ingest_inputs_lambda_name}-role",
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.ingest_parsed_court_document_event_handler_lambda_name}-role",
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.ingest_mapper_lambda_name}-role",
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.ingest_asset_opex_creator_lambda_name}-role",
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.ingest_folder_opex_creator_lambda_name}-role",
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.ingest_upsert_archive_folders_lambda_name}-role",
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.ingest_parent_folder_opex_creator_lambda_name}-role",
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.ingest_asset_reconciler_lambda_name}-role",
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.ingest_step_function_name}-role",
    local.tna_to_preservica_role_arn,
    local.tre_prod_judgment_role,
  ], local.additional_user_roles, local.anonymiser_roles, local.e2e_test_roles)
}

output "dev_kms_user_roles" {
  value = [
    data.aws_ssm_parameter.dev_admin_role.value,
    data.aws_iam_role.org_wiz_access_role.arn,
    module.dr2_ingest_mapper_lambda.lambda_role_arn,
    module.dr2_ingest_step_function.step_function_role_arn
  ]
}

output "rotate_secret_lambda_arn" {
  value = module.dr2_rotate_preservation_system_password_lambda.lambda_arn
}

output "court_document_sqs_arn" {
  value = module.dr2_ingest_parsed_court_document_event_handler_sqs.sqs_arn
}

