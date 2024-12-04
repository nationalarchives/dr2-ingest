locals {
  ingest_parent_folder_opex_creator_lambda_name = "${local.environment}-dr2-ingest-parent-folder-opex-creator"
}

module "dr2_ingest_parent_folder_opex_creator_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.ingest_parent_folder_opex_creator_lambda_name
  handler         = "uk.gov.nationalarchives.ingestparentfolderopexcreator.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${local.ingest_parent_folder_opex_creator_lambda_name}-policy" = templatefile("./templates/iam_policy/ingest_parent_folder_opex_creator_policy.json.tpl", {
      account_id              = data.aws_caller_identity.current.account_id
      lambda_name             = local.ingest_parent_folder_opex_creator_lambda_name
      copy_to_preservica_role = module.copy_tna_to_preservica_role.role_arn
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    DESTINATION_BUCKET = local.preservica_ingest_bucket
    S3_ROLE_ARN        = module.copy_tna_to_preservica_role.role_arn
  }
  tags = {
    Name = local.ingest_parent_folder_opex_creator_lambda_name
  }
}
