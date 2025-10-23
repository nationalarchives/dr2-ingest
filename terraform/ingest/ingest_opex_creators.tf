locals {
  tna_to_preservica_role_name = "${local.environment}-tna-to-preservica-ingest-s3-${local.preservica_tenant}"
}
module "copy_tna_to_preservica_role" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_role"
  assume_role_policy = templatefile("./templates/iam_role/tna_to_preservica_trust_policy.json.tpl", {
    terraform_role_arn                  = module.config.terraform_config[local.default_environment]["terraform_account_role"],
    parent_folder_opex_creator_role_arn = module.dr2_ingest_parent_folder_opex_creator_lambda.lambda_role_arn,
    folder_opex_creator_role_arn        = module.dr2_ingest_folder_opex_creator_lambda.lambda_role_arn,
    asset_opex_creator_role_arn         = module.dr2_ingest_asset_opex_creator_lambda.lambda_role_arn
  })
  name = local.tna_to_preservica_role_name
  policy_attachments = {
    copy_tna_to_preservica_policy = module.copy_tna_to_preservica_policy.policy_arn
  }
  tags = {}
}

module "copy_tna_to_preservica_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.environment}-tna-to-preservica-ingest-s3-${local.preservica_tenant}-policy"
  policy_string = templatefile("./templates/iam_policy/tna_to_preservica_copy.json.tpl", {
    account_id            = data.aws_caller_identity.current.account_id
    preservica_tenant     = local.preservica_tenant
    raw_cache_bucket_name = var.ingest_raw_cache_bucket_name
  })
}

