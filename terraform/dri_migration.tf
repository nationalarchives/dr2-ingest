locals {
  dri_migration_bucket_name = "${local.environment}-dr2-ingest-dri-migration-cache"
  dri_migration_policy_name = "${local.environment}-dr2-ingest-dri-migration-policy"
  dri_migration_role_name   = "${local.environment}-dr2-ingest-dri-migration-role"
}
module "dr2_dri_migration_key" {
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//kms"
  key_name = "${local.environment}-kms-dr2-dri-migration"
  default_policy_variables = {
    ci_roles = [local.terraform_role_arn],
    user_roles = [
      module.dr2_dri_migration_role.role_arn,
      module.dri_preingest.importer_lambda.role,
    ]
  }
}

module "ingest_dri_migration_cache_bucket" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name = local.dri_migration_bucket_name
  kms_key_arn = module.dr2_dri_migration_key.kms_key_arn
}

module "dr2_dri_migration_role" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_role"
  assume_role_policy = templatefile("${path.module}/templates/iam_role/aws_principal_assume_role.json.tpl", {
    aws_arn = data.aws_ssm_parameter.dev_admin_role.value
  })
  name = local.dri_migration_role_name
  policy_attachments = {
    dr2_dri_migration_policy = module.dr2_dri_migration_policy.policy_arn
  }
  tags = {}
}

module "dr2_dri_migration_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = local.dri_migration_policy_name
  policy_string = templatefile("${path.module}/templates/iam_policy/dri_migration_policy.json.tpl", {
    environment = local.environment
    account_id  = data.aws_caller_identity.current.account_id
  })
}
