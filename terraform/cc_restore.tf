locals {
  cc_restore_bucket_name = "${local.environment}-dr2-ingest-cc-restore-cache"
  cc_restore_policy_name = "${local.environment}-dr2-ingest-cc-restore-policy"
  cc_restore_role_name   = "${local.environment}-dr2-ingest-cc-restore-role"
}

module "dr2_cc_restore_key" {
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//kms"
  key_name = "${local.environment}-kms-dr2-cc-restore"
  default_policy_variables = {
    ci_roles = [local.terraform_role_arn],
    user_roles = [
      module.dr2_cc_restore_role.role_arn,
      module.cc_restore_preingest.importer_lambda.role,
    ]
  }
}

module "ingest_cc_restore_cache_bucket" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name     = local.cc_restore_bucket_name
  kms_key_arn     = module.dr2_cc_restore_key.kms_key_arn
  lifecycle_rules = local.lifecycle_rules
}

module "dr2_cc_restore_role" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_role"
  assume_role_policy = templatefile("${path.module}/templates/iam_role/aws_principal_assume_role.json.tpl", {
    aws_arn = data.aws_ssm_parameter.dev_admin_role.value
  })
  name = local.cc_restore_role_name
  policy_attachments = {
    dr2_cc_restore_policy = module.dr2_cc_restore_policy.policy_arn
  }
  tags = {}
}

module "dr2_cc_restore_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = local.cc_restore_policy_name
  policy_string = templatefile("${path.module}/templates/iam_policy/cc_restore_policy.json.tpl", {
    environment = local.environment
    account_id  = data.aws_caller_identity.current.account_id
  })
}
