locals {
  records_metadata_bucket_name = "${local.environment}-dr2-records-metadata"
}
module "dr2_records_metadata_key" {
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//kms"
  key_name = "${local.environment}-kms-dr2-records-metadata"
  default_policy_variables = {
    ci_roles = [local.terraform_role_arn],
    user_roles = [
      data.aws_ssm_parameter.dev_admin_role.value
    ]
  }
}

module "records_metadata_bucket" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name     = local.records_metadata_bucket_name
  kms_key_arn     = module.dr2_records_metadata_key.kms_key_arn
  lifecycle_rules = local.lifecycle_rules
}