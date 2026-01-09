locals {
  adhoc_bucket_name = "${local.environment}-dr2-ingest-adhoc-cache"
}
module "dr2_archivists_key" {
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//kms"
  key_name = "${local.environment}-kms-dr2-archivist"
  default_policy_variables = {
    ci_roles = [local.terraform_role_arn],
    user_roles = [
      data.aws_ssm_parameter.archivist_role.value,
      module.ad_hoc_preingest.package_builder_lambda.role,
      module.ad_hoc_preingest.importer_lambda.role,
      module.dr2_ingest_mapper_lambda.lambda_role_arn,
      module.copy_tna_to_preservica_role.role_arn,
      module.dr2_ingest_validate_generic_ingest_inputs_lambda.lambda_role_arn,
    ]
  }
}

module "ingest_adhoc_cache_bucket" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name = local.adhoc_bucket_name
  kms_key_arn = module.dr2_archivists_key.kms_key_arn
}