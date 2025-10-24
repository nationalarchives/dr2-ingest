data "aws_caller_identity" "current" {}

locals {
  terraform_role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.environment_title}TerraformRole"
  environment        = terraform.workspace
  environments       = toset(sort(concat([local.environment], jsondecode(nonsensitive(data.aws_ssm_parameter.environments.value)))))
  creator            = "dr2-terraform-environments"
  environment_title  = title(local.environment)
}

module "dr2_kms_key" {
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//kms"
  key_name = "${local.environment}-kms-dr2"
  default_policy_variables = {
    user_roles = concat(
      flatten([for x in module.ingest : x.kms_user_roles]),
      flatten([for x in module.tdr_preingest : [x.aggregator_lambda.role, x.package_builder_lambda.role, x.importer_lambda.role]]),
      flatten([for x in module.dri_preingest : [x.aggregator_lambda.role, x.package_builder_lambda.role, x.importer_lambda.role]])
    )
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
    user_roles = flatten([for x in module.ingest : x.dev_kms_user_roles])
    ci_roles   = [local.terraform_role_arn]
    service_details = [
      { service_name = "s3" },
      { service_name = "sns" },
      { service_name = "logs.eu-west-2" },
      { service_name = "cloudwatch" }
    ]
  }
}
