data "aws_ssm_parameter" "preservica_url" {
  name = "/${local.environment}/preservica/url"
}

data "aws_ssm_parameter" "demo_preservica_url" {
  name = "/${local.environment}/preservica/demo/url"
}

data "aws_caller_identity" "current" {}

data "aws_ssm_parameter" "preservica_api_user" {
  name = "/${local.environment}/preservica/user"
}

data "aws_ssm_parameter" "site_outbound_subnet" {
  name = "/mgmt/custodial_copy/site_outbound_subnet"
}

data "aws_ssm_parameter" "custodial_copy_x509_subject_cn" {
  name = "/${local.environment}/custodial_copy/x509_subject_cn"
}

data "aws_organizations_organization" "org" {}
