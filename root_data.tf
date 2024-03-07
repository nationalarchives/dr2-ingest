data "aws_ssm_parameter" "preservica_url" {
  name = "/${local.environment}/preservica/url"
}

data "aws_ssm_parameter" "demo_preservica_url" {
  name = "/${local.environment}/preservica/demo/url"
}

data "aws_caller_identity" "current" {}
