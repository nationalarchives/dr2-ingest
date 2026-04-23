locals {
  environment        = terraform.workspace == "default" ? "intg" : terraform.workspace
  environment_title  = title(local.environment)
  code_deploy_bucket = "mgmt-dp-code-deploy"
  lambda_alias_name  = replace(var.lambda_code_version, ".", "-")
}
