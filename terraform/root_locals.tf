locals {
  environment       = terraform.workspace == "default" ? "intg" : terraform.workspace
  environment_title = title(local.environment)
}
