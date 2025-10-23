variable "deploy_version" {}

locals {
  environment = terraform.workspace == "default" ? "intg" : terraform.workspace
}
