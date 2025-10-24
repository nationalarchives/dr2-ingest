locals {
  environment = terraform.workspace == "default" ? "intg" : terraform.workspace
}
