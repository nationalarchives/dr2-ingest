terraform {
  backend "s3" {
    bucket       = "mgmt-dp-terraform-state"
    key          = "terraform.state"
    region       = "eu-west-2"
    encrypt      = true
    use_lockfile = true
  }
}
provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      Environment = local.environment
      CreatedBy   = "dr2-terraform-environments"
    }
  }
}
