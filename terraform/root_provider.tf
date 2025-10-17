module "config" {
  source  = "./da-terraform-configurations"
  project = "dr2"
}

module "tre_config" {
  source  = "./da-terraform-configurations"
  project = "tre"
}

module "tdr_config" {
  source  = "./da-terraform-configurations"
  project = "tdr"
}

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
      CreatedBy   = local.creator
    }
  }
}
