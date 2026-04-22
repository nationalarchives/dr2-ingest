variable "environment" {}

variable "notifications_topic_arn" {}

variable "private_security_group_ids" {
  default = []
}

variable "private_subnet_ids" {
  default = []
}

variable "vpc_id" {}

variable "lambda_code_version" {}

variable "code_deploy_bucket" {}