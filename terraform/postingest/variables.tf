variable "environment" {}

variable "notifications_topic_arn" {}

variable "deploy_version" {}

variable "table_names" {
  type = object({
    postingest = string
  })
}