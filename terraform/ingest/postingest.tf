module "postingest" {
  source                  = "../postingest"
  environment             = local.environment
  notifications_topic_arn = var.notifications_topic.arn
}