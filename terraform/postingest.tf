module "postingest" {
  source                  = "./postingest"
  environment             = local.environment
  notifications_topic_arn = module.dr2_notifications_sns.sns.arn
  deploy_version          = var.deploy_version
  table_names = module.common.table_names
}