module "postingest" {
  source                  = "./postingest"
  environment             = local.environment
  notifications_topic_arn = module.dr2_notifications_sns.sns_arn
  private_security_group_ids          = [module.https_to_vpc_endpoints_security_group.security_group_id]
  private_subnet_ids                  = module.vpc.private_subnets
}