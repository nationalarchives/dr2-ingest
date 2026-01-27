module "postingest" {
  source                     = "./postingest"
  environment                = local.environment
  notifications_topic_arn    = module.dr2_notifications_sns.sns_arn
  private_security_group_ids = [module.outbound_https_access_for_dynamo_db.security_group_id, module.https_to_vpc_endpoints_security_group.security_group_id]
  private_subnet_ids         = module.vpc.private_subnets
  vpc_id                     = module.vpc.vpc_id
}
