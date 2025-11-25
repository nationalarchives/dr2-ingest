module "postingest" {
  source                     = "./postingest"
  environment                = local.environment
  notifications_topic_arn    = module.dr2_notifications_sns.sns_arn
  private_security_group_ids = [module.outbound_https_access_only.security_group_id, module.outbound_https_access_for_dynamo_db]
  private_subnet_ids         = module.vpc.private_subnets
}
