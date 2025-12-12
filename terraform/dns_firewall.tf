resource "aws_route53_resolver_firewall_domain_list" "allow_domains" {
  name = "allow-specific-services"

  domains = [
    "s3.eu-west-2.amazonaws.com",
    "${local.environment}-dr2-ingest-raw-cache.s3.eu-west-2.amazonaws.com",
    "${local.environment}-dr2-ingest-state.s3.eu-west-2.amazonaws.com",
    "${local.environment}-tre-court-document-pack-out.s3.eu-west-2.amazonaws.com",
    "dynamodb.eu-west-2.amazonaws.com",
    "secretsmanager.eu-west-2.amazonaws.com",
    "sts.eu-west-2.amazonaws.com",
    "states.eu-west-2.amazonaws.com",
    "ssm.eu-west-2.amazonaws.com",
    "sqs.eu-west-2.amazonaws.com",
    "sns.eu-west-2.amazonaws.com",
    "discovery.nationalarchives.gov.uk",
    "tna.preservica.com",
    "tna.preservica.com.cdn.cloudflare.net",
    "s3-r-w.eu-west-2.amazonaws.com"
  ]
}

resource "aws_route53_resolver_firewall_domain_list" "block_all" {
  name    = "block-everything"
  domains = ["*"]
}

resource "aws_route53_resolver_firewall_rule_group" "default" {
  name = "default-dns-firewall"
}

resource "aws_route53_resolver_firewall_rule" "allow_rule" {
  firewall_rule_group_id  = aws_route53_resolver_firewall_rule_group.default.id
  firewall_domain_list_id = aws_route53_resolver_firewall_domain_list.allow_domains.id

  name     = "allow-specific-services-rule"
  priority = 100
  action   = "ALLOW"
}

resource "aws_route53_resolver_firewall_rule" "block_rule" {
  firewall_rule_group_id  = aws_route53_resolver_firewall_rule_group.default.id
  firewall_domain_list_id = aws_route53_resolver_firewall_domain_list.block_all.id

  name           = "block-all-services-rule"
  priority       = 200
  action         = "BLOCK"
  block_response = "NODATA"
}

resource "aws_route53_resolver_firewall_rule_group_association" "vpc_association" {
  name                   = "vpc-firewall-association"
  priority               = 101
  firewall_rule_group_id = aws_route53_resolver_firewall_rule_group.default.id
  vpc_id                 = module.vpc.vpc_id
}