resource "aws_route53_resolver_firewall_domain_list" "allow_domains" {
  name = "allow-specific-domains"

  domains = [
    # AWS Service endpoints 
    "*.s3.${data.aws_region.current.id}.amazonaws.com.",
    "s3.${data.aws_region.current.id}.amazonaws.com.",
    "s3-r-w.${data.aws_region.current.id}.amazonaws.com.",
    "dynamodb.${data.aws_region.current.id}.amazonaws.com.",
    "secretsmanager.${data.aws_region.current.id}.amazonaws.com.",
    "sts.${data.aws_region.current.id}.amazonaws.com.",
    "states.${data.aws_region.current.id}.amazonaws.com.",
    "ssm.${data.aws_region.current.id}.amazonaws.com.",
    "sqs.${data.aws_region.current.id}.amazonaws.com.",
    "sns.${data.aws_region.current.id}.amazonaws.com.",
    # Other services used by DR2
    "discovery.nationalarchives.gov.uk.",
    "tna.preservica.com.",
    "tna.preservica.com.cdn.cloudflare.net."
  ]
}

resource "aws_route53_resolver_firewall_domain_list" "block_all" {
  name    = "block-all-domains"
  domains = ["*."]
}

resource "aws_route53_resolver_firewall_rule_group" "default" {
  name = "default-dns-firewall"
}

resource "aws_route53_resolver_firewall_rule" "allow_rule" {
  firewall_rule_group_id  = aws_route53_resolver_firewall_rule_group.default.id
  firewall_domain_list_id = aws_route53_resolver_firewall_domain_list.allow_domains.id

  name     = "allow-specific-domains"
  priority = 100
  action   = "ALLOW"
}

resource "aws_route53_resolver_firewall_rule" "block_rule" {
  firewall_rule_group_id  = aws_route53_resolver_firewall_rule_group.default.id
  firewall_domain_list_id = aws_route53_resolver_firewall_domain_list.block_all.id

  name           = "block-all-domains"
  priority       = 200
  action         = "BLOCK"
  block_response = "NXDOMAIN"
}

resource "aws_route53_resolver_firewall_rule_group_association" "vpc_association" {
  name                   = "vpc-firewall-association"
  priority               = 101
  firewall_rule_group_id = aws_route53_resolver_firewall_rule_group.default.id
  vpc_id                 = module.vpc.vpc.id
}