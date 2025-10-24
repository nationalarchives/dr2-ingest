module "vpc" {
  source                    = "git::https://github.com/nationalarchives/da-terraform-modules//vpc"
  vpc_name                  = "${local.environment}-vpc"
  az_count                  = local.az_count
  elastic_ip_allocation_ids = data.aws_eip.eip.*.id
  use_nat_gateway           = true
  environment               = local.environment
  private_nacl_rules = [
    { rule_no = 100, cidr_block = "0.0.0.0/0", action = "allow", from_port = 443, to_port = 443, egress = true },
    { rule_no = 100, cidr_block = "0.0.0.0/0", action = "allow", from_port = 1024, to_port = 65535, egress = false },
  ]
  public_nacl_rules = [
    { rule_no = 100, cidr_block = "0.0.0.0/0", action = "allow", from_port = 443, to_port = 443, egress = false },
    { rule_no = 200, cidr_block = "0.0.0.0/0", action = "allow", from_port = 1024, to_port = 65535, egress = false },
    { rule_no = 100, cidr_block = "0.0.0.0/0", action = "allow", from_port = 443, to_port = 443, egress = true },
    { rule_no = 200, cidr_block = "0.0.0.0/0", action = "allow", from_port = 1024, to_port = 65535, egress = true },
  ]
}

data "aws_eip" "eip" {
  count = local.az_count
  filter {
    name   = "tag:Name"
    values = ["${local.environment}-eip-${count.index}"]
  }
}

module "outbound_https_access_only" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//security_group"
  common_tags = {}
  description = "A security group to allow outbound access only"
  name        = "${local.environment}-outbound-https"
  vpc_id      = module.vpc.vpc_id
  rules = {
    egress = [
      {
        port              = 443
        description       = "Outbound https to discovery VPC endpoint"
        security_group_id = module.discovery_inbound_https.security_group_id
        protocol          = "tcp"
      },
      {
        port        = 443
        description = "Outbound https to all IPs"
        cidr_ip_v4  = "0.0.0.0/0"
        protocol    = "tcp"
      },
    ]
  }
}

resource "aws_ec2_managed_prefix_list" "cloudflare_prefix_list" {
  address_family = "IPv4"
  max_entries    = length(local.cloudflare_ip_ranges) + 5
  name           = "${local.environment}-cloudflare-ranges"
  dynamic "entry" {
    for_each = local.cloudflare_ip_ranges
    content {
      cidr = entry.value
    }
  }
}

module "outbound_cloudflare_https_access" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//security_group"
  common_tags = {}
  description = "A security group to allow outbound access to Cloudflare IPs only"
  name        = "${local.environment}-outbound-https-to-cloudflare"
  vpc_id      = module.vpc.vpc_id
  rules = {
    egress = [{
      port           = 443
      description    = "Outbound https Cloudflare access",
      prefix_list_id = aws_ec2_managed_prefix_list.cloudflare_prefix_list.id
      protocol       = "tcp"
    }]
  }
}

