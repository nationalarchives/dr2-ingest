locals {
  ingest_mapper_lambda_name = "${local.environment}-dr2-ingest-mapper"
}

module "dr2_ingest_mapper_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.ingest_mapper_lambda_name
  handler         = "uk.gov.nationalarchives.ingestmapper.Lambda::handleRequest"
  timeout_seconds = local.java_timeout_seconds
  policies = {
    "${local.ingest_mapper_lambda_name}-policy" = templatefile("./templates/iam_policy/ingest_mapper_policy.json.tpl", {
      raw_cache_bucket_name    = local.ingest_raw_cache_bucket_name
      ingest_state_bucket_name = local.ingest_state_bucket_name
      account_id               = data.aws_caller_identity.current.account_id
      lambda_name              = local.ingest_mapper_lambda_name
      dynamo_db_file_table_arn = module.files_table.table_arn
      vpc_id                   = module.vpc.vpc.id
      vpc_arn                  = module.vpc.vpc.arn
    })
  }
  memory_size = local.java_lambda_memory_size
  runtime     = local.java_runtime
  plaintext_env_vars = {
    FILES_DDB_TABLE    = local.files_dynamo_table_name
    OUTPUT_BUCKET_NAME = local.ingest_state_bucket_name
  }
  vpc_config = {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [module.outbound_https_to_discovery.security_group_id, module.outbound_https_access_for_s3.security_group_id, module.outbound_https_access_for_dynamo_db.security_group_id]
  }
  tags = {
    Name = local.ingest_mapper_lambda_name
  }
}

resource "aws_vpc_endpoint" "discovery" {
  vpc_id              = module.vpc.vpc.id
  service_name        = "com.amazonaws.vpce.eu-west-2.vpce-svc-030613f5fe9f42a77"
  private_dns_enabled = true
  vpc_endpoint_type   = "Interface"
  subnet_ids          = module.vpc.private_subnets
  security_group_ids  = [module.discovery_inbound_https.security_group_id]
}

module "discovery_inbound_https" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//security_group"
  common_tags = {}
  description = "A security group to allow inbound access to discovery VPC endpoint from lambda security group"
  name        = "${local.environment}-dr2-discovery-inbound-https"
  vpc_id      = module.vpc.vpc.id
  rules = {
    ingress = [{
      port              = 443
      description       = "Inbound access from lambda security group"
      security_group_id = module.outbound_https_to_discovery.security_group_id
    }]
  }
}

