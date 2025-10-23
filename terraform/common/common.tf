module "config" {
  source  = "../da-terraform-configurations"
  project = "dr2"
}

module "tre_config" {
  source  = "../da-terraform-configurations"
  project = "tre"
}

module "tdr_config" {
  source  = "../da-terraform-configurations"
  project = "tdr"
}

locals {
  environment                                                 = var.environment
  environment_title                                           = title(var.environment)
  az_count                                                    = local.environment == "prod" ? 2 : 1
  ingest_raw_cache_bucket_name                                = "${local.environment}-dr2-ingest-raw-cache"
  sample_files_bucket_name                                    = "${local.environment}-dr2-sample-files"
  ingest_state_bucket_name                                    = "${local.environment}-dr2-ingest-state"
  additional_user_roles                                       = local.environment != "prod" ? [data.aws_ssm_parameter.dev_admin_role.value] : []
  anonymiser_roles                                            = local.environment == "intg" ? flatten([module.dr2_court_document_package_anonymiser_lambda.*.lambda_role_arn]) : []
  anonymiser_lambda_arns                                      = local.environment == "intg" ? flatten([module.dr2_court_document_package_anonymiser_lambda.*.lambda_arn]) : []
  files_dynamo_table_name                                     = "${local.environment}-dr2-ingest-files"
  ingest_lock_dynamo_table_name                               = "${local.environment}-dr2-ingest-lock"
  ingest_queue_dynamo_table_name                              = "${local.environment}-dr2-ingest-queue"
  java_runtime                                                = "java21"
  java_lambda_memory_size                                     = 512
  java_timeout_seconds                                        = 180
  python_runtime                                              = "python3.12"
  python_lambda_memory_size                                   = 128
  python_timeout_seconds                                      = 30
  enable_point_in_time_recovery                               = true
  files_table_batch_parent_global_secondary_index_name        = "BatchParentPathIdx"
  ingest_parsed_court_document_event_handler_test_bucket_name = "${local.environment}-dr2-ingest-parsed-court-document-test-input"
  ingest_lock_table_group_id_gsi_name                         = "IngestLockGroupIdx"
  ingest_lock_table_hash_key                                  = "assetId"
  dev_notifications_channel_id                                = local.environment == "prod" ? "C06EDJPF0VB" : "C052LJASZ08"
  general_notifications_channel_id                            = local.environment == "prod" ? "C06E20AR65V" : "C068RLCPZFE"
  tre_prod_judgment_role                                      = "arn:aws:iam::${module.tre_config.account_numbers["prod"]}:role/prod-tre-editorial-judgment-out-copier"
  step_function_failure_log_group                             = "step-function-failures"
  terraform_role_arn                                          = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${title(local.environment)}TerraformRole"
  preservica_tenant                                           = local.environment == "prod" ? "tna" : "tnatest"
  tna_to_preservica_role_arn                                  = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.environment}-tna-to-preservica-ingest-s3-${local.preservica_tenant}"
  creator                                                     = "dr2-terraform-environments"
  sse_encryption                                              = "sse"
  visibility_timeout                                          = 180
  redrive_maximum_receives                                    = 5
  dashboard_lambdas = concat([
    local.court_document_anonymiser_lambda_name,
    local.entity_event_lambda_name,
    local.get_latest_preservica_version,
    local.ingest_queue_creator_name,
    local.ip_lock_checker_lambda_name,
    local.rotate_preservation_system_password_name
  ], keys(var.ingest.lambdas))
  queues = concat([
    var.ingest.court_document_event_handler_sqs,
    module.dr2_custodial_copy_queue,
    module.dr2_custodial_copy_queue_creator_queue,
    module.dr2_custodial_copy_db_builder_queue,
    module.dr2_external_notifications_queue,
  ], var.ingest.importer_sqs_queues)
  messages_visible_threshold = 1000000
  # The list comes from https://www.cloudflare.com/en-gb/ips
  cloudflare_ip_ranges                   = toset(["173.245.48.0/20", "103.21.244.0/22", "103.22.200.0/22", "103.31.4.0/22", "141.101.64.0/18", "108.162.192.0/18", "190.93.240.0/20", "188.114.96.0/20", "197.234.240.0/22", "198.41.128.0/17", "162.158.0.0/15", "104.16.0.0/13", "104.24.0.0/14", "172.64.0.0/13", "131.0.72.0/22"])
  outbound_security_group_ids            = [module.outbound_https_access_only.security_group_id, module.outbound_cloudflare_https_access.security_group_id]
  code_deploy_bucket                     = "mgmt-dp-code-deploy"
  step_function_failure_eventbridge_rule = "${local.environment}-dr2-eventbridge-ingest-step-function-failure"
}


data "aws_iam_role" "org_wiz_access_role" {
  name = "org-wiz-access-role"
}

resource "random_password" "preservica_password" {
  length = 20
}

resource "random_string" "preservica_user" {
  length  = 10
  special = false
}

resource "aws_secretsmanager_secret" "preservica_secret" {
  name = "${local.environment}-preservica-api-login-details-${random_string.preservica_user.result}"
}

resource "aws_secretsmanager_secret" "preservica_read_metadata_read_content" {
  name = "${local.environment}-preservica-api-read-metadata-read-content"
}

resource "aws_secretsmanager_secret" "preservica_read_metadata" {
  name = "${local.environment}-preservica-api-read-metadata"
}

resource "aws_secretsmanager_secret" "preservica_read_update_metadata_insert_content" {
  name = "${local.environment}-preservica-api-read-update-metadata-insert-content"
}

resource "aws_secretsmanager_secret_rotation" "secret_rotation" {
  rotation_lambda_arn = module.dr2_rotate_preservation_system_password_lambda.lambda_arn
  secret_id           = aws_secretsmanager_secret.preservica_secret.id
  rotation_rules {
    schedule_expression = "rate(4 hours)"
  }
}

resource "aws_secretsmanager_secret_rotation" "secret_rotation_read_metadata_read_content" {
  rotation_lambda_arn = module.dr2_rotate_preservation_system_password_lambda.lambda_arn
  secret_id           = aws_secretsmanager_secret.preservica_read_metadata_read_content.id
  rotation_rules {
    schedule_expression = "rate(4 hours)"
  }
}

resource "aws_secretsmanager_secret_rotation" "secret_rotation_read_metadata" {
  rotation_lambda_arn = module.dr2_rotate_preservation_system_password_lambda.lambda_arn
  secret_id           = aws_secretsmanager_secret.preservica_read_metadata.id
  rotation_rules {
    schedule_expression = "rate(4 hours)"
  }
}

resource "aws_secretsmanager_secret_rotation" "secret_rotation_read_update_metadata_insert_content" {
  rotation_lambda_arn = module.dr2_rotate_preservation_system_password_lambda.lambda_arn
  secret_id           = aws_secretsmanager_secret.preservica_read_update_metadata_insert_content.id
  rotation_rules {
    schedule_expression = "rate(4 hours)"
  }
}

resource "aws_secretsmanager_secret" "demo_preservica_secret" {
  name = "${local.environment}-demo-preservica-api-login-details-${random_string.preservica_user.result}"
}

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

module "dr2_kms_key" {
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//kms?ref=DR2-2511-do-not-ignore-filename-if-set"
  key_name = "${local.environment}-kms-dr2"
  default_policy_variables = {
    user_roles_decoupled = concat([
      data.aws_iam_role.org_wiz_access_role.arn,
      replace(var.ingest.lambdas[var.ingest.lambda_names.ingest_asset_opex_creator].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.find_existing_asset].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.validate_ingest_inputs].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.ingest_reconciler].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.folder_opex_creator].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.parent_folder_opex_creator].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.tdr_aggregator].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.ingest_mapper].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.tdr_package_builder].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.court_document_handler].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.upsert_folders].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.dri_importer].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.tdr_importer].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.dri_package_builder].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.folder_opex_creator].role, "intg-", "*"),
      replace(var.ingest.lambdas[var.ingest.lambda_names.dri_aggregator].role, "intg-", "*"),
      replace(var.ingest.ingest_step_function.step_function_role_arn, "intg-", "*"),
      local.tna_to_preservica_role_arn,
      local.tre_prod_judgment_role,
    ], local.additional_user_roles, local.anonymiser_roles, var.ingest.e2e_tests_role)
    ci_roles = [local.terraform_role_arn]
    service_details = [
      { service_name = "cloudwatch" },
      { service_name = "sns", service_source_account = module.tre_config.account_numbers["prod"] },
      { service_name = "sns" },
    ]
  }
}

module "dr2_developer_key" {
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//kms"
  key_name = "${local.environment}-kms-dr2-dev"
  default_policy_variables = {
    user_roles = [
      data.aws_ssm_parameter.dev_admin_role.value,
      data.aws_iam_role.org_wiz_access_role.arn,
      var.ingest.lambdas[var.ingest.lambda_names.ingest_mapper].role,
      var.ingest.ingest_step_function.step_function_role_arn
    ]
    ci_roles = [local.terraform_role_arn]
    service_details = [
      { service_name = "s3" },
      { service_name = "sns" },
      { service_name = "logs.eu-west-2" },
      { service_name = "cloudwatch" }
    ]
  }
}

data "aws_ssm_parameter" "dev_admin_role" {
  name = "/${local.environment}/developer_role"
}

module "ingest_raw_cache_bucket" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name = local.ingest_raw_cache_bucket_name
  bucket_policy = templatefile("./templates/s3/lambda_access_bucket_policy.json.tpl", {
    lambda_role_arns = jsonencode([var.ingest.lambdas[var.ingest.lambda_names.court_document_handler].role]),
    bucket_name      = local.ingest_raw_cache_bucket_name
  })
  kms_key_arn = module.dr2_kms_key.kms_key_arn
}

module "sample_files_bucket" {
  source            = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name       = local.sample_files_bucket_name
  create_log_bucket = false
  kms_key_arn       = module.dr2_kms_key.kms_key_arn
}


module "ingest_state_bucket" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name = local.ingest_state_bucket_name
  bucket_policy = templatefile("./templates/s3/lambda_access_bucket_policy.json.tpl", {
    lambda_role_arns = jsonencode([var.ingest.lambdas[var.ingest.lambda_names.ingest_mapper].role]),
    bucket_name      = local.ingest_state_bucket_name
  })
  kms_key_arn = module.dr2_developer_key.kms_key_arn
}

module "files_table" {
  source                         = "git::https://github.com/nationalarchives/da-terraform-modules//dynamo"
  hash_key                       = { name = "id", type = "S" }
  range_key                      = { name = "batchId", type = "S" }
  table_name                     = local.files_dynamo_table_name
  server_side_encryption_enabled = true
  kms_key_arn                    = module.dr2_kms_key.kms_key_arn
  ttl_attribute_name             = "ttl"
  stream_enabled                 = true
  stream_view_type               = "NEW_IMAGE"
  deletion_protection_enabled    = true
  additional_attributes = [
    { name = "batchId", type = "S" },
    { name = "parentPath", type = "S" }
  ]
  global_secondary_indexes = [
    {
      name            = local.files_table_batch_parent_global_secondary_index_name
      hash_key        = "batchId"
      range_key       = "parentPath"
      projection_type = "ALL"
    }
  ]
  point_in_time_recovery_enabled = local.enable_point_in_time_recovery
}

module "ingest_lock_table" {
  source                         = "git::https://github.com/nationalarchives/da-terraform-modules//dynamo"
  hash_key                       = { name = local.ingest_lock_table_hash_key, type = "S" }
  table_name                     = local.ingest_lock_dynamo_table_name
  server_side_encryption_enabled = false
  additional_attributes = [
    { name = "groupId", type = "S" }
  ]
  global_secondary_indexes = [
    {
      name            = local.ingest_lock_table_group_id_gsi_name
      hash_key        = "groupId"
      projection_type = "ALL"
    }
  ]
  point_in_time_recovery_enabled = local.enable_point_in_time_recovery
}

data "aws_ssm_parameter" "slack_token" {
  name            = "/mgmt/slack/token"
  with_decryption = true
}

resource "aws_ssm_parameter" "flow_control_config" {
  name  = "/${local.environment}/flow-control-config"
  type  = "String"
  value = templatefile("${path.root}/templates/ssm/ingest_flow_control_config.json.tpl", {})
}

module "eventbridge_alarm_notifications_destination" {
  source                     = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination"
  authorisation_header_value = "Bearer ${data.aws_ssm_parameter.slack_token.value}"
  name                       = "${local.environment}-dr2-eventbridge-slack-destination"
}



module "cloudwatch_event_alarm_event_bridge_rule_alarm_only" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  event_pattern = templatefile("${path.root}/templates/eventbridge/cloudwatch_alarm_event_pattern.json.tpl", {
    cloudwatch_alarms = jsonencode(flatten([[for queue in local.queues : queue.event_alarms], [var.ingest.postingest.cc_confirmer_queue_oldest_message_alarm_arn]])),
    state_value       = "ALARM"
  })
  name                = "${local.environment}-dr2-eventbridge-alarm-state-change-alarm-only"
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  api_destination_input_transformer = {
    input_paths = {
      "alarmName"    = "$.detail.alarmName",
      "currentValue" = "$.detail.state.value"
    }
    input_template = templatefile("${path.root}/templates/eventbridge/slack_message_input_template.json.tpl", {
      channel_id   = local.dev_notifications_channel_id
      slackMessage = ":warning: Cloudwatch alarm <alarmName> has entered state <currentValue>"
    })
  }
}

module "cloudwatch_alarm_event_bridge_rule" {
  for_each = toset(["OK", "ALARM"])
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  event_pattern = templatefile("${path.root}/templates/eventbridge/cloudwatch_alarm_event_pattern.json.tpl", {
    cloudwatch_alarms = jsonencode(flatten([for queue in local.queues : queue.alarms]))
    state_value       = each.value
  })
  name                = "${local.environment}-dr2-eventbridge-alarm-state-change-${lower(each.value)}"
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  api_destination_input_transformer = {
    input_paths = {
      "alarmName"    = "$.detail.alarmName",
      "currentValue" = "$.detail.state.value"
    }
    input_template = templatefile("${path.root}/templates/eventbridge/slack_message_input_template.json.tpl", {
      channel_id   = local.dev_notifications_channel_id
      slackMessage = ":${each.value == "OK" ? "green-tick" : "alert-noflash-slow"}: Cloudwatch alarm <alarmName> has entered state <currentValue>"
    })
  }
}

module "guard_duty_findings_eventbridge_rule" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  event_pattern = templatefile("${path.root}/templates/eventbridge/source_detail_type_event_pattern.json.tpl", {
    source = "aws.guardduty", detail_type = "GuardDuty Finding"
  })
  name                = "${local.environment}-dr2-guard-duty-notify"
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  api_destination_input_transformer = {
    input_paths = {
      "account" : "$.account",
      "id" : "$.detail.id",
      "region" : "$.region",
      "title" : "$.detail.title"
    }
    input_template = templatefile("${path.root}/templates/eventbridge/guard_duty_slack_message.json.tpl", {})
  }
}

module "secret_rotation_eventbridge_rule" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  event_pattern = templatefile("${path.root}/templates/eventbridge/secrets_manager_rotation.json.tpl", {
    rotation_event = "RotationFailed"
  })
  name                = "${local.environment}-dr2-failed-secrets-manager-rotation"
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  api_destination_input_transformer = {
    input_paths = {
      "secretId" : "$.detail.additionalEventData.SecretId"
    }
    input_template = templatefile("${path.root}/templates/eventbridge/slack_message_input_template.json.tpl", {
      channel_id   = local.dev_notifications_channel_id
      slackMessage = ":alert-noflash-slow: Secret rotation for secret `<secretId>` has failed"
    })
  }
}

module "dev_slack_message_eventbridge_rule" {
  source              = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  event_pattern       = templatefile("${path.root}/templates/eventbridge/custom_detail_type_event_pattern.json.tpl", { detail_type = "DR2DevMessage" })
  name                = "${local.environment}-dr2-eventbridge-dev-slack-message"
  api_destination_input_transformer = {
    input_paths = {
      "slackMessage" = "$.detail.slackMessage"
    }
    input_template = templatefile("${path.root}/templates/eventbridge/slack_message_input_template.json.tpl", {
      channel_id   = local.dev_notifications_channel_id
      slackMessage = "<slackMessage>"
    })
  }
}

module "general_slack_message_eventbridge_rule" {
  source              = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  event_pattern       = templatefile("${path.root}/templates/eventbridge/custom_detail_type_event_pattern.json.tpl", { detail_type = "DR2Message" })
  name                = "${local.environment}-dr2-eventbridge-general-slack-message"
  api_destination_input_transformer = {
    input_paths = {
      "slackMessage" = "$.detail.slackMessage"
    }
    input_template = templatefile("${path.root}/templates/eventbridge/slack_message_input_template.json.tpl", {
      channel_id   = local.general_notifications_channel_id
      slackMessage = "<slackMessage>"
    })
  }
}

resource "aws_cloudwatch_log_resource_policy" "eventbridge_resource_policy" {
  policy_document = templatefile("${path.root}/templates/logs/logs_resource_policy.json.tpl", { account_id = data.aws_caller_identity.current.account_id })
  policy_name     = "${local.environment}-dr2-trust-events-to-store-log-events"
}

resource "aws_cloudwatch_dashboard" "ingest_dashboard" {
  dashboard_body = templatefile("${path.root}/templates/logs/ingest_dashboard.json.tpl", {
    account_id                      = data.aws_caller_identity.current.account_id,
    environment                     = local.environment,
    step_function_failure_log_group = local.step_function_failure_log_group
    source_list                     = join(" | ", [for lambda in local.dashboard_lambdas : format("SOURCE '/aws/lambda/%s'", lambda)])
  })
  dashboard_name = "${local.environment}-dr2-ingest-dashboard"

}

resource "aws_vpc_endpoint" "discovery" {
  vpc_id              = module.vpc.vpc_id
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
  vpc_id      = module.vpc.vpc_id
  rules = {
    ingress = [{
      port              = 443
      description       = "Inbound access from lambda security group"
      security_group_id = module.outbound_https_access_only.security_group_id
    }]
  }
}

module "failed_ingest_step_function_event_bridge_rule" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  event_pattern = templatefile("${path.root}/templates/eventbridge/step_function_failed_event_pattern.json.tpl", {
    step_function_arns = jsonencode(var.ingest.step_function_arns)
  })
  name                = local.step_function_failure_eventbridge_rule
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  api_destination_input_transformer = {
    input_paths = {
      "name"   = "$.detail.name",
      "status" = "$.detail.status",
      "sfnArn" = "$.detail.stateMachineArn"
    }
    input_template = templatefile("${path.root}/templates/eventbridge/slack_message_input_template.json.tpl", {
      channel_id   = local.dev_notifications_channel_id
      slackMessage = ":alert-noflash-slow: Step function `<sfnArn>` with name <name> has <status>"
    })
  }
  log_group_destination_input_transformer = {
    log_group_name = local.step_function_failure_log_group
    input_paths = {
      "name"      = "$.detail.name",
      "status"    = "$.detail.status",
      "startDate" = "$.detail.startDate",
      "sfnArn"    = "$.detail.stateMachineArn"
    }
    input_template = templatefile("${path.root}/templates/eventbridge/cloudwatch_message_input_template.json.tpl", {
      message = "Step function `<sfnArn>` with name <name> has <status>"
    })
  }
  lambda_target_arn = "arn:aws:lambda:eu-west-2:${data.aws_caller_identity.current.account_id}:function:${var.ingest.lambda_names.failed_notification}"
}

module "dr2_ingest_parsed_court_document_event_handler_test_input_bucket" {
  count       = local.environment != "prod" ? 1 : 0
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name = local.ingest_parsed_court_document_event_handler_test_bucket_name
  bucket_policy = templatefile("./templates/s3/lambda_access_bucket_policy.json.tpl", {
    lambda_role_arns = jsonencode([var.ingest.lambdas[var.ingest.lambda_names.court_document_handler].role, "arn:aws:iam::${module.tre_config.account_numbers["prod"]}:role/prod-tre-editorial-judgment-out-copier"]),
    bucket_name      = local.ingest_parsed_court_document_event_handler_test_bucket_name
  })
  kms_key_arn = module.dr2_kms_key.kms_key_arn
}
