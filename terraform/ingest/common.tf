module "tre_config" {
  source  = "../da-terraform-configurations"
  project = "tre"
}

module "config" {
  source  = "../da-terraform-configurations"
  project = "dr2"
}

locals {
  ingest_step_function_name              = "${local.environment}-dr2-ingest"
  ingest_run_workflow_step_function_name = "${local.environment}-dr2-ingest-run-workflow"
  additional_user_roles                  = local.environment != "prod" ? [data.aws_ssm_parameter.dev_admin_role.value] : []
  anonymiser_roles                       = local.environment == "intg" ? [module.dr2_court_document_package_anonymiser_lambda[0].lambda_role_arn] : []
  e2e_test_roles                         = local.environment == "intg" ? [module.dr2_run_e2e_tests_role[0].role_arn] : []
  files_dynamo_table_name                = "${local.environment}-dr2-ingest-files"

  ingest_queue_dynamo_table_name                       = "${local.environment}-dr2-ingest-queue"
  ingest_flow_control_config_ssm_parameter_name        = "/${local.environment}/flow-control-config"
  enable_point_in_time_recovery                        = true
  files_table_batch_parent_global_secondary_index_name = "BatchParentPathIdx"
  dev_notifications_channel_id                         = local.environment == "prod" ? "C06EDJPF0VB" : "C052LJASZ08"
  general_notifications_channel_id                     = local.environment == "prod" ? "C06E20AR65V" : "C068RLCPZFE"
  tre_prod_judgment_role                               = "arn:aws:iam::${module.tre_config.account_numbers["prod"]}:role/prod-tre-editorial-judgment-out-copier"
  java_lambda_memory_size                              = 512
  java_runtime                                         = "java21"
  java_timeout_seconds                                 = 180
  python_runtime                                       = "python3.12"
  python_lambda_memory_size                            = 128
  python_timeout_seconds                               = 30
  step_function_failure_log_group                      = "step-function-failures"
  preservica_tenant                                    = local.environment == "prod" ? "tna" : "tnatest"
  preservica_ingest_bucket                             = "com.preservica.${local.preservica_tenant}.bulk1"
  tna_to_preservica_role_arn                           = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.environment}-tna-to-preservica-ingest-s3-${local.preservica_tenant}"
  creator                                              = "dr2-terraform-environments"
  sse_encryption                                       = "sse"
  visibility_timeout                                   = 180
  redrive_maximum_receives                             = 5
  ingest_run_workflow_sfn_arn                          = "arn:aws:states:eu-west-2:${data.aws_caller_identity.current.account_id}:stateMachine:${local.ingest_run_workflow_step_function_name}"
  dashboard_lambdas = concat([
    local.court_document_anonymiser_lambda_name,
    local.get_latest_preservica_version,
    local.ingest_asset_opex_creator_lambda_name,
    local.ingest_asset_reconciler_lambda_name,
    local.ingest_failure_notifications_lambda_name,
    local.ingest_find_existing_asset_name,
    local.ingest_folder_opex_creator_lambda_name,
    local.ingest_mapper_lambda_name,
    local.ingest_parent_folder_opex_creator_lambda_name,
    local.ingest_parsed_court_document_event_handler_lambda_name,
    local.ingest_start_workflow_lambda_name,
    local.ingest_upsert_archive_folders_lambda_name,
    local.ingest_validate_generic_ingest_inputs_lambda_name,
    local.ingest_workflow_monitor_lambda_name,
    local.ip_lock_checker_lambda_name,
    local.rotate_preservation_system_password_name
  ], var.preingest_lambda_names, var.entity_event_lambda_names)
  queues = [
    module.dr2_ingest_parsed_court_document_event_handler_sqs,
    var.preingest.tdr.aggregator_sqs,
    var.preingest.tdr.importer_sqs,
    var.preingest.dri.aggregator_sqs,
    var.preingest.dri.importer_sqs,
    var.custodial_copy.custodial_copy_queue,
    var.custodial_copy.custodial_copy_queue_creator_queue,
    var.custodial_copy.custodial_copy_builder_queue,
    var.external_notifications.queue
  ]
  retry_statement             = jsonencode([{ ErrorEquals = ["States.ALL"], IntervalSeconds = 2, MaxAttempts = 6, BackoffRate = 2, JitterStrategy = "FULL" }])
  messages_visible_threshold  = 1000000
  outbound_security_group_ids = [var.security_groups.outbound_https_access_only_security_group_id, var.security_groups.outbound_cloudflare_https_access_security_group_id]
}

data "aws_ssm_parameter" "dev_admin_role" {
  name = "/${local.environment}/developer_role"
}

data "aws_iam_role" "org_wiz_access_role" {
  name = "org-wiz-access-role"
}

module "dr2_ingest_step_function" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//sfn"
  step_function_definition = templatefile("${path.root}/templates/sfn/ingest_sfn_definition.json.tpl", {
    step_function_name                                = local.ingest_step_function_name,
    account_id                                        = data.aws_caller_identity.current.account_id
    ingest_validate_generic_ingest_inputs_lambda_name = local.ingest_validate_generic_ingest_inputs_lambda_name
    ingest_mapper_lambda_name                         = local.ingest_mapper_lambda_name
    ingest_find_existing_asset_name_lambda_name       = local.ingest_find_existing_asset_name
    ingest_asset_opex_creator_lambda_name             = local.ingest_asset_opex_creator_lambda_name
    ingest_folder_opex_creator_lambda_name            = local.ingest_folder_opex_creator_lambda_name
    ingest_parent_folder_opex_creator_lambda_name     = local.ingest_parent_folder_opex_creator_lambda_name
    ingest_asset_reconciler_lambda_name               = local.ingest_asset_reconciler_lambda_name
    ingest_lock_table_name                            = var.ingest_lock_table.name
    ingest_lock_table_group_id_gsi_name               = var.ingest_lock_table.group_id_gsi_name
    ingest_lock_table_hash_key                        = var.ingest_lock_table.hash_key
    ingest_run_workflow_sfn_name                      = local.ingest_run_workflow_step_function_name
    notifications_topic_name                          = var.notifications_topic.name
    ingest_state_bucket_name                          = var.bucket_names.ingest_state_bucket_name
    preservica_bucket_name                            = local.preservica_ingest_bucket
    ingest_files_table_name                           = local.files_dynamo_table_name
    ingest_queue_table_name                           = local.ingest_queue_dynamo_table_name
    ingest_flow_control_lambda_name                   = local.ingest_flow_control_lambda_name
    retry_statement                                   = local.retry_statement
    postingest_table_name                             = var.postingest.table_name
  })
  step_function_name = local.ingest_step_function_name
  step_function_role_policy_attachments = {
    step_function_policy = module.dr2_ingest_step_function_policy.policy_arn
  }
}

module "dr2_ingest_run_workflow_step_function" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//sfn"
  step_function_definition = templatefile("${path.root}/templates/sfn/ingest_run_workflow_sfn_definition.json.tpl", {
    step_function_name                        = local.ingest_run_workflow_step_function_name
    account_id                                = data.aws_caller_identity.current.account_id
    ingest_upsert_archive_folders_lambda_name = local.ingest_upsert_archive_folders_lambda_name
    ingest_start_workflow_lambda_name         = local.ingest_start_workflow_lambda_name
    ingest_workflow_monitor_lambda_name       = local.ingest_workflow_monitor_lambda_name
    retry_statement                           = local.retry_statement,
    upsert_lambda_retry_statement             = jsonencode([{ ErrorEquals = ["States.ALL"], IntervalSeconds = module.dr2_ingest_upsert_archive_folders_lambda.lambda_function.timeout, MaxAttempts = 10, BackoffRate = 1, JitterStrategy = "FULL" }])
  })
  step_function_name = local.ingest_run_workflow_step_function_name
  step_function_role_policy_attachments = {
    step_function_policy = module.dr2_ingest_run_workflow_step_function_policy.policy_arn
  }
}

module "dr2_ingest_step_function_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.environment}-dr2-ingest-step-function-policy"
  policy_string = templatefile("${path.root}/templates/iam_policy/ingest_step_function_policy.json.tpl", {
    account_id                                        = data.aws_caller_identity.current.account_id
    ingest_validate_generic_ingest_inputs_lambda_name = local.ingest_validate_generic_ingest_inputs_lambda_name
    ingest_mapper_lambda_name                         = local.ingest_mapper_lambda_name
    ingest_upsert_archive_folders_lambda_name         = local.ingest_upsert_archive_folders_lambda_name
    ingest_find_existing_asset_lambda_name            = local.ingest_find_existing_asset_name
    ingest_asset_opex_creator_lambda_name             = local.ingest_asset_opex_creator_lambda_name
    ingest_folder_opex_creator_lambda_name            = local.ingest_folder_opex_creator_lambda_name
    ingest_parent_folder_opex_creator_lambda_name     = local.ingest_parent_folder_opex_creator_lambda_name
    ingest_start_workflow_lambda_name                 = local.ingest_start_workflow_lambda_name
    ingest_workflow_monitor_lambda_name               = local.ingest_workflow_monitor_lambda_name
    ingest_asset_reconciler_lambda_name               = local.ingest_asset_reconciler_lambda_name
    ingest_flow_control_lambda_name                   = local.ingest_flow_control_lambda_name
    ingest_lock_table_name                            = var.ingest_lock_table.name
    ingest_lock_table_group_id_gsi_name               = var.ingest_lock_table.group_id_gsi_name
    notifications_topic_name                          = var.notifications_topic.name
    ingest_queue_table_name                           = local.ingest_queue_dynamo_table_name
    ingest_state_bucket_name                          = var.bucket_names.ingest_state_bucket_name
    ingest_sfn_name                                   = local.ingest_step_function_name
    ingest_run_workflow_sfn_name                      = local.ingest_run_workflow_step_function_name
    ingest_files_table_name                           = local.files_dynamo_table_name
    tna_to_preservica_role_arn                        = local.tna_to_preservica_role_arn
    preingest_tdr_step_function_arn                   = var.preingest.tdr.sfn_arn
    preingest_dri_step_function_arn                   = var.preingest.dri.sfn_arn
    ingest_run_workflow_sfn_arn                       = local.ingest_run_workflow_sfn_arn
    postingest_table_name                             = var.postingest.table_name
  })
}

module "dr2_ingest_run_workflow_step_function_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.environment}-dr2-ingest-run-workflow-step-function-policy"
  policy_string = templatefile("${path.root}/templates/iam_policy/ingest_run_workflow_step_function_policy.json.tpl", {
    account_id                                = data.aws_caller_identity.current.account_id
    ingest_upsert_archive_folders_lambda_name = local.ingest_upsert_archive_folders_lambda_name
    ingest_start_workflow_lambda_name         = local.ingest_start_workflow_lambda_name
    ingest_workflow_monitor_lambda_name       = local.ingest_workflow_monitor_lambda_name,
    ingest_step_function_name                 = local.ingest_step_function_name
  })
}

module "files_table" {
  source                         = "git::https://github.com/nationalarchives/da-terraform-modules//dynamo"
  hash_key                       = { name = "id", type = "S" }
  range_key                      = { name = "batchId", type = "S" }
  table_name                     = local.files_dynamo_table_name
  server_side_encryption_enabled = true
  kms_key_arn                    = var.kms_key_arn
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

module "ingest_queue_table" {
  source                         = "git::https://github.com/nationalarchives/da-terraform-modules//dynamo"
  hash_key                       = { name = "sourceSystem", type = "S" }
  range_key                      = { name = "queuedAt", type = "S" }
  table_name                     = local.ingest_queue_dynamo_table_name
  server_side_encryption_enabled = false
  deletion_protection_enabled    = true
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
    cloudwatch_alarms = jsonencode(flatten([[for queue in local.queues : queue.event_alarms], [var.postingest.cc_confirmer_queue_oldest_message_alarm_arn]])),
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

module "failed_ingest_step_function_event_bridge_rule" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  event_pattern = templatefile("${path.root}/templates/eventbridge/step_function_failed_event_pattern.json.tpl", {
    step_function_arns = jsonencode([
      module.dr2_ingest_step_function.step_function_arn,
      var.preingest.tdr.sfn_arn,
      var.preingest.dri.sfn_arn,
      module.dr2_ingest_run_workflow_step_function.step_function_arn
    ])
  })
  name                = "${local.environment}-dr2-eventbridge-ingest-step-function-failure"
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
  lambda_target_arn = "arn:aws:lambda:eu-west-2:${data.aws_caller_identity.current.account_id}:function:${local.ingest_failure_notifications_lambda_name}"
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
