module "cloudwatch_event_alarm_event_bridge_rule_alarm_only" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  event_pattern = templatefile("${path.module}/templates/cloudwatch_alarm_event_pattern.json.tpl", {
    cloudwatch_alarms = jsonencode(flatten([module.dr2_importer_sqs.event_alarms, module.dr2_preingest_aggregator_queue.event_alarms])),
    state_value       = "ALARM"
  })
  name                = "${local.environment}-dr2-eventbridge-${var.source_name}-queue-alarm-only"
  api_destination_arn = var.slack_api_destination_arn
  api_destination_input_transformer = {
    input_paths = {
      "alarmName"    = "$.detail.alarmName",
      "currentValue" = "$.detail.state.value"
    }
    input_template = templatefile("${path.module}/templates/slack_message_input_template.json.tpl", {
      channel_id   = var.general_notifications_channel_id
      slackMessage = ":warning: Cloudwatch alarm <alarmName> has entered state <currentValue>"
    })
  }
}

module "cloudwatch_alarm_event_bridge_rule" {
  for_each = toset(["OK", "ALARM"])
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  event_pattern = templatefile("${path.module}/templates/cloudwatch_alarm_event_pattern.json.tpl", {
    cloudwatch_alarms = jsonencode(flatten([module.dr2_importer_sqs.alarms, module.dr2_preingest_aggregator_queue.alarms]))
    state_value       = each.value
  })
  name                = "${local.environment}-dr2-eventbridge-${var.source_name}-preingest-queues-${lower(each.value)}"
  api_destination_arn = var.slack_api_destination_arn
  api_destination_input_transformer = {
    input_paths = {
      "alarmName"    = "$.detail.alarmName",
      "currentValue" = "$.detail.state.value"
    }
    input_template = templatefile("${path.module}/templates/slack_message_input_template.json.tpl", {
      channel_id   = var.general_notifications_channel_id
      slackMessage = ":${each.value == "OK" ? "green-tick" : "alert-noflash-slow"}: Cloudwatch alarm <alarmName> has entered state <currentValue>"
    })
  }
}