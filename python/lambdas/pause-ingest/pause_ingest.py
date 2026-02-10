import os
import json
import boto3

eventbridge = boto3.client("events")
lambda_client = boto3.client("lambda")
ssm_client = boto3.client("ssm")

def send_eventbridge_message(detail):
    eventbridge.put_events(
        Entries=[
            {
                "Source": "pause-ingest",
                "DetailType": "DR2DevMessage",
                "Detail": json.dumps(detail),
                "EventBusName": "default"
            }
        ]
    )


def update_sqs_trigger(trigger_arns, enabled):
    for trigger_arn in trigger_arns:
        event_source_mappings = lambda_client.list_event_source_mappings(EventSourceArn=trigger_arn)
        for mapping in event_source_mappings.get("EventSourceMappings", []):
            uuid = mapping["UUID"]
            lambda_client.update_event_source_mapping(
                UUID=uuid,
                Enabled=enabled
            )


def get_flow_control_config(ssm_parameter_name):
    param = ssm_client.get_parameter(Name=ssm_parameter_name, WithDecryption=True)
    return json.loads(param["Parameter"]["Value"])


def set_flow_control_enabled(ssm_parameter_name, enabled):
    flow_config = get_flow_control_config(ssm_parameter_name)
    flow_config['enabled'] = enabled
    ssm_client.put_parameter(
        Name=ssm_parameter_name,
        Value=json.dumps(flow_config, indent=2),
        Overwrite=True
    )

def ingest_paused(ssm_parameter_name, trigger_arns):
    paused = False
    for trigger_arn in trigger_arns:
        mappings = lambda_client.list_event_source_mappings(EventSourceArn=trigger_arn)
        for mapping in mappings.get("EventSourceMappings", []):
            if not mapping.get("Enabled", True):
                paused = True
    flow_config = get_flow_control_config(ssm_parameter_name)
    if not flow_config.get("enabled"):
        paused = True
    return paused

def lambda_handler(event, context):
    trigger_arns = json.loads(os.environ.get("TRIGGER_ARNS"))
    environment = os.environ.get("ENVIRONMENT")
    ssm_parameter_name = f"/{environment}/flow-control-config"

    if 'pause' in event:
        if event['pause']:
            pause_message = {
                "slackMessage": f":alert-noflash-slow: Ingest has been paused in environment {environment}"
            }
            update_sqs_trigger(trigger_arns, False)
            set_flow_control_enabled(ssm_parameter_name, False)
            send_eventbridge_message(pause_message)
        else:
            resume_message = {
                "slackMessage": f":green-tick: Ingest has been resumed in environment {environment}"
            }
            update_sqs_trigger(trigger_arns, True)
            set_flow_control_enabled(ssm_parameter_name, True)
            send_eventbridge_message(resume_message)

    if event.get("source") == "aws.events" and ingest_paused(ssm_parameter_name, trigger_arns):
        still_paused_message = {
            "slackMessage": f":alert-noflash-slow: Ingest is still paused on environment {environment}"
        }
        send_eventbridge_message(still_paused_message)
