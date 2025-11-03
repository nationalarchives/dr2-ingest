import os
import json
import boto3

eventbridge = boto3.client("events")
secretsmanager = boto3.client("secretsmanager")


def send_eventbridge_message(detail):
    eventbridge.put_events(
        Entries=[
            {
                "Source": "preservica-activity-pause",
                "DetailType": "DR2DevMessage",
                "Detail": json.dumps(detail),
                "EventBusName": "default"
            }
        ]
    )

def disable_secret_rotation(secrets_manager_details):
    for secret_detail in secrets_manager_details:
        secretsmanager.cancel_rotate_secret(SecretId=secret_detail['id'])

def enable_secret_rotation(secrets_manager_details):
    for secret_detail in secrets_manager_details:
        secretsmanager.rotate_secret(
            SecretId=secret_detail['id'],
            RotationLambdaARN=secret_detail['lambda_arn'],
            RotationRules={'ScheduleExpression': secret_detail['schedule_expression']},
            RotateImmediately=False
        )


def activity_paused(rule_name, secrets_manager_details):
    rotation_disabled = False
    for detail in secrets_manager_details:
        resp = secretsmanager.describe_secret(SecretId=detail['id'])
        rotation = resp.get('RotationEnabled', False)
        if not rotation:
            rotation_disabled = True
            break

    rule_desc = eventbridge.describe_rule(Name=rule_name)
    rule_disabled = rule_desc.get('State', 'ENABLED').upper() != 'ENABLED'
    return rotation_disabled or rule_disabled


def lambda_handler(event, context):
    secrets_manager_details = json.loads(os.environ.get("SECRETS_MANAGER_DETAILS"))
    environment = os.environ.get("ENVIRONMENT")
    rule_name = f"{environment}-dr2-entity-event-schedule"
    if 'pause' in event:
        if event['pause']:
            pause_message = {
                "slackMessage": f":alert-noflash-slow: Preservica activity has been paused in environment {environment}"
            }
            disable_secret_rotation(secrets_manager_details)
            eventbridge.disable_rule(Name=rule_name)
            send_eventbridge_message(pause_message)
        else:
            enable_secret_rotation(secrets_manager_details)
            eventbridge.enable_rule(Name=rule_name)
            resume_message = {
                "slackMessage": f":green-tick: Preservica activity has been resumed in environment {environment}"
            }
            send_eventbridge_message(resume_message)

    elif event.get("source") == "aws.events" and activity_paused(rule_name, secrets_manager_details):
        still_paused_message = {
            "slackMessage": f":alert-noflash-slow: Preservica activity is still paused on environment {environment}"
        }
        send_eventbridge_message(still_paused_message)
