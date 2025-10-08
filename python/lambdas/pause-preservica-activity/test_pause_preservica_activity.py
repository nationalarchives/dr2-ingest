import pytest
from unittest.mock import patch, call
import json
import pause_preservica_activity

secrets_manager_details = [
    {"id": "secret-arn-1", "lambda_arn": "lambda-arn-1", "schedule_expression": "expression1"},
    {"id": "secret-arn-2", "lambda_arn": "lambda-arn-2", "schedule_expression": "expression2"},
]


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv('SECRETS_MANAGER_DETAILS', json.dumps(secrets_manager_details))
    monkeypatch.setenv('ENVIRONMENT', 'test')


@pytest.fixture
def boto3_mocks():
    with patch('pause_preservica_activity.eventbridge') as eventbridge_mock, \
            patch('pause_preservica_activity.secretsmanager') as secretsmanager_mock:
        yield eventbridge_mock, secretsmanager_mock


def test_pause_preservica_activity(env, boto3_mocks):
    eventbridge_mock, secretsmanager_mock = boto3_mocks

    event = {'pause': True}
    pause_preservica_activity.lambda_handler(event, None)
    detail = {"slackMessage": ":alert-noflash-slow: Preservica activity has been paused in environment test"}
    eventbridge_mock.put_events.assert_called_once_with(
        Entries=[{
            "Source": "preservica-activity-pause",
            "DetailType": "DR2DevMessage",
            "Detail": json.dumps(detail),
            "EventBusName": "default"
        }]
    )

    eventbridge_mock.disable_rule.assert_called_once_with(Name="test-dr2-entity-event-schedule")

    secretsmanager_mock.cancel_rotate_secret.assert_has_calls([
        call(SecretId='secret-arn-1'),
        call(SecretId='secret-arn-2'),
    ], any_order=True)


def test_resume_preservica_activity(env, boto3_mocks):
    eventbridge_mock, secretsmanager_mock = boto3_mocks

    event = {'pause': False}
    pause_preservica_activity.lambda_handler(event, None)

    eventbridge_mock.put_events.assert_called_once_with(
        Entries=[{
            "Source": "preservica-activity-pause",
            "DetailType": "DR2DevMessage",
            "Detail": '{"slackMessage": ":green-tick: Preservica activity has been resumed in environment test"}',
            "EventBusName": "default"
        }]
    )

    secretsmanager_mock.rotate_secret.assert_has_calls([
        call(
            SecretId='secret-arn-1',
            RotationLambdaARN='lambda-arn-1',
            RotationRules={'ScheduleExpression': 'expression1'},
            RotateImmediately=False
        ),
        call(
            SecretId='secret-arn-2',
            RotationLambdaARN='lambda-arn-2',
            RotationRules={'ScheduleExpression': 'expression2'},
            RotateImmediately=False
        ),
    ], any_order=True)

    eventbridge_mock.enable_rule.assert_called_once_with(Name="test-dr2-entity-event-schedule")


def test_all_secrets_enabled_rotation_enabled(env, boto3_mocks):
    eventbridge_mock, secretsmanager_mock = boto3_mocks
    secretsmanager_mock.describe_secret.side_effect = [
        {'ARN': 'secret-arn-1', 'RotationEnabled': True},
        {'ARN': 'secret-arn-2', 'RotationEnabled': True},
    ]
    eventbridge_mock.describe_rule.side_effect = [
        {'State': 'ENABLED'}
    ]
    event = {'source': 'aws.events'}
    pause_preservica_activity.lambda_handler(event, None)
    eventbridge_mock.put_events.assert_not_called()


def test_one_secret_rotation_disabled(env, boto3_mocks):
    eventbridge_mock, secretsmanager_mock = boto3_mocks
    secretsmanager_mock.describe_secret.side_effect = [
        {'ARN': 'secret-arn-1', 'RotationEnabled': True},
        {'ARN': 'secret-arn-2', 'RotationEnabled': False},
    ]
    eventbridge_mock.describe_rule.side_effect = [
        {'State': 'ENABLED'}
    ]
    event = {'source': 'aws.events'}
    pause_preservica_activity.lambda_handler(event, None)
    detail = {"slackMessage": ":alert-noflash-slow: Preservica activity is still paused on environment test"}
    eventbridge_mock.put_events.assert_called_once_with(
        Entries=[{
            "Source": "preservica-activity-pause",
            "DetailType": "DR2DevMessage",
            "Detail": json.dumps(detail),
            "EventBusName": "default"
        }]
    )


def test_all_secret_rotation_disabled(env, boto3_mocks):
    eventbridge_mock, secretsmanager_mock = boto3_mocks
    secretsmanager_mock.describe_secret.side_effect = [
        {'ARN': 'secret-arn-1', 'RotationEnabled': False},
        {'ARN': 'secret-arn-2', 'RotationEnabled': False},
    ]
    eventbridge_mock.describe_rule.side_effect = [
        {'State': 'ENABLED'}
    ]
    event = {'source': 'aws.events'}
    pause_preservica_activity.lambda_handler(event, None)

    detail = {"slackMessage": ":alert-noflash-slow: Preservica activity is still paused on environment test"}
    eventbridge_mock.put_events.assert_called_once_with(
        Entries=[{
            "Source": "preservica-activity-pause",
            "DetailType": "DR2DevMessage",
            "Detail": json.dumps(detail),
            "EventBusName": "default"
        }]
    )


def test_secret_rotation_enabled_rule_disabled(env, boto3_mocks):
    eventbridge_mock, secretsmanager_mock = boto3_mocks
    secretsmanager_mock.describe_secret.side_effect = [
        {'ARN': 'secret-arn-1', 'RotationEnabled': True},
        {'ARN': 'secret-arn-2', 'RotationEnabled': True},
    ]
    eventbridge_mock.describe_rule.side_effect = [
        {'State': 'DISABLED'}
    ]
    event = {'source': 'aws.events'}
    pause_preservica_activity.lambda_handler(event, None)

    detail = {"slackMessage": ":alert-noflash-slow: Preservica activity is still paused on environment test"}
    eventbridge_mock.put_events.assert_called_once_with(
        Entries=[{
            "Source": "preservica-activity-pause",
            "DetailType": "DR2DevMessage",
            "Detail": json.dumps(detail),
            "EventBusName": "default"
        }]
    )


def test_no_eventbridge_message_if_empty_input(env, boto3_mocks):
    eventbridge_mock, _ = boto3_mocks
    eventbridge_mock.describe_rule.side_effect = [{'State': 'DISABLED'}]
    event = {}
    pause_preservica_activity.lambda_handler(event, None)
    eventbridge_mock.put_events.assert_not_called()

def test_eventbridge_error(env, boto3_mocks):
    eventbridge_mock, secretsmanager_mock = boto3_mocks

    eventbridge_mock.put_events.side_effect = Exception("EventBridge error")

    event = {'pause': True}
    with pytest.raises(Exception, match="EventBridge error"):
        pause_preservica_activity.lambda_handler(event, None)


def test_disable_rule_error(env, boto3_mocks):
    eventbridge_mock, _ = boto3_mocks
    eventbridge_mock.disable_rule.side_effect = Exception("Disable rule error")

    event = {'pause': True}
    with pytest.raises(Exception, match="Disable rule error"):
        pause_preservica_activity.lambda_handler(event, None)


def test_secrets_manager_error(env, boto3_mocks):
    _, secretsmanager_mock = boto3_mocks
    secretsmanager_mock.cancel_rotate_secret.side_effect = Exception("Secrets Manager error")

    event = {'pause': True}
    with pytest.raises(Exception, match="Secrets Manager error"):
        pause_preservica_activity.lambda_handler(event, None)
