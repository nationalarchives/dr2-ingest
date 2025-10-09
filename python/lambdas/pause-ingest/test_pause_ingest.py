import pytest
from unittest.mock import patch, call
import json
import pause_ingest

@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv('TRIGGER_ARNS', '["agg-arn-1","agg-arn-2","court-arn"]')
    monkeypatch.setenv('SSM_PARAMETER_NAME', 'param-name')
    monkeypatch.setenv('ENVIRONMENT', 'test')

@pytest.fixture
def boto3_mocks():
    with patch('pause_ingest.eventbridge') as eventbridge_mock, \
         patch('pause_ingest.lambda_client') as lambda_mock, \
         patch('pause_ingest.ssm_client') as ssm_mock:
        yield eventbridge_mock, lambda_mock, ssm_mock

def setup_ssm(ssm_mock, max_concurrency=5, previous_max=None):
    value = {'maxConcurrency': max_concurrency}
    if previous_max is not None:
        value['previousMaxConcurrency'] = previous_max
    ssm_mock.get_parameter.return_value = {
        'Parameter': {'Value': json.dumps(value)}
    }

def test_pause_ingest(env, boto3_mocks):
    eventbridge_mock, lambda_mock, ssm_mock = boto3_mocks

    setup_ssm(ssm_mock, max_concurrency=5)

    lambda_mock.list_event_source_mappings.side_effect = [
        {'EventSourceMappings': [{'UUID': 'agg-uuid-1'}]},
        {'EventSourceMappings': [{'UUID': 'agg-uuid-2'}]},
        {'EventSourceMappings': [{'UUID': 'court-uuid'}]}
    ]

    event = {'pause': True}
    pause_ingest.lambda_handler(event, None)

    eventbridge_mock.put_events.assert_called_once_with(
        Entries=[{
            "Source": "pause-ingest",
            "DetailType": "DR2DevMessage",
            "Detail": '{"slackMessage": ":alert-noflash-slow: Ingest has been paused in environment test"}',
            "EventBusName": "default"
        }]
    )

    lambda_mock.update_event_source_mapping.assert_has_calls([
        call(UUID='agg-uuid-1', Enabled=False),
        call(UUID='agg-uuid-2', Enabled=False),
        call(UUID='court-uuid', Enabled=False)
    ], any_order=True)

    _, kwargs = ssm_mock.put_parameter.call_args
    value = json.loads(kwargs['Value'])
    assert value['maxConcurrency'] == 0
    assert value['previousMaxConcurrency'] == 5

def test_resume_ingest(env, boto3_mocks):
    eventbridge_mock, lambda_mock, ssm_mock = boto3_mocks

    setup_ssm(ssm_mock, max_concurrency=0, previous_max=5)

    lambda_mock.list_event_source_mappings.side_effect = [
        {'EventSourceMappings': [{'UUID': 'agg-uuid-1'}]},
        {'EventSourceMappings': [{'UUID': 'agg-uuid-2'}]},
        {'EventSourceMappings': [{'UUID': 'court-uuid'}]}
    ]

    event = {'pause': False}
    pause_ingest.lambda_handler(event, None)

    eventbridge_mock.put_events.assert_called_once_with(
        Entries=[{
            "Source": "pause-ingest",
            "DetailType": "DR2DevMessage",
            "Detail": '{"slackMessage": ":green-tick: Ingest has been resumed in environment test"}',
            "EventBusName": "default"
        }]
    )

    lambda_mock.update_event_source_mapping.assert_has_calls([
        call(UUID='agg-uuid-1', Enabled=True),
        call(UUID='agg-uuid-2', Enabled=True),
        call(UUID='court-uuid', Enabled=True)
    ], any_order=True)

    args, kwargs = ssm_mock.put_parameter.call_args
    value = json.loads(kwargs['Value'])
    assert value['maxConcurrency'] == 5
    assert 'previousMaxConcurrency' not in value

def test_scheduled_event_both_enabled_max_gt_zero(env, boto3_mocks):
    eventbridge_mock, lambda_mock, ssm_mock = boto3_mocks
    lambda_mock.list_event_source_mappings.side_effect = [
        {'EventSourceMappings': [{'UUID': 'agg-uuid-1', 'Enabled': True}]},
        {'EventSourceMappings': [{'UUID': 'agg-uuid-2', 'Enabled': True}]},
        {'EventSourceMappings': [{'UUID': 'court-uuid', 'Enabled': True}]}
    ]
    setup_ssm(ssm_mock, max_concurrency=5)
    event = {'source': 'aws.events'}
    pause_ingest.lambda_handler(event, None)
    eventbridge_mock.put_events.assert_not_called()

def test_scheduled_event_agg_disabled(env, boto3_mocks):
    eventbridge_mock, lambda_mock, ssm_mock = boto3_mocks
    lambda_mock.list_event_source_mappings.side_effect = [
        {'EventSourceMappings': [{'UUID': 'agg-uuid-1', 'Enabled': False}]},
        {'EventSourceMappings': [{'UUID': 'agg-uuid-2', 'Enabled': False}]},
        {'EventSourceMappings': [{'UUID': 'court-uuid', 'Enabled': True}]}
    ]
    setup_ssm(ssm_mock, max_concurrency=5)
    event = {'source': 'aws.events'}
    pause_ingest.lambda_handler(event, None)
    eventbridge_mock.put_events.assert_called_once_with(
        Entries=[{
            "Source": "pause-ingest",
            "DetailType": "DR2DevMessage",
            "Detail": '{"slackMessage": ":alert-noflash-slow: Ingest is still paused on environment test"}',
            "EventBusName": "default"
        }]
    )

def test_scheduled_event_court_disabled(env, boto3_mocks):
    eventbridge_mock, lambda_mock, ssm_mock = boto3_mocks
    lambda_mock.list_event_source_mappings.side_effect = [
        {'EventSourceMappings': [{'UUID': 'agg-uuid-1', 'Enabled': True}]},
        {'EventSourceMappings': [{'UUID': 'agg-uuid-2', 'Enabled': True}]},
        {'EventSourceMappings': [{'UUID': 'court-uuid', 'Enabled': False}]}
    ]
    setup_ssm(ssm_mock, max_concurrency=5)
    event = {'source': 'aws.events'}
    pause_ingest.lambda_handler(event, None)
    eventbridge_mock.put_events.assert_called_once_with(
        Entries=[{
            "Source": "pause-ingest",
            "DetailType": "DR2DevMessage",
            "Detail": '{"slackMessage": ":alert-noflash-slow: Ingest is still paused on environment test"}',
            "EventBusName": "default"
        }]
    )

def test_scheduled_event_max_concurrency_zero(env, boto3_mocks):
    eventbridge_mock, lambda_mock, ssm_mock = boto3_mocks
    lambda_mock.list_event_source_mappings.side_effect = [
        {'EventSourceMappings': [{'UUID': 'agg-uuid-1', 'Enabled': True}]},
        {'EventSourceMappings': [{'UUID': 'agg-uuid-2', 'Enabled': True}]},
        {'EventSourceMappings': [{'UUID': 'court-uuid', 'Enabled': True}]}
    ]
    setup_ssm(ssm_mock, max_concurrency=0)
    event = {'source': 'aws.events'}
    pause_ingest.lambda_handler(event, None)
    eventbridge_mock.put_events.assert_called_once_with(
        Entries=[{
            "Source": "pause-ingest",
            "DetailType": "DR2DevMessage",
            "Detail": '{"slackMessage": ":alert-noflash-slow: Ingest is still paused on environment test"}',
            "EventBusName": "default"
        }]
    )

def test_no_eventbridge_message_if_empty_input(env, boto3_mocks):
    eventbridge_mock, lambda_mock, ssm_mock = boto3_mocks
    lambda_mock.list_event_source_mappings.side_effect = [
        {'EventSourceMappings': [{'UUID': 'agg-uuid-1', 'Enabled': True}]},
        {'EventSourceMappings': [{'UUID': 'agg-uuid-2', 'Enabled': True}]},
        {'EventSourceMappings': [{'UUID': 'court-uuid', 'Enabled': True}]}
    ]
    setup_ssm(ssm_mock, max_concurrency=0)
    event = {}
    pause_ingest.lambda_handler(event, None)
    eventbridge_mock.put_events.assert_not_called()
def test_eventbridge_error(env, boto3_mocks):
    eventbridge_mock, lambda_mock, ssm_mock = boto3_mocks
    setup_ssm(ssm_mock, max_concurrency=5)

    eventbridge_mock.put_events.side_effect = Exception("EventBridge error")

    event = {'pause': True}
    with pytest.raises(Exception, match="EventBridge error"):
        pause_ingest.lambda_handler(event, None)

def test_lambda_error(env, boto3_mocks):
    eventbridge_mock, lambda_mock, ssm_mock = boto3_mocks
    setup_ssm(ssm_mock, max_concurrency=5)
    lambda_mock.list_event_source_mappings.side_effect = Exception("Lambda error")

    event = {'pause': True}
    with pytest.raises(Exception, match="Lambda error"):
        pause_ingest.lambda_handler(event, None)

def test_ssm_error(env, boto3_mocks):
    eventbridge_mock, lambda_mock, ssm_mock = boto3_mocks
    ssm_mock.get_parameter.side_effect = Exception("SSM error")

    event = {'pause': True}
    with pytest.raises(Exception, match="SSM error"):
        pause_ingest.lambda_handler(event, None)
