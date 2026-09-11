import json
import os
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import ingest_metric_collector

SOURCE_SYSTEMS = ["TDR", "COURTDOC", "ADHOC", "DRI", "DEFAULT"]
MAPPER_LAMBDA_STATE_NAME = "Get metadata and update Files table"


def generate_metrics(state_machine_arn="arn:some_arn", state_machine_name="test-dr2-something", value=0,
                     metric_name="ExecutionsRunning", source_system="", unit="Count"):
    metrics = {
        "MetricName": metric_name,
        "Dimensions": [
            {"Name": "StateMachineArn", "Value": state_machine_arn},
            {"Name": "StateMachineName", "Value": state_machine_name},
        ],
        "Value": value,
        "Unit": unit
    }
    if source_system and metric_name == "ExecutionsRunning":
        metrics["Dimensions"].append({"Name": "SourceSystem", "Value": source_system})
    elif source_system:
        metrics["Dimensions"] = [{"Name": "SourceSystem", "Value": source_system}]
    else:
        pass

    return metrics


@patch.dict(os.environ, {"SOURCE_SYSTEMS": json.dumps(SOURCE_SYSTEMS),
                         "MAPPER_LAMBDA_STATE_NAME": json.dumps(MAPPER_LAMBDA_STATE_NAME)})
class TestLambdaFunction(unittest.TestCase):
    expected_source_systems = ("TDR", "COURTDOC", "ADHOC", "DRI", "DEFAULT")

    @patch("ingest_metric_collector.boto3.client")
    def test_get_stepfunction_metrics_should_return_empty_metrics_when_there_are_no_state_machines(self,
                                                                                                   mock_boto_client):
        mock_sfn = MagicMock()
        mock_sfn.get_paginator.return_value.paginate.return_value = [{"stateMachines": []}]
        mock_boto_client.return_value = mock_sfn

        metrics = ingest_metric_collector.get_stepfunction_metrics("env-prefix", SOURCE_SYSTEMS,
                                                                   MAPPER_LAMBDA_STATE_NAME)
        self.assertEqual([], metrics)

    @patch("ingest_metric_collector.boto3.client")
    def test_get_stepfunction_metrics_should_return_single_metric_when_no_executions_and_state_machine_not_from_known_source_systems(
            self, mock_boto_client):
        mock_sfn = MagicMock()
        mock_sfn.get_paginator.return_value.paginate.return_value = [
            {"stateMachines": [{"name": "unknown-ss-something", "stateMachineArn": "arn:some_arn"}]}
        ]
        mock_sfn.list_executions.return_value = {"executions": []}
        mock_boto_client.return_value = mock_sfn

        metrics = ingest_metric_collector.get_stepfunction_metrics("TDR-", SOURCE_SYSTEMS,
                                                                   MAPPER_LAMBDA_STATE_NAME)
        # Should return one metric with 0 executions
        self.assertEqual(1, len(metrics))

        expected_metric = generate_metrics("arn:some_arn", "unknown-ss-something", 0)
        self.assertEqual(expected_metric, metrics[0])

    @patch("ingest_metric_collector.boto3.client")
    def test_get_stepfunction_metrics_should_return_metric_per_source_system_when_source_system_is_known(self,
                                                                                                         mock_boto_client):
        mock_sfn = MagicMock()
        mock_sfn.get_paginator.return_value.paginate.return_value = [
            {"stateMachines": [{"name": "test-dr2-something", "stateMachineArn": "arn:some_arn"}]}
        ]
        mock_sfn.list_executions.return_value = {"executions": []}
        mock_boto_client.return_value = mock_sfn

        metrics = ingest_metric_collector.get_stepfunction_metrics("test-dr2-", SOURCE_SYSTEMS,
                                                                   MAPPER_LAMBDA_STATE_NAME)

        self.assertEqual(6, len(metrics))
        expected_metric = generate_metrics()
        self.assertEqual(expected_metric, metrics[0])

        for n, ss in enumerate(self.expected_source_systems):
            expected_metric = generate_metrics(source_system=ss)
            self.assertEqual(expected_metric, metrics[n + 1])

    @patch("ingest_metric_collector.boto3.client")
    def test_get_stepfunction_metrics_should_return_metrics_when_executions_and_source_system_exist(self,
                                                                                                    mock_boto_client):
        mock_sfn = MagicMock()
        state_machine_mock = MagicMock()
        state_machine_mock.paginate.return_value = [
            {"stateMachines": [{"name": "test-dr2-something", "stateMachineArn": "arn:some_arn"}]}
        ]
        get_execution_history_mock = MagicMock()
        executions_list = [
            {"name": "TDR_job1", "executionArn": "arn:aws:states:region:123456789012:execution:test-dr2:TDR_job1",
             "stateMachineArn": "arn:aws:states:region:123456789012:stateMachine:test-dr2"},
            {"name": "COURTDOC_task1",
             "executionArn": "arn:aws:states:region:123456789012:execution:stateMachine:COURTDOC_task1",
             "stateMachineArn": "arn:aws:states:region:123456789012:stateMachine:test-dr2"
             },
            # unknown ss should get added to DEFAULT
            {"name": "RANDOM_job2",
             "executionArn": "arn:aws:states:region:123456789012:execution:stateMachine:RANDOM_job2",
             "stateMachineArn": "arn:aws:states:region:123456789012:stateMachine:test-dr2"},
            # This execution should be excluded because the state machine arn does not end with "test-dr2"
            {"name": "RANDOM_job3",
             "executionArn": "arn:aws:states:region:123456789012:execution:stateMachine:RANDOM_job3",
             "stateMachineArn": "arn:aws:states:region:123456789012:stateMachine:not-test-dr2"}
        ]

        events = [[
            {
                "events": [
                    {
                        "eventId": 1,
                        "type": "ExecutionStarted"
                    },
                    {
                        "eventId": 2,
                        "type": "TaskStateExited",
                        "stateExitedEventDetails": {
                            "name": "Get metadata and update Files table",
                            "output": f"""{{"totalAssetCount":{n + 1},"totalFileBytes":{(n + 1) * 1000}}}"""
                        }
                    }
                ]
            }]
            for n, execution in enumerate(executions_list)
        ]

        get_execution_history_mock.paginate.side_effect = events
        arg_to_mock = {"list_state_machines": state_machine_mock, "get_execution_history": get_execution_history_mock}
        mock_sfn.get_paginator.side_effect = lambda arg: arg_to_mock[arg]

        mock_sfn.list_executions.return_value = {"executions": executions_list}
        mock_boto_client.return_value = mock_sfn

        metrics = ingest_metric_collector.get_stepfunction_metrics("test-dr2", SOURCE_SYSTEMS,
                                                                   MAPPER_LAMBDA_STATE_NAME)

        expected_executions_running = 1
        expected_executions_running_per_ss = len(self.expected_source_systems)
        expected_asset_count_and_bytes = (len(executions_list) - 1) * 2
        expected_metrics_length = expected_executions_running + expected_executions_running_per_ss + expected_asset_count_and_bytes

        self.assertEqual(expected_metrics_length, len(metrics))

        total_executions_running = metrics.pop(0)
        expected_executions = generate_metrics(value=len(executions_list))
        self.assertEqual(expected_executions, total_executions_running)

        for (ss, executions) in zip(self.expected_source_systems, (1, 1, 0, 0, 2)):
            expected_metric = generate_metrics(value=executions, source_system=ss)
            self.assertEqual(expected_metric, metrics.pop(0))

        for (ss, count, total_bytes) in (("TDR", 1, 1000), ("COURTDOC", 2, 2000), ("DEFAULT", 3, 3000)):
            count_expected_metric = generate_metrics(value=count, metric_name="AssetCount", source_system=ss)
            self.assertEqual(count_expected_metric, metrics.pop(0))

            bytes_expected_metric = generate_metrics(value=total_bytes, metric_name="Bytes", source_system=ss)
            self.assertEqual(bytes_expected_metric, metrics.pop(0))

        self.assertEqual(len(metrics), 0)

    @patch("ingest_metric_collector.boto3.client")
    def test_get_stepfunction_metrics_should_throw_an_error_if(self, mock_boto_client):
        cases = [
            ("no 'TaskStateExited' events are present",
             "Task 'Get metadata and update Files table' not found in list of events with the status 'TaskStateExited'"),
            ("no 'TaskStateExited' events with the expected Mapper lambda name",
             f"Task 'Get metadata and update Files table' not found in list of events with the status 'TaskStateExited'"),
            ("no Napper lambda Output Exists", "Mapper Lambda Task exited but produced no output.")
        ]

        for case_no, (state, exception_msg) in enumerate(cases):
            # subTest keeps the loop running even if an assertion fails
            with self.subTest(msg=state, expected="Exception: " + exception_msg):
                mock_sfn = MagicMock()
                state_machine_mock = MagicMock()
                state_machine_mock.paginate.return_value = [
                    {"stateMachines": [{"name": "test-dr2-something", "stateMachineArn": "arn:some_arn"}]}
                ]
                get_execution_history_mock = MagicMock()
                executions_list = [
                    {"name": "TDR_job1",
                     "executionArn": "arn:aws:states:region:123456789012:execution:test-dr2:TDR_job1",
                     "stateMachineArn": "arn:aws:states:region:123456789012:stateMachine:test-dr2"}
                ]

                events = [
                    [
                        {
                            "events": [
                                {
                                    "eventId": 1,
                                    "type": "ExecutionStarted"
                                },
                                {
                                    "eventId": 2,
                                    "type": "TaskStateExited",
                                    "stateExitedEventDetails": {
                                        "name": "Get metadata and update Files table",
                                        "output": f"""{{"totalAssetCount":1,"totalFileBytes":1000}}"""
                                    }
                                }
                            ]
                        }
                    ]
                ]
                events_list = events[0][0]["events"]
                match case_no:
                    case 0:
                        events_list.pop()
                    case 1:
                        events_list[1]["stateExitedEventDetails"]["name"] = "Not Mapper name"
                    case 2:
                        del events_list[-1]["stateExitedEventDetails"]["output"]
                get_execution_history_mock.paginate.side_effect = events
                arg_to_mock = {"list_state_machines": state_machine_mock,
                               "get_execution_history": get_execution_history_mock}
                mock_sfn.get_paginator.side_effect = lambda arg: arg_to_mock[arg]

                mock_sfn.list_executions.return_value = {"executions": executions_list}
                mock_boto_client.return_value = mock_sfn

                with self.assertRaises(Exception) as e:
                    ingest_metric_collector.get_stepfunction_metrics("test-dr2", SOURCE_SYSTEMS,
                                                                     MAPPER_LAMBDA_STATE_NAME)
                self.assertEqual(exception_msg, str(e.exception))

    @patch("ingest_metric_collector.boto3.client")
    def test_get_flow_control_metrics_should_return_zero_when_no_items_in_queue(self, mock_boto_client):
        mock_dynamo = MagicMock()
        mock_dynamo.query.return_value = {"Items": []}
        mock_boto_client.return_value = mock_dynamo

        metrics = ingest_metric_collector.get_flow_control_metrics("test-dr2", SOURCE_SYSTEMS)

        self.assertEqual(10, len(metrics))

        for n, ss in enumerate(self.expected_source_systems):
            ingest_queued_metric = generate_metrics(metric_name="IngestsQueued", source_system=ss)
            queue_age_metric = generate_metrics(metric_name="ApproximateAgeOfOldestQueuedIngest", source_system=ss,
                                                unit="Seconds")
            queue_asset_count_metric = generate_metrics(metric_name="QueuedAssetCount",
                                                        source_system=ss, unit="Count")
            expected_bytes = 1000 if ss == "TDR" else 0
            queue_bytes_metric = generate_metrics(value=expected_bytes, metric_name="QueuedBytes", source_system=ss,
                                                  unit="Bytes")
            ingest_queued_metric["Dimensions"] = ingest_queued_metric["Dimensions"][2:]
            queue_age_metric["Dimensions"] = queue_age_metric["Dimensions"][2:]
            queue_asset_count_metric["Dimensions"] = queue_asset_count_metric["Dimensions"][2:]
            queue_bytes_metric["Dimensions"] = queue_bytes_metric["Dimensions"][2:]

    def make_source_system_specific_mock(self, mock_mapping):
        def query_side_effect(**kwargs):
            ss_value = kwargs["ExpressionAttributeValues"][":ssPlaceHolder"]["S"]
            return {"Items": mock_mapping.get(ss_value, [])}

        return query_side_effect

    @patch("ingest_metric_collector.boto3.client")
    def test_get_flow_control_metrics_should_return_executions_and_age_when_there_are_items_in_queue(self,
                                                                                                     mock_boto_client):
        now = datetime.now(timezone.utc)

        mock_mapping = {
            "TDR": [
                {
                    "sourceSystem": {"S": "CRM"},
                    "queuedAt": {"S": (now - timedelta(seconds=60)).isoformat()},
                    "queuedAssetCount": 1,
                    "queuedBytes": 1000
                }
            ],
            "COURTDOC": [],
            "DEFAULT": [],
        }

        mock_dynamo = MagicMock()
        mock_dynamo.query.side_effect = self.make_source_system_specific_mock(mock_mapping)
        mock_boto_client.return_value = mock_dynamo

        metrics = ingest_metric_collector.get_flow_control_metrics("test-dr2", SOURCE_SYSTEMS)
        self.assertEqual(12, len(metrics))

        for ss, (count, seconds) in zip(self.expected_source_systems, ((1, 60), (0, 0), (0, 0), (0, 0), (0, 0))):
            ingest_queued_metric = generate_metrics(value=count, metric_name="IngestsQueued", source_system=ss)
            queue_age_metric = generate_metrics(value=seconds, metric_name="ApproximateAgeOfOldestQueuedIngest",
                                                source_system=ss, unit="Seconds")
            expected_asset_count, expected_bytes = (1, 1000) if ss == "TDR" else (0, 0)

            queue_bytes_metric = generate_metrics(value=expected_bytes, metric_name="QueuedBytes", source_system=ss,
                                                  unit="Bytes")
            ingest_queued_metric["Dimensions"] = ingest_queued_metric["Dimensions"]
            queue_age_metric["Dimensions"] = queue_age_metric["Dimensions"]

            self.assertEqual(ingest_queued_metric, metrics.pop(0))
            age_metric = metrics.pop(0)
            age_metric["Value"] = round(age_metric["Value"], 2)
            self.assertEqual(queue_age_metric, age_metric)

            if ss == "TDR":
                queue_asset_count_metric = generate_metrics(value=expected_asset_count, metric_name="QueuedAssetCount",
                                                            source_system=ss, unit="Count")
                queue_asset_count_metric["Dimensions"] = queue_asset_count_metric["Dimensions"]
                queue_bytes_metric["Dimensions"] = queue_bytes_metric["Dimensions"]
                self.assertEqual(queue_asset_count_metric, metrics.pop(0))
                self.assertEqual(queue_bytes_metric, metrics.pop(0))

    @patch("ingest_metric_collector.boto3.client")
    @patch("ingest_metric_collector.get_stepfunction_metrics", side_effect=Exception("sfn error"))
    @patch("ingest_metric_collector.get_flow_control_metrics",
           return_value=[{"MetricName": "ApproximateAgeOfOldestQueuedIngest", "Unit": "seconds", "Value": 0}])
    def test_lambda_handler_should_return_valid_metrics_when_get_stepfunction_metrics_fails_but_get_flow_control_metrics_succeed(
            self, mock_flow_control, mock_sfn, mock_boto_client):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        ingest_metric_collector.lambda_handler({}, DummyContext())

        self.assertEqual(
            ("intg-dr2-ingest", ("TDR", "COURTDOC", "ADHOC", "DRI", "DEFAULT"), '"Get metadata and update Files table"'),
            mock_sfn.call_args.args
        )
        self.assertEqual(("intg-dr2-ingest", ("TDR", "COURTDOC", "ADHOC", "DRI", "DEFAULT")), mock_flow_control.call_args.args)

        mock_client.put_metric_data.assert_called_once_with(
            Namespace="intg-dr2-ingest",
            MetricData=[{"MetricName": "ApproximateAgeOfOldestQueuedIngest", "Unit": "seconds", "Value": 0}]
        )

    @patch("ingest_metric_collector.boto3.client")
    @patch("ingest_metric_collector.get_flow_control_metrics", side_effect=Exception("flow control metrics error"))
    @patch("ingest_metric_collector.get_stepfunction_metrics",
           return_value=[{"MetricName": "ExecutionsRunning", "Value": 1}])
    def test_lambda_handler_should_return_valid_metrics_when_get_stepfunction_metrics_succeed_but_get_flow_control_metrics_fails(
            self, mock_sfn, mock_flow_control, mock_boto_client):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        ingest_metric_collector.lambda_handler({}, DummyContext())

        self.assertEqual(
            ("intg-dr2-ingest", ("TDR", "COURTDOC", "ADHOC", "DRI", "DEFAULT"), '"Get metadata and update Files table"'),
            mock_sfn.call_args.args
        )
        self.assertEqual(("intg-dr2-ingest", ("TDR", "COURTDOC", "ADHOC", "DRI", "DEFAULT")), mock_flow_control.call_args.args)

        mock_client.put_metric_data.assert_called_once_with(
            Namespace="intg-dr2-ingest",
            MetricData=[{"MetricName": "ExecutionsRunning", "Value": 1}]
        )

    @patch("ingest_metric_collector.boto3.client")
    @patch("ingest_metric_collector.get_stepfunction_metrics", side_effect=Exception("step function exception"))
    @patch("ingest_metric_collector.get_flow_control_metrics", side_effect=Exception("flow control exception"))
    def test_lambda_handler_should_throw_exception_when_get_stepfunction_metrics_as_well_as_get_flow_control_metrics_fails(
            self, mock_flow_control, mock_sfn, mock_boto_client):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        with self.assertRaises(Exception) as context:
            ingest_metric_collector.lambda_handler({}, DummyContext())
        self.assertIn("Failed to collect metrics for step function as well as the queued executions",
                      str(context.exception))

        self.assertEqual(
            ("intg-dr2-ingest", ("TDR", "COURTDOC", "ADHOC", "DRI", "DEFAULT"), '"Get metadata and update Files table"'),
            mock_sfn.call_args.args
        )
        self.assertEqual(("intg-dr2-ingest", ("TDR", "COURTDOC", "ADHOC", "DRI", "DEFAULT")), mock_flow_control.call_args.args)
        mock_client.put_metric_data.assert_not_called()

    @patch("ingest_metric_collector.boto3.client")
    @patch("ingest_metric_collector.get_stepfunction_metrics",
           return_value=[{"MetricName": "ExecutionsRunning", "Value": 1}])
    @patch("ingest_metric_collector.get_flow_control_metrics",
           return_value=[{"MetricName": "ApproximateAgeOfOldestQueuedIngest", "Unit": "seconds", "Value": 0}])
    def test_lambda_handler_should_throw_exception_when_put_metric_to_cloudwatch_fails(self, mock_flow_control,
                                                                                       mock_sfn, mock_boto_client):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        mock_client.put_metric_data.side_effect = Exception("dummy reason should be embedded in message")

        with self.assertRaises(Exception) as context:
            ingest_metric_collector.lambda_handler({}, DummyContext())

        self.assertEqual(
            ("intg-dr2-ingest", ("TDR", "COURTDOC", "ADHOC", "DRI", "DEFAULT"), '"Get metadata and update Files table"'),
            mock_sfn.call_args.args
        )
        self.assertEqual(("intg-dr2-ingest", ("TDR", "COURTDOC", "ADHOC", "DRI", "DEFAULT")),
                         mock_flow_control.call_args.args)
        self.assertIn(
            "Failed to send metrics to CloudWatch due to underlying exception: 'dummy reason should be embedded in message'",
            str(context.exception)
        )


class DummyContext:
    def __init__(self, function_name="intg-some-lambda-function-name"):
        self.function_name = function_name


if __name__ == '__main__':
    unittest.main()
