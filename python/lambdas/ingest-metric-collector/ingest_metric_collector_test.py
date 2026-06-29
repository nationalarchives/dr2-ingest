import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import ingest_metric_collector

def generate_metrics(state_machine_arn="arn:some_arn", state_machine_name= "test-dr2-something", value=0,
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
    if source_system:
        metrics["Dimensions"].append({"Name": "SourceSystem", "Value": source_system})

    return metrics


class TestLambdaFunction(unittest.TestCase):
    expected_source_systems = ("TDR", "COURTDOC", "ADHOC", "DRI", "PA", "DEFAULT")

    @patch("ingest_metric_collector.boto3.client")
    def test_get_stepfunction_metrics_should_return_empty_metrics_when_there_are_no_state_machines(self, mock_boto_client):
        mock_sfn = MagicMock()
        mock_sfn.get_paginator.return_value.paginate.return_value = [{"stateMachines": []}]
        mock_boto_client.return_value = mock_sfn

        metrics = ingest_metric_collector.get_stepfunction_metrics("env-prefix")
        self.assertEqual([], metrics)

    @patch("ingest_metric_collector.boto3.client")
    def test_get_stepfunction_metrics_should_return_single_metric_when_no_executions_and_state_machine_not_from_known_source_systems(self, mock_boto_client):
        mock_sfn = MagicMock()
        mock_sfn.get_paginator.return_value.paginate.return_value = [
            {"stateMachines": [{"name": "unknown-ss-something", "stateMachineArn": "arn:some_arn"}]}
        ]
        mock_sfn.list_executions.return_value = {"executions": []}
        mock_boto_client.return_value = mock_sfn

        metrics = ingest_metric_collector.get_stepfunction_metrics("TDR-")
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

        metrics = ingest_metric_collector.get_stepfunction_metrics("test-dr2-")

        self.assertEqual(7, len(metrics))
        expected_metric = generate_metrics()
        self.assertEqual(expected_metric, metrics[0])

        for n, ss in enumerate(self.expected_source_systems):
            expected_metric = generate_metrics(source_system=ss)
            self.assertEqual(expected_metric, metrics[n + 1])


    @patch("ingest_metric_collector.boto3.client")
    def test_get_stepfunction_metrics_should_return_metrics_when_executions_and_source_system_exist(self, mock_boto_client):
        mock_sfn = MagicMock()
        mock_sfn.get_paginator.return_value.paginate.return_value = [
            {"stateMachines": [{"name": "test-dr2-something", "stateMachineArn": "arn:some_arn"}]}
        ]
        mock_sfn.list_executions.return_value = {
            "executions": [
                {"name": "TDR_job1"},
                {"name": "COURTDOC_task1"},
                {"name": "RANDOM_job2"},  # unknown ss should get added to DEFAULT
            ]
        }
        mock_boto_client.return_value = mock_sfn

        metrics = ingest_metric_collector.get_stepfunction_metrics("test-dr2")

        # 1 metric for total executions + metrics per source system
        self.assertEqual(1 + len(self.expected_source_systems), len(metrics))

        for n, (ss, executions) in enumerate(zip(self.expected_source_systems, (1, 1, 0, 0, 0, 1))):
            expected_metric = generate_metrics(value=executions, source_system=ss)
            self.assertEqual(expected_metric, metrics[n + 1])

    @patch("ingest_metric_collector.boto3.client")
    def test_get_flow_control_metrics_should_return_zero_when_no_items_in_queue(self, mock_boto_client):
        mock_dynamo = MagicMock()
        mock_dynamo.query.return_value = {"Items": []}
        mock_boto_client.return_value = mock_dynamo

        metrics = ingest_metric_collector.get_flow_control_metrics("test-dr2")

        self.assertEqual(12, len(metrics))

        for n, ss in enumerate(self.expected_source_systems):
            ingest_queued_metric = generate_metrics(metric_name="IngestsQueued", source_system=ss)
            queue_age_metric = generate_metrics(metric_name="ApproximateAgeOfOldestQueuedIngest", source_system=ss,
                                                unit="Seconds")
            ingest_queued_metric["Dimensions"] = ingest_queued_metric["Dimensions"][2:]
            queue_age_metric["Dimensions"] = queue_age_metric["Dimensions"][2:]

            self.assertEqual(ingest_queued_metric, metrics.pop(0))
            self.assertEqual(queue_age_metric, metrics.pop(0))

    def make_source_system_specific_mock(self, mock_mapping):
        def query_side_effect(**kwargs):
            ss_value = kwargs["ExpressionAttributeValues"][":ssPlaceHolder"]["S"]
            return {"Items": mock_mapping.get(ss_value, [])}

        return query_side_effect

    @patch("ingest_metric_collector.boto3.client")
    def test_get_flow_control_metrics_should_return_executions_and_age_when_there_are_items_in_queue(self, mock_boto_client):
        now = datetime.now(timezone.utc)

        mock_mapping = {
            "TDR": [
                {
                    "sourceSystem": {"S": "CRM"},
                    "queuedAt": {"S": (now - timedelta(seconds=60)).isoformat()},
                }
            ],
            "COURTDOC": [],
            "DEFAULT": [],
        }

        mock_dynamo = MagicMock()
        mock_dynamo.query.side_effect = self.make_source_system_specific_mock(mock_mapping)
        mock_boto_client.return_value = mock_dynamo

        metrics = ingest_metric_collector.get_flow_control_metrics("test-dr2")
        self.assertEqual(12, len(metrics))

        for ss, (count, seconds) in zip(self.expected_source_systems, ((1, 60), (0, 0), (0, 0), (0, 0), (0, 0))):
            ingest_queued_metric = generate_metrics(value=count, metric_name="IngestsQueued", source_system=ss)
            queue_age_metric = generate_metrics(value=seconds, metric_name="ApproximateAgeOfOldestQueuedIngest",
                                                source_system=ss, unit="Seconds")
            ingest_queued_metric["Dimensions"] = ingest_queued_metric["Dimensions"][2:]
            queue_age_metric["Dimensions"] = queue_age_metric["Dimensions"][2:]

            self.assertEqual(ingest_queued_metric, metrics.pop(0))
            age_metric = metrics.pop(0)
            age_metric["Value"] = round(age_metric["Value"], 2)
            self.assertEqual(queue_age_metric, age_metric)

    @patch("ingest_metric_collector.boto3.client")
    @patch("ingest_metric_collector.get_stepfunction_metrics", side_effect=Exception("sfn error"))
    @patch("ingest_metric_collector.get_flow_control_metrics", return_value=[{"MetricName": "ApproximateAgeOfOldestQueuedIngest", "Unit": "seconds", "Value": 0}])
    def test_lambda_handler_should_return_valid_metrics_when_get_stepfunction_metrics_fails_but_get_flow_control_metrics_succeed(self, mock_flow_control, mock_sfn, mock_boto_client):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        ingest_metric_collector.lambda_handler({}, DummyContext())

        mock_client.put_metric_data.assert_called_once_with(
            Namespace="intg-dr2-ingest",
            MetricData=[{"MetricName": "ApproximateAgeOfOldestQueuedIngest", "Unit": "seconds", "Value": 0}]
        )

    @patch("ingest_metric_collector.boto3.client")
    @patch("ingest_metric_collector.get_flow_control_metrics", side_effect=Exception("flow control metrics error"))
    @patch("ingest_metric_collector.get_stepfunction_metrics", return_value=[{"MetricName": "ExecutionsRunning", "Value": 1}])
    def test_lambda_handler_should_return_valid_metrics_when_get_stepfunction_metrics_succeed_but_get_flow_control_metrics_fails(self, mock_sfn, mock_flow_control, mock_boto_client):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        ingest_metric_collector.lambda_handler({}, DummyContext())

        mock_client.put_metric_data.assert_called_once_with(
            Namespace="intg-dr2-ingest",
            MetricData=[{"MetricName": "ExecutionsRunning", "Value": 1}]
        )

    @patch("ingest_metric_collector.boto3.client")
    @patch("ingest_metric_collector.get_stepfunction_metrics", side_effect=Exception("step function exception"))
    @patch("ingest_metric_collector.get_flow_control_metrics", side_effect=Exception("flow control exception"))
    def test_lambda_handler_should_throw_exception_when_get_stepfunction_metrics_as_well_as_get_flow_control_metrics_fails(self, mock_flow_control, mock_sfn, mock_boto_client):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        with self.assertRaises(Exception) as context:
            ingest_metric_collector.lambda_handler({}, DummyContext())
        self.assertIn("Failed to collect metrics for step function as well as age", str(context.exception))

        mock_client.put_metric_data.assert_not_called()

    @patch("ingest_metric_collector.boto3.client")
    @patch("ingest_metric_collector.get_stepfunction_metrics",  return_value=[{"MetricName": "ExecutionsRunning", "Value": 1}])
    @patch("ingest_metric_collector.get_flow_control_metrics", return_value=[{"MetricName": "ApproximateAgeOfOldestQueuedIngest", "Unit": "seconds", "Value": 0}])
    def test_lambda_handler_should_throw_exception_when_put_metric_to_cloudwatch_fails(self, mock_flow_control, mock_sfn, mock_boto_client):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        mock_client.put_metric_data.side_effect = Exception("dummy reason should be embedded in message")

        with self.assertRaises(Exception) as context:
            ingest_metric_collector.lambda_handler({}, DummyContext())
        self.assertIn("Failed to send metrics to CloudWatch due to underlying exception: 'dummy reason should be embedded in message'", str(context.exception))



class DummyContext:
    def __init__(self, function_name="intg-some-lambda-function-name"):
        self.function_name = function_name

if __name__ == '__main__':
    unittest.main()
