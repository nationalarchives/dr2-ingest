import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock
import ingest_metric_collector

class TestLambdaFunction(unittest.TestCase):

    @patch("ingest_metric_collector.boto3.client")
    def test_should_return_empty_metrics_when_there_are_no_state_machines(self, mock_boto_client):
        mock_sfn = MagicMock()
        mock_sfn.get_paginator.return_value.paginate.return_value = [{"stateMachines": []}]
        mock_boto_client.return_value = mock_sfn

        metrics = ingest_metric_collector.get_stepfunction_metrics("env-prefix")
        self.assertEqual(metrics, [])

    @patch("ingest_metric_collector.boto3.client")
    def test_should_return_single_metric_when_no_executions_and_state_machine_not_from_known_source_systems(self, mock_boto_client):
        mock_sfn = MagicMock()
        mock_sfn.get_paginator.return_value.paginate.return_value = [
            {"stateMachines": [{"name": "unknown-prefix-something", "stateMachineArn": "arn:some_arn"}]}
        ]
        mock_sfn.list_executions.return_value = {"executions": []}
        mock_boto_client.return_value = mock_sfn

        metrics = ingest_metric_collector.get_stepfunction_metrics("TDR-")
        # Should return one metric with 0 executions
        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0]["Value"], 0)


    @patch("ingest_metric_collector.boto3.client")
    def test_should_return_metric_per_source_system_when_source_system_is_known(self,
                                                                                                            mock_boto_client):
        mock_sfn = MagicMock()
        mock_sfn.get_paginator.return_value.paginate.return_value = [
            {"stateMachines": [{"name": "test-dr2-something", "stateMachineArn": "arn:some_arn"}]}
        ]
        mock_sfn.list_executions.return_value = {"executions": []}
        mock_boto_client.return_value = mock_sfn

        metrics = ingest_metric_collector.get_stepfunction_metrics("test-dr2-")
        # Should return one metric with 0 executions
        self.assertEqual(len(metrics), 4) #1 for running executions and 3 for each source system
        self.assertEqual(metrics[0]["Value"], 0)


    @patch("ingest_metric_collector.boto3.client")
    def test_should_return_metrics_when_executions_and_source_system_exist(self, mock_boto_client):
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
        metric_names = [m["Dimensions"] for m in metrics]
        self.assertEqual(len(metrics), 1 + len(ingest_metric_collector.SOURCE_SYSTEMS))  # total + sources + DEFAULT

        # Check that TDR, COURTDOC and DEFAULT, all got Value as 1
        tdr_metric = next(
            m for m in metrics
            if {"Name": "SourceSystem", "Value": "TDR"} in m["Dimensions"]
        )
        self.assertEqual(tdr_metric["Value"], 1)

        courtdoc_metric = next(
            m for m in metrics
            if {"Name": "SourceSystem", "Value": "COURTDOC"} in m["Dimensions"]
        )
        self.assertEqual(courtdoc_metric["Value"], 1)


        default_metric = next(m for m in metrics if any(
            d.get("Value") == "DEFAULT" for d in m["Dimensions"]
        ))
        self.assertEqual(default_metric["Value"], 1)

    @patch("ingest_metric_collector.boto3.client")
    def test_should_return_zero_when_no_items_in_queue(self, mock_boto_client):
        mock_dynamo = MagicMock()
        mock_dynamo.query.return_value = {"Items": []}
        mock_boto_client.return_value = mock_dynamo

        metrics = ingest_metric_collector.get_flow_control_metrics("test-dr2")

        self.assertEqual(6, len(metrics)) # expect 6 as we have 3 source systems with 2 entries for each
        for m in metrics:
            self.assertEqual(0, m["Value"])

    def make_source_system_specific_mock(self, mock_mapping):
        def query_side_effect(**kwargs):
            ss_value = kwargs["ExpressionAttributeValues"][":ssPlaceHolder"]["S"]
            return {"Items": mock_mapping.get(ss_value, [])}

        return query_side_effect

    @patch("ingest_metric_collector.boto3.client")
    def test_with_items_in_queue(self, mock_boto_client):
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
        self.assertEqual(6, len(metrics))

        # IngestsQueued should be 1
        queued_metric = [m for m in metrics if m["MetricName"] == "IngestsQueued"]
        for m in queued_metric:
            ss = next(d["Value"] for d in m["Dimensions"] if d["Name"] == "SourceSystem")
            if ss == "TDR":
                self.assertEqual(1, m["Value"])
            elif ss == "COURTDOC":
                self.assertEqual(0, m["Value"])
            elif ss == "DEFAULT":
                self.assertEqual(0, m["Value"])
            else:
                self.fail(f"This should never happen: Unexpected source system {ss}")


        age_metric = [m for m in metrics if m["MetricName"] == "ApproximateAgeOfOldestQueuedIngest"]
        for m in age_metric:
            ss = next(d["Value"] for d in m["Dimensions"] if d["Name"] == "SourceSystem")
            if ss == "TDR":
                self.assertAlmostEqual(60, m["Value"], delta=2)
            elif ss == "COURTDOC":
                self.assertEqual(0, m["Value"])
            elif ss == "DEFAULT":
                self.assertEqual(0, m["Value"])
            else:
                self.fail(f"This should never happen: Unexpected source system {ss}")


if __name__ == '__main__':
    unittest.main()
