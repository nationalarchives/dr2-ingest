import unittest
from datetime import datetime
from unittest.mock import Mock, patch

from pytz import UTC

from sfn_timings import lambda_function


@patch.dict("os.environ", {"WORKFLOW_SFN_ARN": "workflow-arn"})
@patch.dict("os.environ", {"INGEST_SFN_ARN": "ingest-arn"})
@patch.dict("os.environ", {"PREINGEST_SFN_ARN": "preingest-arn"})
@patch.dict("os.environ", {"OUTPUT_BUCKET_NAME": "bucket-name"})
@patch.dict("os.environ", {"AWS_DEFAULT_REGION": "eu-west-2"})
class TestIngestTimingsHandler(unittest.TestCase):
    workflow_executions = [
        {
            "executionArn": "workflowArn",
            "name": "ingestExecutionName-nameOfWorkflowExecution",
            "stateMachineArn": "workflowStateMachineArn",
            "status": "SUCCEEDED",
            "startDate": UTC.localize(datetime(2025, 1, 2)),
            "stopDate": UTC.localize(datetime(2025, 1, 2))
        },
        {
            "executionArn": "workflowArn2",
            "name": "ingestExecutionName2-nameOfWorkflowExecution2",
            "stateMachineArn": "workflowStateMachineArn2",
            "status": "SUCCEEDED",
            "startDate": UTC.localize(datetime(2025, 1, 1)),
            "stopDate": UTC.localize(datetime(2025, 1, 1))
        }
    ]

    ingest_executions_responses = [
        {
            "executions": [
                {
                    "executionArn": "ingestArn",
                    "stateMachineArn": "ingestArnStateMachineArn",
                    "name": "ingestExecutionName",
                    "status": "SUCCEEDED",
                    "startDate": UTC.localize(datetime(2025, 1, 2)),
                    "stopDate": UTC.localize(datetime(2025, 1, 2))
                }
            ],
            "nextToken": "next_token"
        },
        {
            "executions": [
                {
                    "executionArn": "ingestArn2",
                    "stateMachineArn": "ingestArnStateMachineArn2",
                    "name": "ingestExecutionName2",
                    "status": "SUCCEEDED",
                    "startDate": UTC.localize(datetime(2025, 1, 1)),
                    "stopDate": UTC.localize(datetime(2025, 1, 1))
                }
            ]
        }
    ]

    def test_next_token_should_return_the_next_token_from_the_executions_response(self):
        executions_info = {"executions": [], "nextToken": "next_page_token"}
        next_token = lambda_function.next_token(executions_info)

        self.assertEqual({"nextToken": "next_page_token"}, next_token)

    def test_next_token_should_return_an_empty_map_if_there_is_no_next_token_in_the_executions_response(self):
        executions_info = {"executions": []}
        next_token = lambda_function.next_token(executions_info)

        self.assertEqual({}, next_token)

    def test_get_workflow_executions_should_call_list_executions_once_and_return_executions_if_there_is_no_next_token(
        self):
        cut_off_date = UTC.localize(datetime(2024, 12, 21))
        sfn_client = Mock()
        response = {"executions": [self.workflow_executions[0]]}

        sfn_client.list_executions = Mock(return_value=response)

        executions = lambda_function.get_workflow_executions(sfn_client, cut_off_date, "workflow-arn",
                                                             relevant_executions=[], next_page_token={})

        self.assertEqual(1, sfn_client.list_executions.call_count)
        self.assertEqual({"maxResults": 100, "stateMachineArn": "workflow-arn", "statusFilter": "SUCCEEDED"},
                         sfn_client.list_executions.call_args_list[0][1])

        self.assertEqual(
            [
                {
                    "executionArn": "workflowArn",
                    "name": "ingestExecutionName-nameOfWorkflowExecution",
                    "stateMachineArn": "workflowStateMachineArn",
                    "status": "SUCCEEDED",
                    "startDate": UTC.localize(datetime(2025, 1, 2)),
                    "stopDate": UTC.localize(datetime(2025, 1, 2))
                }
            ],
            executions
        )

    def test_get_workflow_executions_should_call_list_executions_once_and_return_executions_if_there_is_a_next_token(
        self):
        cut_off_date = UTC.localize(datetime(2024, 12, 21))
        sfn_client = Mock()
        response1 = {
            "executions": [self.workflow_executions[0]],
            "nextToken": "next_token"
        }
        response2 = {"executions": [self.workflow_executions[1]]}

        sfn_client.list_executions = Mock()
        sfn_client.list_executions.side_effect = [response1, response2]

        executions = lambda_function.get_workflow_executions(sfn_client, cut_off_date, "workflow-arn",
                                                             relevant_executions=[], next_page_token={})

        self.assertEqual(2, sfn_client.list_executions.call_count)
        self.assertEqual({"maxResults": 100, "stateMachineArn": "workflow-arn", "statusFilter": "SUCCEEDED"},
                         sfn_client.list_executions.call_args_list[0][1])
        self.assertEqual({"maxResults": 100, "stateMachineArn": "workflow-arn", "statusFilter": "SUCCEEDED",
                          "nextToken": "next_token"},
                         sfn_client.list_executions.call_args_list[1][1])
        self.assertEqual(
            [
                {
                    "executionArn": "workflowArn",
                    "name": "ingestExecutionName-nameOfWorkflowExecution",
                    "stateMachineArn": "workflowStateMachineArn",
                    "status": "SUCCEEDED",
                    "startDate": UTC.localize(datetime(2025, 1, 2)),
                    "stopDate": UTC.localize(datetime(2025, 1, 2))
                },
                {
                    "executionArn": "workflowArn2",
                    "name": "ingestExecutionName2-nameOfWorkflowExecution2",
                    "stateMachineArn": "workflowStateMachineArn2",
                    "status": "SUCCEEDED",
                    "startDate": UTC.localize(datetime(2025, 1, 1)),
                    "stopDate": UTC.localize(datetime(2025, 1, 1))
                }
            ],
            executions
        )

    def test_get_workflow_executions_should_stop_processing_executions_if_they_are_before_the_cut_off(
        self):
        cut_off_date = UTC.localize(datetime(2025, 1, 2))
        sfn_client = Mock()
        response1 = {
            "executions": [self.workflow_executions[0]],
            "nextToken": "next_token"
        }
        response2 = {"executions": [self.workflow_executions[1]]}

        sfn_client.list_executions = Mock()
        sfn_client.list_executions.side_effect = [response1, response2]

        executions = lambda_function.get_workflow_executions(sfn_client, cut_off_date, "workflow-arn",
                                                             relevant_executions=[], next_page_token={})

        self.assertEqual(2, sfn_client.list_executions.call_count)
        self.assertEqual({"maxResults": 100, "stateMachineArn": "workflow-arn", "statusFilter": "SUCCEEDED"},
                         sfn_client.list_executions.call_args_list[0][1])
        self.assertEqual({"maxResults": 100, "stateMachineArn": "workflow-arn", "statusFilter": "SUCCEEDED",
                          "nextToken": "next_token"}, sfn_client.list_executions.call_args_list[1][1])
        self.assertEqual(
            [
                {
                    "executionArn": "workflowArn",
                    "name": "ingestExecutionName-nameOfWorkflowExecution",
                    "stateMachineArn": "workflowStateMachineArn",
                    "status": "SUCCEEDED",
                    "startDate": UTC.localize(datetime(2025, 1, 2)),
                    "stopDate": UTC.localize(datetime(2025, 1, 2))
                }
            ],
            executions
        )

    def test_get_ingest_timings_should_call_list_executions_once_and_return_executions_if_there_is_no_next_token(self):
        cut_off_date = UTC.localize(datetime(2024, 12, 21))
        sfn_client = Mock()
        response = {"executions": self.ingest_executions_responses[0]["executions"]}

        sfn_client.list_executions = Mock(return_value=response)

        executions = lambda_function.get_ingest_timings(sfn_client, cut_off_date, self.workflow_executions,
                                                        "ingest-arn", "preingest-arn")

        self.assertEqual(1, sfn_client.list_executions.call_count)
        self.assertEqual(0, sfn_client.describe_execution.call_count)
        self.assertEqual({"maxResults": 100, "stateMachineArn": "ingest-arn", "statusFilter": "SUCCEEDED"},
                         sfn_client.list_executions.call_args_list[0][1])

        self.assertEqual(
            [
                {
                    "name": "ingestExecutionName",
                    "steps": {
                        "preservicaIngest": {
                            "endDate": 1735776000.0,
                            "name": "ingestExecutionName",
                            "startDate": 1735776000.0
                        },
                        "preservicaWorkflow": {
                            "endDate": 1735776000.0,
                            "name": "ingestExecutionName-nameOfWorkflowExecution",
                            "startDate": 1735776000.0
                        }
                    }
                }
            ],
            list(executions)
        )

    def test_get_ingest_timings_should_call_list_executions_once_and_return_executions_if_there_is_a_next_token(self):
        cut_off_date = UTC.localize(datetime(2024, 12, 21))
        sfn_client = Mock()
        lambda_function.sfn_client = lambda: sfn_client

        sfn_client.list_executions = Mock()
        sfn_client.list_executions.side_effect = self.ingest_executions_responses

        executions = lambda_function.get_ingest_timings(sfn_client, cut_off_date, self.workflow_executions,
                                                        "ingest-arn", "preingest-arn")

        self.assertEqual(2, sfn_client.list_executions.call_count)
        self.assertEqual(0, sfn_client.describe_execution.call_count)
        self.assertEqual({"maxResults": 100, "stateMachineArn": "ingest-arn", "statusFilter": "SUCCEEDED"},
                         sfn_client.list_executions.call_args_list[0][1])
        self.assertEqual({"maxResults": 100, "stateMachineArn": "ingest-arn", "statusFilter": "SUCCEEDED",
                          "nextToken": "next_token"}, sfn_client.list_executions.call_args_list[1][1])
        self.assertEqual(
            [
                {
                    "name": "ingestExecutionName",
                    "steps": {
                        "preservicaIngest": {
                            "endDate": 1735776000.0,
                            "name": "ingestExecutionName",
                            "startDate": 1735776000.0
                        },
                        "preservicaWorkflow": {
                            "endDate": 1735776000.0,
                            "name": "ingestExecutionName-nameOfWorkflowExecution",
                            "startDate": 1735776000.0
                        }
                    }
                },
                {
                    "name": "ingestExecutionName2",
                    "steps": {
                        "preservicaIngest": {
                            "endDate": 1735689600.0,
                            "name": "ingestExecutionName2",
                            "startDate": 1735689600.0},
                        "preservicaWorkflow": {
                            "endDate": 1735689600.0,
                            "name": "ingestExecutionName2-nameOfWorkflowExecution2",
                            "startDate": 1735689600.0
                        }
                    }
                }
            ],
            list(executions)
        )

    def test_get_ingest_timings_should_stop_processing_executions_if_they_are_before_the_cut_off(self):
        cut_off_date = UTC.localize(datetime(2025, 1, 2))
        sfn_client = Mock()

        sfn_client.list_executions = Mock()
        sfn_client.list_executions.side_effect = self.ingest_executions_responses

        executions = lambda_function.get_ingest_timings(sfn_client, cut_off_date, self.workflow_executions,
                                                        "ingest-arn", "preingest-arn")

        self.assertEqual(2, sfn_client.list_executions.call_count)
        self.assertEqual(0, sfn_client.describe_execution.call_count)
        self.assertEqual({"maxResults": 100, "stateMachineArn": "ingest-arn", "statusFilter": "SUCCEEDED"},
                         sfn_client.list_executions.call_args_list[0][1])
        self.assertEqual({"maxResults": 100, "stateMachineArn": "ingest-arn", "statusFilter": "SUCCEEDED",
                          "nextToken": "next_token"},
                         sfn_client.list_executions.call_args_list[1][1])
        self.assertEqual(
            [
                {
                    "name": "ingestExecutionName",
                    "steps": {
                        "preservicaIngest": {
                            "endDate": 1735776000.0,
                            "name": "ingestExecutionName",
                            "startDate": 1735776000.0
                        },
                        "preservicaWorkflow": {
                            "endDate": 1735776000.0,
                            "name": "ingestExecutionName-nameOfWorkflowExecution",
                            "startDate": 1735776000.0
                        }
                    }
                }
            ],
            list(executions)
        )

    def test_get_ingest_timings_should_call_describe_execution_once_and_return_preingest_info_if_the_name_of_the_execution_starts_with_tdr(
        self):
        cut_off_date = UTC.localize(datetime(2024, 12, 21))
        sfn_client = Mock()
        lambda_function.sfn_client = lambda: sfn_client
        sfn_execs_response = {
            "executions": [
                {
                    "executionArn": "ingestArn",
                    "stateMachineArn": "ingestArnStateMachineArn",
                    "name": "TDR_ingestExecutionName",
                    "status": "SUCCEEDED",
                    "startDate": UTC.localize(datetime(2025, 1, 2)),
                    "stopDate": UTC.localize(datetime(2025, 1, 2))
                }
            ]
        }

        preingest_sfn_response = {
            "executionArn": "preingestArn",
            "stateMachineArn": "preingestArnStateMachineArn",
            "name": "preingestExecutionName",
            "status": "SUCCEEDED",
            "startDate": UTC.localize(datetime(2025, 1, 2)),
            "stopDate": UTC.localize(datetime(2025, 1, 2))
        }

        sfn_client.list_executions = Mock(return_value=sfn_execs_response)
        sfn_client.describe_execution = Mock(return_value=preingest_sfn_response)

        workflow_executions = [{
            "executionArn": "workflowArn",
            "name": "TDR_ingestExecutionName-nameOfWorkflowExecution",
            "stateMachineArn": "workflowStateMachineArn",
            "status": "SUCCEEDED",
            "startDate": UTC.localize(datetime(2025, 1, 2)),
            "stopDate": UTC.localize(datetime(2025, 1, 2))
        }]

        executions = lambda_function.get_ingest_timings(sfn_client, cut_off_date, workflow_executions, "ingest-arn",
                                                        "preingest-arn")

        self.assertEqual(1, sfn_client.list_executions.call_count)
        self.assertEqual(1, sfn_client.describe_execution.call_count)
        self.assertEqual({"maxResults": 100, "stateMachineArn": "ingest-arn", "statusFilter": "SUCCEEDED"},
                         sfn_client.list_executions.call_args_list[0][1])

        self.assertEqual({"executionArn": "preingest-arn:TDR_ingestExecutionName"},
                         sfn_client.describe_execution.call_args_list[0][1])

        self.assertEqual(
            [
                {
                    "name": "TDR_ingestExecutionName",
                    "steps": {
                        "preservicaIngest": {
                            "endDate": 1735776000.0,
                            "name": "TDR_ingestExecutionName",
                            "startDate": 1735776000.0
                        },
                        "preservicaWorkflow": {
                            "endDate": 1735776000.0,
                            "name": "TDR_ingestExecutionName-nameOfWorkflowExecution",
                            "startDate": 1735776000.0
                        },
                        "preingest": {
                            "endDate": 1735776000.0,
                            "name": "preingestExecutionName",
                            "startDate": 1735776000.0
                        }
                    }
                }
            ],
            list(executions)
        )

    def test_lambda_handler_should_call_correct_functions(self):
        event = {"time": "2025-04-21T10:00:00Z"}
        sfn_client = Mock()
        lambda_function.sfn_client = lambda: sfn_client
        sfn_response = {"executions": [self.workflow_executions[0]]}
        lambda_function.sfn_client.list_executions = Mock(return_value=sfn_response)

        s3_client = Mock()
        lambda_function.s3_client = lambda: s3_client
        s3_client.put_object = Mock()

        with (patch("sfn_timings.lambda_function.get_workflow_executions") as get_workflow_executions,
              patch("sfn_timings.lambda_function.get_ingest_timings") as get_ingest_timings):
            get_workflow_executions.return_value = []
            get_ingest_timings.return_value = ({"name": "ingestExecutionName"},)
            lambda_function.lambda_handler(event, {})

            expected_time = UTC.localize(datetime(2025, 4, 21, 9, 50))
            get_workflow_executions.assert_called_with(sfn_client, expected_time, "workflow-arn")
            get_ingest_timings.assert_called_with(sfn_client, expected_time, [], "ingest-arn", "preingest-arn")

            expected_body = """{"name": "ingestExecutionName"}"""
            s3_client.put_object.assert_called_with(Body=expected_body, Bucket="bucket-name",
                                                    Key="step-function-timings-for-ingestExecutionName.json",
                                                    ContentType="application/json")

    def test_lambda_handler_should_throw_an_error_if_s3_returns_an_error(self):
        event = {"time": "2025-04-21T10:00:00Z"}
        sfn_client = Mock()
        lambda_function.sfn_client = lambda: sfn_client
        sfn_response = {"executions": [self.workflow_executions[0]]}
        sfn_client.list_executions = Mock(return_value=sfn_response)

        s3_client = Mock()
        lambda_function.s3_client = lambda: s3_client
        s3_client.put_object.side_effect = Exception("S3 Error")

        with (patch("sfn_timings.lambda_function.get_workflow_executions") as get_workflow_executions,
              patch("sfn_timings.lambda_function.get_ingest_timings") as get_ingest_timings):
            get_workflow_executions.return_value = []
            get_ingest_timings.return_value = ({"name": "ingestExecutionName"},)

            with self.assertRaises(Exception) as cm:
                lambda_function.lambda_handler(event, {})

            self.assertEqual("There was an error when attempting to upload file "
                             "step-function-timings-for-ingestExecutionName.json to bucket bucket-name: S3 Error",
                             cm.exception.args[0])


if __name__ == "__main__":
    unittest.main()
