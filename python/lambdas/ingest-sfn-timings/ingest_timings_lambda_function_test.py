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
            "executions": [
                {
                    "executionArn": "workflowArn",
                    "stateMachineArn": "workflowStateMachineArn",
                    "name": "ingestExecutionName-nameOfWorkflowExecution",
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
                    "executionArn": "workflowArn2",
                    "stateMachineArn": "workflowStateMachineArn2",
                    "name": "ingestExecutionName-nameOfWorkflowExecution2",
                    "status": "SUCCEEDED",
                    "startDate": UTC.localize(datetime(2025, 1, 1)),
                    "stopDate": UTC.localize(datetime(2025, 1, 1))
                }
            ]
        }
    ]

    preingest_executions_responses = [
        {
            "executions": [
                {
                    "executionArn": "preingestArn",
                    "stateMachineArn": "preingestArnStateMachineArn",
                    "name": "preingestExecutionName",
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
                    "executionArn": "preingestArn2",
                    "stateMachineArn": "preingestArnStateMachineArn2",
                    "name": "preingestExecutionName2",
                    "status": "SUCCEEDED",
                    "startDate": UTC.localize(datetime(2025, 1, 1)),
                    "stopDate": UTC.localize(datetime(2025, 1, 1))
                }
            ]
        }
    ]

    ingest_executions_responses = [
        {
            "executions": [
                {
                    "executionArn": "ingestArn",
                    "stateMachineArn": "ingestArnStateMachineArn",
                    "name": "TDR_ingestExecutionName",
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

    def test_get_executions_should_call_the_paginator_and_return_the_executions(self):
        sfn_client = Mock()
        paginator = Mock()
        response = [self.workflow_executions[0]]

        sfn_client.get_paginator = Mock(return_value=paginator)
        paginator.paginate = Mock(return_value=response)
        executions = lambda_function.get_executions(sfn_client, {"arg1": "value1", "arg2": "value2"})

        self.assertEqual(1, sfn_client.get_paginator.call_count)
        self.assertEqual(("list_executions",), sfn_client.get_paginator.call_args_list[0][0])

        self.assertEqual(1, paginator.paginate.call_count)
        self.assertEqual({"arg1": "value1", "arg2": "value2"}, paginator.paginate.call_args_list[0][1])

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

    def test_get_preingest_executions_should_call_get_executions_and_return_the_executions(self):
        response = self.preingest_executions_responses[0]

        lambda_function.get_executions = Mock(return_value=response)

        executions = lambda_function.get_preingest_executions("sfn_client", "preingest_sfn_arn")

        self.assertEqual(1, lambda_function.get_executions.call_count)
        self.assertEqual(("sfn_client", {"stateMachineArn": "preingest_sfn_arn", "maxResults": 1000, "statusFilter":
            "SUCCEEDED"}), lambda_function.get_executions.call_args_list[0][0])

        self.assertEqual(
            {"executions": [{
                "executionArn": "preingestArn",
                "name": "preingestExecutionName",
                "stateMachineArn": "preingestArnStateMachineArn",
                "status": "SUCCEEDED",
                "startDate": UTC.localize(datetime(2025, 1, 2)),
                "stopDate": UTC.localize(datetime(2025, 1, 2))
            }],
                "nextToken": "next_token"
            },
            executions
        )

    def test_get_workflow_executions_should_call_get_executions_and_return_the_executions(self):
        response = self.workflow_executions[0]

        lambda_function.get_executions = Mock(return_value=response)

        executions = lambda_function.get_workflow_executions("sfn_client", "workflow_sfn_arn")

        self.assertEqual(1, lambda_function.get_executions.call_count)
        self.assertEqual(("sfn_client", {"stateMachineArn": "workflow_sfn_arn", "maxResults": 1000, "statusFilter":
            "SUCCEEDED"}), lambda_function.get_executions.call_args_list[0][0])

        self.assertEqual(
            {"executions": [{
                "executionArn": "workflowArn",
                "name": "ingestExecutionName-nameOfWorkflowExecution",
                "stateMachineArn": "workflowStateMachineArn",
                "status": "SUCCEEDED",
                "startDate": UTC.localize(datetime(2025, 1, 2)),
                "stopDate": UTC.localize(datetime(2025, 1, 2))
            }],
                "nextToken": "next_token"
            },
            executions
        )

    def test_get_ingest_timings_should_call_get_executions_once_and_return_preingest_info_if_the_name_of_the_execution_starts_with_tdr(
        self):
        response = self.ingest_executions_responses[0]["executions"]
        lambda_function.get_executions = Mock(return_value=response)

        preingest_executions = [{
            "executionArn": "preingestArn",
            "stateMachineArn": "preingestArnStateMachineArn",
            "name": "TDR_ingestExecutionName",
            "status": "SUCCEEDED",
            "startDate": UTC.localize(datetime(2025, 1, 2)),
            "stopDate": UTC.localize(datetime(2025, 1, 2))
        }]

        workflow_executions = [{
            "executionArn": "workflowArn",
            "name": "TDR_ingestExecutionName-nameOfWorkflowExecution",
            "stateMachineArn": "workflowStateMachineArn",
            "status": "SUCCEEDED",
            "startDate": UTC.localize(datetime(2025, 1, 2)),
            "stopDate": UTC.localize(datetime(2025, 1, 2))
        }]

        executions = lambda_function.get_ingest_timings("sfn_client", "ingest-arn", workflow_executions,
                                                        preingest_executions)

        self.assertEqual(1, lambda_function.get_executions.call_count)
        self.assertEqual(("sfn_client", {"stateMachineArn": "ingest-arn", "maxResults": 1000, "statusFilter":
            "SUCCEEDED"}), lambda_function.get_executions.call_args_list[0][0])

        self.assertEqual(
            [
                {
                    "name": "TDR_ingestExecutionName",
                    "steps": {
                        "ingest": {
                            "endDate": 1735776000.0,
                            "name": "TDR_ingestExecutionName",
                            "startDate": 1735776000.0
                        },
                        "workflow": {
                            "endDate": 1735776000.0,
                            "name": "TDR_ingestExecutionName-nameOfWorkflowExecution",
                            "startDate": 1735776000.0
                        },
                        "preingest": {
                            "endDate": 1735776000.0,
                            "name": "TDR_ingestExecutionName",
                            "startDate": 1735776000.0
                        }
                    }
                }
            ],
            list(executions)
        )

    def test_get_ingest_timings_should_call_get_executions_once_but_not_return_preingest_info_if_the_name_of_the_execution_does_not_start_with_tdr(
        self):
        response = self.ingest_executions_responses[0]["executions"]
        response[0]["name"] = "NotTDR_ingestExecutionName"
        lambda_function.get_executions = Mock(return_value=response)

        preingest_executions = [{
            "executionArn": "preingestArn",
            "stateMachineArn": "preingestArnStateMachineArn",
            "name": "NotTDR_ingestExecutionName",
            "status": "SUCCEEDED",
            "startDate": UTC.localize(datetime(2025, 1, 2)),
            "stopDate": UTC.localize(datetime(2025, 1, 2))
        }]

        workflow_executions = [{
            "executionArn": "workflowArn",
            "name": "NotTDR_ingestExecutionName-nameOfWorkflowExecution",
            "stateMachineArn": "workflowStateMachineArn",
            "status": "SUCCEEDED",
            "startDate": UTC.localize(datetime(2025, 1, 2)),
            "stopDate": UTC.localize(datetime(2025, 1, 2))
        }]

        executions = lambda_function.get_ingest_timings("sfn_client", "ingest-arn", workflow_executions,
                                                        preingest_executions)

        self.assertEqual(1, lambda_function.get_executions.call_count)
        self.assertEqual(("sfn_client", {"stateMachineArn": "ingest-arn", "maxResults": 1000, "statusFilter":
            "SUCCEEDED"}), lambda_function.get_executions.call_args_list[0][0])

        self.assertEqual(
            [
                {
                    "name": "NotTDR_ingestExecutionName",
                    "steps": {
                        "ingest": {
                            "endDate": 1735776000.0,
                            "name": "NotTDR_ingestExecutionName",
                            "startDate": 1735776000.0
                        },
                        "workflow": {
                            "endDate": 1735776000.0,
                            "name": "NotTDR_ingestExecutionName-nameOfWorkflowExecution",
                            "startDate": 1735776000.0
                        }
                    }
                }
            ],
            list(executions)
        )

    def test_lambda_handler_should_call_correct_functions(self):
        sfn_client = "sfn_client"
        lambda_function.step_function_client = lambda: sfn_client
        s3_client = Mock()
        lambda_function.s3_client = lambda: s3_client
        s3_client.put_object = Mock()

        with (patch("sfn_timings.lambda_function.get_preingest_executions") as get_preingest_executions,
              patch("sfn_timings.lambda_function.get_workflow_executions") as get_workflow_executions,
              patch("sfn_timings.lambda_function.get_ingest_timings") as get_ingest_timings):
            get_preingest_executions.return_value = []
            get_workflow_executions.return_value = []
            get_ingest_timings.return_value = ({"name": "ingestExecutionName"},)

            lambda_function.lambda_handler({}, {})

            get_preingest_executions.assert_called_with("sfn_client", "preingest-arn")
            get_workflow_executions.assert_called_with("sfn_client", "workflow-arn")
            get_ingest_timings.assert_called_with("sfn_client", "ingest-arn", [], [])

            expected_body = """[{"name": "ingestExecutionName"}]"""
            s3_client.put_object.assert_called_with(Body=expected_body, Bucket="bucket-name",
                                                    Key="step-function-timings.json",
                                                    ContentType="application/json")

    def test_lambda_handler_should_throw_an_error_if_s3_returns_an_error(self):
        s3_client = Mock()
        lambda_function.s3_client = lambda: s3_client
        s3_client.put_object.side_effect = Exception("S3 Error")

        with (patch("sfn_timings.lambda_function.get_preingest_executions") as get_preingest_executions,
              patch("sfn_timings.lambda_function.get_workflow_executions") as get_workflow_executions,
              patch("sfn_timings.lambda_function.get_ingest_timings") as get_ingest_timings):
            get_preingest_executions.return_value = []
            get_workflow_executions.return_value = []
            get_ingest_timings.return_value = ({"name": "ingestExecutionName"},)

            with self.assertRaises(Exception) as cm:
                lambda_function.lambda_handler({}, {})

            self.assertEqual("There was an error when attempting to upload file "
                             "step-function-timings.json to bucket bucket-name: S3 Error",
                             cm.exception.args[0])


if __name__ == "__main__":
    unittest.main()
