import json
import os
from datetime import datetime, timedelta

import boto3
from pytz import UTC

common_exec_args = {"maxResults": 100, "statusFilter": "SUCCEEDED"}

s3_client = lambda: boto3.client("s3")
sfn_client = lambda: boto3.client("stepfunctions")


def next_token(executions_info):
    token = executions_info.get("nextToken", "")
    return {} if token == "" else {"nextToken": token}


def get_workflow_executions(sfn, cut_off_date, workflow_sfn_arn, relevant_executions=[], next_page_token={}) \
    -> list[dict]:
    execution_args = {"stateMachineArn": workflow_sfn_arn} | common_exec_args
    execution_args.update(next_page_token)

    if next_page_token or not relevant_executions:
        workflow_sfn_executions_info = sfn.list_executions(**execution_args)
        workflow_sfn_executions = workflow_sfn_executions_info["executions"]

        for execution in workflow_sfn_executions:
            stop_date = execution["stopDate"]

            if stop_date < cut_off_date:
                break

            relevant_executions.append(execution)
        else:
            next_page_token = next_token(workflow_sfn_executions_info)
            relevant_executions = get_workflow_executions(sfn, cut_off_date, workflow_sfn_arn, relevant_executions,
                                                          next_page_token)

    return relevant_executions


def get_ingest_timings(sfn, cut_off_date, workflow_sfn_executions, ingest_sfn_arn, preingest_sfn_arn, executions=
tuple(), next_page_token={}):
    execution_args = {"stateMachineArn": ingest_sfn_arn} | common_exec_args
    execution_args.update(next_page_token)

    if next_page_token or not executions:
        ingest_sfn_executions_info = sfn.list_executions(**execution_args)
        ingest_sfn_executions = ingest_sfn_executions_info["executions"]

        for execution in ingest_sfn_executions:
            stop_date = execution["stopDate"]

            if stop_date < cut_off_date:
                break

            name = execution["name"]
            execution_info = {
                "name": name,
                "steps": {
                    "preservicaIngest": {
                        "name": name,
                        "startDate": execution["startDate"].timestamp(),
                        "endDate": execution["stopDate"].timestamp()
                    }
                }
            }

            if name.startswith("TDR_"):
                preingest_sfn_execution_info = sfn.describe_execution(executionArn=f"{preingest_sfn_arn}:{name}")

                execution_info["steps"]["preingest"] = {
                    "name": preingest_sfn_execution_info["name"],
                    "startDate": preingest_sfn_execution_info["startDate"].timestamp(),
                    "endDate": preingest_sfn_execution_info["stopDate"].timestamp()
                }

            workflow_execution = \
                [workflow_ex for workflow_ex in workflow_sfn_executions if name in workflow_ex["name"]][0]

            execution_info["steps"]["preservicaWorkflow"] = {
                "name": workflow_execution["name"],
                "startDate": workflow_execution["startDate"].timestamp(),
                "endDate": workflow_execution["stopDate"].timestamp()
            }

            executions = executions + (execution_info,)

        else:
            next_page_token = next_token(ingest_sfn_executions_info)
            executions = get_ingest_timings(sfn, cut_off_date, workflow_sfn_executions, ingest_sfn_arn,
                                            preingest_sfn_arn, executions, next_page_token)

    return executions


def lambda_handler(event, context):
    event_triggered_date = UTC.localize(datetime.strptime(event["time"], "%Y-%m-%dT%H:%M:%SZ"))
    last_ingest_completion_date = event_triggered_date - timedelta(
        minutes=10)  # 10 minutes since the last lambda execution

    workflow_sfn_arn = os.environ["WORKFLOW_SFN_ARN"]
    ingest_sfn_arn = os.environ["INGEST_SFN_ARN"]
    preingest_sfn_arn = os.environ["PREINGEST_SFN_ARN"]
    bucket_name = os.environ["OUTPUT_BUCKET_NAME"]

    sfn = sfn_client()
    workflow_sfn_executions = get_workflow_executions(sfn, last_ingest_completion_date, workflow_sfn_arn)
    ingest_timings = get_ingest_timings(sfn, last_ingest_completion_date, workflow_sfn_executions, ingest_sfn_arn,
                                        preingest_sfn_arn)

    for ingest_timing in ingest_timings:
        json_file_key = f"""step-function-timings-for-{ingest_timing["name"]}.json"""
        ingest_timing_json_obj = json.dumps(ingest_timing)

        try:
            s3_client().put_object(Body=ingest_timing_json_obj, Bucket=bucket_name, Key=json_file_key,
                                   ContentType="application/json")
        except Exception as e:
            print(f"There was an error when attempting to upload file {json_file_key} to bucket {bucket_name}: {e}")
