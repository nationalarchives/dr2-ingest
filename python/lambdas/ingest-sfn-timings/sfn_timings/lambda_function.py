import json
import os

import boto3

common_exec_args = {"maxResults": 1000, "statusFilter": "SUCCEEDED"}

s3_client = lambda: boto3.client("s3")
step_function_client = lambda: boto3.client("stepfunctions")


def get_executions(sfn_client, execution_args):
    executions = []
    paginator = sfn_client.get_paginator("list_executions")
    page_iterator = paginator.paginate(**execution_args)

    for page in page_iterator:
        workflow_sfn_executions = page.get("executions", [])
        executions.extend(workflow_sfn_executions)

    return executions


def get_preingest_executions(sfn_client, preingest_sfn_arn):
    execution_args = {"stateMachineArn": preingest_sfn_arn} | common_exec_args
    preingest_executions = get_executions(sfn_client, execution_args)
    return preingest_executions


def get_workflow_executions(sfn_client, workflow_sfn_arn):
    execution_args = {"stateMachineArn": workflow_sfn_arn} | common_exec_args
    workflow_executions = get_executions(sfn_client, execution_args)
    return workflow_executions


def get_ingest_timings(sfn_client, ingest_sfn_arn, workflow_sfn_executions, preingest_executions):
    executions = []
    execution_args = {"stateMachineArn": ingest_sfn_arn} | common_exec_args
    preingest_prefixes = ("TDR_",)

    ingest_sfn_executions = get_executions(sfn_client, execution_args)
    for execution in ingest_sfn_executions:
        name = execution["name"]
        execution_info = {
            "name": name,
            "steps": {
                "ingest": {
                    "name": name,
                    "startDate": execution["startDate"].timestamp(),
                    "endDate": execution["stopDate"].timestamp()
                }
            }
        }

        started_by_preingest = any(name.startswith(prefix) for prefix in preingest_prefixes)
        if started_by_preingest:
            preingest_execution_info = [preingest_ex for preingest_ex in preingest_executions if preingest_ex[
                "name"].endswith(name)][0]
            execution_info["steps"][f"preingest"] = {
                "name": preingest_execution_info["name"],
                "startDate": preingest_execution_info["startDate"].timestamp(),
                "endDate": preingest_execution_info["stopDate"].timestamp()
            }

        workflow_execution = [workflow_ex for workflow_ex in workflow_sfn_executions if name in workflow_ex["name"]][0]

        execution_info["steps"]["workflow"] = {
            "name": workflow_execution["name"],
            "startDate": workflow_execution["startDate"].timestamp(),
            "endDate": workflow_execution["stopDate"].timestamp()
        }

        executions.append(execution_info)

    return executions


def lambda_handler(event, context):
    workflow_sfn_arn = os.environ["WORKFLOW_SFN_ARN"]
    ingest_sfn_arn = os.environ["INGEST_SFN_ARN"]
    preingest_sfn_arn = os.environ["PREINGEST_SFN_ARN"]
    bucket_name = os.environ["OUTPUT_BUCKET_NAME"]

    sfn_client = step_function_client()
    preingest_executions = get_preingest_executions(sfn_client, preingest_sfn_arn)
    workflow_sfn_executions = get_workflow_executions(sfn_client, workflow_sfn_arn)
    ingest_timings = get_ingest_timings(sfn_client, ingest_sfn_arn, workflow_sfn_executions, preingest_executions)

    json_file_key = f"step-function-timings.json"
    ingest_timings_json_obj = json.dumps(ingest_timings)

    try:
        s3_client().put_object(Body=ingest_timings_json_obj, Bucket=bucket_name, Key=json_file_key,
                               ContentType="application/json")
    except Exception as e:
        raise Exception(
            f"There was an error when attempting to upload file {json_file_key} to bucket {bucket_name}: {e}"
        )
