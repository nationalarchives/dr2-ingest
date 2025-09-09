import collections
import re
from datetime import datetime, timezone

import boto3
from dateutil.parser import isoparse
from moto.utilities.paginator import paginate

SOURCE_SYSTEMS = ["TDR", "COURTDOC", "DEFAULT"] # do we have one for DRI migration?

def get_stepfunction_metrics(resources_prefix):
    metric_data = []
    sfn_client = boto3.client("stepfunctions")

    paginator = sfn_client.get_paginator("list_state_machines")

    for page in paginator.paginate():
        for state_machine in page["stateMachines"]:
            state_machine_name = state_machine["name"]
            state_machine_arn = state_machine["stateMachineArn"]

            executions = sfn_client.list_executions(stateMachineArn = state_machine_arn, statusFilter="RUNNING")["executions"]
            metric_data.append(
                {
                    "MetricName": "ExecutionsRunning",
                    "Dimensions": [
                        {"Name" : "StateMachineArn", "Value": state_machine_arn},
                    ],
                    "Value" : len(executions),
                    "Unit": "Count"
                }
            )

            if state_machine_name.startswith(resources_prefix) :
                execution_ss = [e["name"].split("_", 1)[0] for e in executions]
                counts = collections.Counter(execution_ss)
                execution_counts = {ss: counts.get(ss, 0) for ss in SOURCE_SYSTEMS}
                unlisted_ss_count = sum(count for system, count in counts.items() if system not in SOURCE_SYSTEMS)
                execution_counts["DEFAULT"] = execution_counts.get("DEFAULT", 0) + unlisted_ss_count

                metric_data.extend(
                    {
                        "MetricName": "ExecutionsRunning",
                        "Dimensions" : [
                            {"Name": "StateMachineArn", "Value": state_machine_arn},
                            {"Name" : "SourceSystem", "Value": ss},
                        ],
                        "Value" : counts_ss,
                        "Unit": "Count"
                    }
                    for ss, counts_ss in execution_counts.items()
                )
    return metric_data

def get_flow_control_metrics(resources_prefix):
    metric_data = []
    dynamo_client = boto3.client("dynamodb")
    queue_table = resources_prefix + "-queue"

    for source_system in SOURCE_SYSTEMS:
        item_result = dynamo_client.query(
            TableName = queue_table,
            KeyConditionExpression = "sourceSystem = :ssPlaceHolder",
            ExpressionAttributeValues = {":ssPlaceHolder": {"S": source_system}}
        )
        metric_data.append(
            {
                "MetricName": "IngestsQueued",
                "Dimensions": [
                    {"Name": "SourceSystem", "Value": source_system},
                ],
                "Value": len(item_result["Items"]),
                "Unit": "Count"
            }
        )

        if item_result["Items"]:
            queued_at = isoparse(item_result["Items"][0]["queuedAt"]["S"].split("_")[0])
            oldest_item_age = (datetime.now(timezone.utc) - queued_at).total_seconds()
        else:
            oldest_item_age = 0
        metric_data.append(
            {
                "MetricName": "ApproximateAgeOfOldestQueuedIngest",
                "Dimensions": [
                    {"Name": "SourceSystem", "Value": source_system},
                ],
                "Value": oldest_item_age,
                "Unit": "seconds"
            }
        )
    return metric_data

def lambda_handler(event, context):
    resources_prefix = context.function_name.split("-")[0] + "-dr2-ingest"
    metric_data = [
        *get_stepfunction_metrics(resources_prefix),
        *get_flow_control_metrics(resources_prefix)
    ]

    cloudwatch_client = boto3.client('cloudwatch')
    cloudwatch_client.put_metric_data(
        Namespace = resources_prefix,
        MetricData = metric_data
    )