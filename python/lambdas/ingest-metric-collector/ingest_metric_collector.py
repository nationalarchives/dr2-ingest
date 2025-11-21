import collections
import logging
from datetime import datetime, timezone

import boto3
from dateutil.parser import isoparse

SOURCE_SYSTEMS = {"TDR", "COURTDOC", "PA", "DEFAULT"}

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
                        {"Name" : "StateMachineName", "Value": state_machine_name},
                    ],
                    "Value" : len(executions),
                    "Unit": "Count"
                }
            )

            if state_machine_name.startswith(resources_prefix) :
                execution_ss = [e["name"].split("_", 1)[0] for e in executions]
                counts = collections.Counter(execution_ss)
                ss_execution_counts = {ss: counts.get(ss, 0) for ss in SOURCE_SYSTEMS}
                unlisted_ss_count = sum(count for system, count in counts.items() if system not in SOURCE_SYSTEMS)
                ss_execution_counts["DEFAULT"] = ss_execution_counts.get("DEFAULT", 0) + unlisted_ss_count

                metric_data.extend(
                    {
                        "MetricName": "ExecutionsRunningBySourceSystem",
                        "Dimensions" : [
                            {"Name": "StateMachineArn", "Value": state_machine_arn},
                            {"Name": "StateMachineName", "Value": state_machine_name},
                            {"Name" : "SourceSystem", "Value": ss},
                        ],
                        "Value" : counts_ss,
                        "Unit": "Count"
                    }
                    for ss, counts_ss in ss_execution_counts.items()
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
        items = item_result["Items"]
        metric_data.append(
            {
                "MetricName": "IngestsQueued",
                "Dimensions": [
                    {"Name": "SourceSystem", "Value": source_system},
                ],
                "Value": len(items),
                "Unit": "Count"
            }
        )

        if items:
            queued_at = isoparse(items[0]["queuedAt"]["S"].split("_")[0])
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
                "Unit": "Seconds"
            }
        )
    return metric_data

def lambda_handler(event, context):
    resources_prefix = context.function_name.split("-")[0] + "-dr2-ingest"
    metric_data = []
    sfn_collection_failed = False
    age_collection_failed = False
    try:
        sfn_metrics = get_stepfunction_metrics(resources_prefix)
        metric_data.extend(sfn_metrics)
        logger.info("Successfully collected step function metrics")
    except Exception as e:
        logger.warning("Failed to collect step function metrics: %s", e, exc_info = True)
        sfn_collection_failed = True

    try:
        age_metrics = get_flow_control_metrics(resources_prefix)
        metric_data.extend(age_metrics)
        logger.info("Successfully collected age metrics")
    except Exception as e:
        logger.warning("Failed to collect age metrics@ %s", e, exc_info = True)
        age_collection_failed = True

    if sfn_collection_failed and age_collection_failed:
        raise Exception(f"Failed to collect metrics for step function as well as age of executions. Unable to proceed")
    else:
        try:
            cloudwatch_client = boto3.client('cloudwatch')
            cloudwatch_client.put_metric_data(
                Namespace = resources_prefix,
                MetricData = metric_data
            )
        except Exception as e:
            raise Exception(f"Failed to send metrics to CloudWatch due to underlying exception: '{e}'")