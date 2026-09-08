import collections
import logging
import os
from datetime import datetime, timezone
import json

import boto3
from dateutil.parser import isoparse

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def ss_metrics_template(metric_name, source_system, value, unit):
    return {
        "MetricName": metric_name,
        "Dimensions": [
            {"Name": "SourceSystem", "Value": source_system},
        ],
        "Value": value,
        "Unit": unit
    }


def get_stepfunction_metrics(resources_prefix, source_systems, sfn_state_with_output):
    metric_data = []
    sfn_client = boto3.client("stepfunctions")

    paginator = sfn_client.get_paginator("list_state_machines")

    for page in paginator.paginate():
        for state_machine in page["stateMachines"]:
            state_machine_name = state_machine["name"]
            state_machine_arn = state_machine["stateMachineArn"]

            executions = sfn_client.list_executions(stateMachineArn=state_machine_arn, statusFilter="RUNNING")[
                "executions"]
            metric_data.append(
                {
                    "MetricName": "ExecutionsRunning",
                    "Dimensions": [
                        {"Name": "StateMachineArn", "Value": state_machine_arn},
                        {"Name": "StateMachineName", "Value": state_machine_name},
                    ],
                    "Value": len(executions),
                    "Unit": "Count"
                }
            )

            if state_machine_name.startswith(resources_prefix):
                execution_ss = [e["name"].split("_", 1)[0] for e in executions]
                counts = collections.Counter(execution_ss)
                ss_execution_counts = {ss: counts.get(ss, 0) for ss in source_systems}
                unlisted_ss_count = sum(count for system, count in counts.items() if system not in source_systems)
                ss_execution_counts["DEFAULT"] = ss_execution_counts.get("DEFAULT", 0) + unlisted_ss_count

                metric_data.extend(
                    {
                        "MetricName": "ExecutionsRunning",
                        "Dimensions": [
                            {"Name": "StateMachineArn", "Value": state_machine_arn},
                            {"Name": "StateMachineName", "Value": state_machine_name},
                            {"Name": "SourceSystem", "Value": ss},
                        ],
                        "Value": counts_ss,
                        "Unit": "Count"
                    }
                    for ss, counts_ss in ss_execution_counts.items()
                )

            for execution in executions:
                history_paginator = sfn_client.get_paginator("get_execution_history")
                execution_arn = execution["executionArn"]
                execution_name = execution["name"]
                ss = execution_name.split("_", 1)[0]
                ss = ss if ss in source_systems else "DEFAULT"

                history_pages = history_paginator.paginate(executionArn=execution_arn)

                for history_page in history_pages:
                    for history_event in history_page["events"]:
                        mapper_task_exited = history_event["type"] == "TaskStateExited" and history_event[
                            "stateExitedEventDetails"]["name"] == sfn_state_with_output
                        if mapper_task_exited:
                            mapper_output_str = history_event["stateExitedEventDetails"].get("output")
                            if mapper_output_str:
                                output_dict: dict = json.loads(mapper_output_str)
                                total_asset_count = output_dict["totalAssetCount"]
                                total_file_bytes = output_dict["totalFileBytes"]
                                metric_data.extend([
                                    {
                                        "MetricName": "AssetCount",
                                        "Dimensions": [
                                            {"Name": "SourceSystem", "Value": ss},
                                        ],
                                        "Value": int(total_asset_count),
                                        "Unit": "Count"
                                    },
                                    {
                                        "MetricName": "Bytes",
                                        "Dimensions": [
                                            {"Name": "SourceSystem", "Value": ss},
                                        ],
                                        "Value": int(total_file_bytes),
                                        "Unit": "Count"
                                    }
                                ]
                                )
                                break
                            else:
                                raise Exception("Mapper Lambda Task exited but produced no output.")

                    else:
                        raise Exception(
                            f"Task '{sfn_state_with_output}' not found in list of events with the status 'TaskStateExited'")
    return metric_data


def get_flow_control_metrics(resources_prefix, source_systems):
    metric_data = []
    dynamo_client = boto3.client("dynamodb")
    queue_table = resources_prefix + "-queue"

    for source_system in source_systems:
        item_result = dynamo_client.query(
            TableName=queue_table,
            KeyConditionExpression="sourceSystem = :ssPlaceHolder",
            ExpressionAttributeValues={":ssPlaceHolder": {"S": source_system}}
        )
        items = item_result["Items"]

        metric_data.append(
            ss_metrics_template("IngestsQueued", source_system, len(items), "Count")
        )

        if items:
            queued_at = isoparse(items[0]["queuedAt"]["S"].split("_")[0])
            oldest_item_age = int((datetime.now(timezone.utc) - queued_at).total_seconds())
            queued_asset_count = sum(int(item["queuedAssetCount"]) for item in items)
            queued_file_bytes = sum(int(item["queuedBytes"]) for item in items)
        else:
            oldest_item_age = 0
            queued_asset_count = 0
            queued_file_bytes = 0
        metric_data.append(
            ss_metrics_template("ApproximateAgeOfOldestQueuedIngest", source_system, oldest_item_age, "Seconds")
        )

        metric_data.append(
            ss_metrics_template("QueuedAssetCount", source_system, queued_asset_count, "Count")
        )

        metric_data.append(
            ss_metrics_template("QueuedBytes", source_system, queued_file_bytes, "Bytes")
        )
    return metric_data


def lambda_handler(event, context):
    source_systems = tuple(json.loads(os.environ["SOURCE_SYSTEMS"]))
    mapper_lambda_state_name = tuple(json.loads(os.environ["MAPPER_LAMBDA_STATE_NAME"]))

    resources_prefix = context.function_name.split("-")[0] + "-dr2-ingest"
    metric_data = []
    sfn_collection_failed = False
    queue_collection_failed = False
    try:
        sfn_metrics = get_stepfunction_metrics(resources_prefix, source_systems, mapper_lambda_state_name)
        metric_data.extend(sfn_metrics)
        logger.info("Successfully collected step function metrics")
    except Exception as e:
        logger.warning("Failed to collect step function metrics: %s", e, exc_info=True)
        sfn_collection_failed = True

    try:
        queue_metrics = get_flow_control_metrics(resources_prefix, source_systems)
        metric_data.extend(queue_metrics)
        logger.info("Successfully collected queue metrics")
    except Exception as e:
        logger.warning("Failed to collect queue metrics@ %s", e, exc_info=True)
        queue_collection_failed = True

    if sfn_collection_failed and queue_collection_failed:
        raise Exception(
            f"Failed to collect metrics for step function as well as the queued executions. Unable to proceed"
        )
    else:
        try:
            cloudwatch_client = boto3.client('cloudwatch')
            cloudwatch_client.put_metric_data(
                Namespace=resources_prefix,
                MetricData=metric_data
            )
        except Exception as e:
            raise Exception(f"Failed to send metrics to CloudWatch due to underlying exception: '{e}'")
