import os
import json
import urllib3
import boto3
from urllib3.exceptions import ConnectTimeoutError
from boto3 import resource
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

convert_value_to_bool = {"true": True, "false": False}
attribute_to_add = "ingested_CC"


def get_items_with_id(table, primary_key, primary_key_value):
    response = table.query(KeyConditionExpression=Key(primary_key).eq(primary_key_value))
    items = response["Items"]

    return items


def add_true_to_ingest_cc_attribute(table, primary_key, sort_key, items_with_id):
    for item_with_id in items_with_id:
        attribute_value = item_with_id.get(attribute_to_add, "false")
        attribute_value_is_true = convert_value_to_bool.get(attribute_value, False)

        if not attribute_value_is_true:
            primary_key_value = item_with_id[primary_key]
            sort_key_value = item_with_id[sort_key]

            table.update_item(
                Key={primary_key: primary_key_value, sort_key: sort_key_value},
                UpdateExpression=f"SET {attribute_to_add} = :ingestedCCValue",
                ExpressionAttributeValues={":ingestedCCValue": "true"},
                ConditionExpression=f"attribute_exists({primary_key})"
            )


def get_messages_from_json_event(event) -> list[dict]:
    sqs_records = event["Records"]
    messages = [json.loads(sqs_record["body"]) for sqs_record in sqs_records]
    return messages


def lambda_handler(event, context):
    table = resource("dynamodb").Table(os.environ["DYNAMO_TABLE_NAME"])
    primary_key = "id"
    sort_key = "batchId"

    message_jsons_as_dicts: list[dict] = get_messages_from_json_event(event)

    for message_json_as_dict in message_jsons_as_dicts:
        primary_key_value = message_json_as_dict["tableItemIdentifier"]
        items_with_id = get_items_with_id(table, primary_key, primary_key_value)
        add_true_to_ingest_cc_attribute(table, primary_key, sort_key, items_with_id)
