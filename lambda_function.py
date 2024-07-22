import os
import json
import urllib3
import boto3
from urllib3.exceptions import ConnectTimeoutError
from boto3 import resource
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr


def get_item_with_id(table, primary_key, primary_key_value):
    response = table.query(KeyConditionExpression=Key(primary_key).eq(primary_key_value))
    items = response["Items"]

    if len(items) == 0:
        raise Exception(f"{len(items)} items have the '{primary_key}' primary key of '{primary_key_value}'")
    else:
        return items[0]


def add_true_to_ingest_cc_attribute(table, primary_key, sort_key, item_with_id):
    primary_key_value = item_with_id[primary_key]
    sort_key_value = item_with_id[sort_key]

    table.update_item(
        Key={primary_key: primary_key_value, sort_key: sort_key_value},
        UpdateExpression="SET ingested_CC = :ingestedCCValue",
        ExpressionAttributeValues={":ingestedCCValue": "true"},
        ConditionExpression=f"attribute_exists({primary_key})"
    )


def get_message_from_json_event(event):
    return json.loads(event['Records'][0]['Sns']['Message'])


def lambda_handler(event, context):
    message_json_as_dict = get_message_from_json_event(event)
    primary_key_value = message_json_as_dict["tableItemIdentifier"]
    primary_key = "id"
    sort_key = "batchId"

    table = resource("dynamodb").Table(os.environ["DYNAMO_TABLE_NAME"])
    item_with_id = get_item_with_id(table, primary_key, primary_key_value)
    add_true_to_ingest_cc_attribute(table, primary_key, sort_key, item_with_id)
