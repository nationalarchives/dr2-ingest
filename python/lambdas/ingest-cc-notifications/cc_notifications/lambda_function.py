import json
import os

import boto3

convert_value_to_bool = {"true": True, "false": False}
attribute_to_add = "ingested_CC"


def get_items_with_id(client, table_name, primary_key, primary_key_value):
    response = client.query(
        TableName=table_name,
        KeyConditionExpression=f"{primary_key} = :value",
        ExpressionAttributeValues={":value": {"S": primary_key_value}},
        ProjectionExpression=f"id,batchId,{attribute_to_add}"
    )
    items = response["Items"]

    return items


def add_true_to_ingest_cc_attribute(client, table_name, primary_key, sort_key, items_with_id):
    for item_with_id in items_with_id:
        attribute_type_and_value = item_with_id.get(attribute_to_add, {"S": "false"})
        attribute_value = attribute_type_and_value["S"]
        attribute_value_is_true = convert_value_to_bool.get(attribute_value, False)

        if not attribute_value_is_true:
            primary_key_type_and_value = item_with_id[primary_key]
            sort_key_value_type_and_value = item_with_id[sort_key]

            client.update_item(
                TableName=table_name,
                Key={primary_key: primary_key_type_and_value, sort_key: sort_key_value_type_and_value},
                UpdateExpression=f"SET {attribute_to_add} = :ingestedCCValue",
                ExpressionAttributeValues={":ingestedCCValue": {"S": "true"}},
                ConditionExpression=f"attribute_exists({primary_key})"
            )


def get_messages_from_json_event(event) -> list[dict]:
    sqs_records = event["Records"]
    messages = [json.loads(sqs_record["body"]) for sqs_record in sqs_records]
    non_deleted_messages = [message
                            for message in messages
                            if message["status"] != "Deleted"]
    return non_deleted_messages


def lambda_handler(event, context):
    client = boto3.client('dynamodb')
    table_name = os.environ["FILES_DDB_TABLE"]
    primary_key = "id"
    sort_key = "batchId"

    message_jsons_as_dicts: list[dict] = get_messages_from_json_event(event)

    for message_json_as_dict in message_jsons_as_dicts:
        primary_key_value = message_json_as_dict["tableItemIdentifier"]
        if primary_key_value == "":
            raise ValueError(f"Table identifier is missing for message {message_json_as_dict}")
        items_with_id = get_items_with_id(client, table_name, primary_key, primary_key_value)
        add_true_to_ingest_cc_attribute(client, table_name, primary_key, sort_key, items_with_id)
