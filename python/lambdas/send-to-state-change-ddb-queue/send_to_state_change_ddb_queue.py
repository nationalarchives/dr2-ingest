import json
import boto3
import os

sqs_client = boto3.client("sqs")


def lambda_handler(event, context):
    sqs_client.send_message(
        QueueUrl=os.environ["QUEUE_URL"],
        MessageBody=json.dumps(event),
    )
    return {}