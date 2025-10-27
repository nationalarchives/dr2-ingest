import io
import json

import boto3
from botocore.config import Config

config = Config(region_name="eu-west-2")
session = boto3.Session()

# The following methods are a thin wrapper over the underlying AWS calls to isolate all AWS interactions in one place
def get_account_number():
    sts = session.client("sts")
    return sts.get_caller_identity()["Account"]

def upload_metadata(asset_id, bucket, metadata):
    s3_client = session.client("s3")
    json_bytes = io.BytesIO(json.dumps([metadata]).encode("utf-8"))
    s3_client.upload_fileobj(json_bytes, bucket, f"{asset_id}.metadata")

def upload_file(asset_id, bucket, file_id, file_path):
    s3_client = session.client("s3")
    s3_client.upload_file(file_path, bucket, f'{asset_id}/{file_id}')

def send_message(asset_id, bucket, queue_url):
    sqs_client = session.client("sqs", config=config)
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps({'assetId': asset_id, 'bucket': bucket}))

def refresh_session():
    global session
    session = boto3.Session()