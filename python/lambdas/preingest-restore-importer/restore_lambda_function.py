import itertools
import json
import os
from urllib import parse
from xml.etree import ElementTree as Et

import boto3

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


def lambda_handler(event, context):
    destination_bucket = os.environ["OUTPUT_BUCKET_NAME"]
    destination_queue = os.environ["OUTPUT_QUEUE_URL"]
    for record in event["Records"]:
        body: dict[str, str] = json.loads(record["body"])
        asset_id = body["assetId"]
        parsed_metadata_url = parse.urlparse(body["metadataLocation"])
        metadata_file_key = str(parsed_metadata_url.path[1:])
        source_bucket = parsed_metadata_url.netloc
        response = s3_client.get_object(Bucket=source_bucket, Key=metadata_file_key)
        xml_metadata = Et.fromstring(response['Body'].read().decode('utf-8'))
        try:
            transfer_files = [f"{asset_id}/{node.text}" for node in xml_metadata.findall("./{*}Representation/{*}ContentObjects/{*}ContentObject")]
            transfer_files.append(metadata_file_key)
            copy_objects(destination_bucket, transfer_files, source_bucket)
            sqs_body = {"id": asset_id, "location": f"s3://{destination_bucket}/{metadata_file_key}", "filesPrefix": asset_id}
            for batch in itertools.batched(transfer_files, 1000):
                keys_to_delete = [{'Key': key} for key in batch]
                s3_client.delete_objects(Bucket=source_bucket, Delete={'Objects': keys_to_delete})
            sqs_client.send_message(QueueUrl=destination_queue, MessageBody=json.dumps(sqs_body))
        except Exception as e:
            print(json.dumps({"error": str(e), "assetId": asset_id}))
            raise Exception(e)


def copy_objects(destination_bucket, s3_keys, source_bucket):
    for s3_key in s3_keys:
        try:
            copy_source = {"Bucket": source_bucket, "Key": s3_key}
            s3_client.copy(copy_source, destination_bucket, s3_key, {"TaggingDirective": "REPLACE"})
        except Exception as e:
            print(f"Error during copy of '{s3_key}' from '{source_bucket}' to '{destination_bucket}': {e}")
            raise e
