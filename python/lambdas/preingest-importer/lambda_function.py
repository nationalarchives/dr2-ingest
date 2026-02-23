import itertools
import json
import os
import re
import uuid

import boto3
import botocore
import jsonschema
from botocore.exceptions import ClientError

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


def lambda_handler(event, context):
    destination_bucket = os.environ["OUTPUT_BUCKET_NAME"]
    destination_queue = os.environ["OUTPUT_QUEUE_URL"]
    delete_from_source = os.getenv("DELETE_FROM_SOURCE", "false") == "true"
    for record in event["Records"]:
        body: dict[str, str] = json.loads(record["body"])
        asset_id = body["assetId"] if "assetId" in body else body["fileId"]
        metadata_file_id = f"{asset_id}.metadata"
        source_bucket = body["bucket"]
        try:
            file_objects = assert_objects_exist_in_bucket(source_bucket, asset_id)
            validate_metadata(source_bucket, metadata_file_id)
            transfer_files = [f['Key'] for f in file_objects]
            copy_objects(destination_bucket, transfer_files, source_bucket)
            potential_message_id = {key: value for key, value in body.items() if key == "messageId"}
            sqs_body = {"id": asset_id, "location": f"s3://{destination_bucket}/{metadata_file_id}"}
            sqs_body.update(potential_message_id)
            if delete_from_source:
                for batch in itertools.batched(transfer_files, 1000):
                    keys_to_delete = [{'Key': key} for key in batch]
                    s3_client.delete_objects(Bucket=source_bucket, Delete={'Objects': keys_to_delete})
            sqs_client.send_message(QueueUrl=destination_queue, MessageBody=json.dumps(sqs_body))
        except Exception as e:
            print(json.dumps({"error": str(e), "assetId": asset_id}))
            raise Exception(e)


def list_all_objects(source_bucket, file_id):
    all_contents = []
    is_truncated = True
    while is_truncated:
        response = s3_client.list_objects(Bucket=source_bucket, Prefix=file_id)
        all_contents.extend(response['Contents'])
        is_truncated = response['IsTruncated']

    return all_contents


def assert_objects_exist_in_bucket(source_bucket, asset_id):
    try:
        contents = list_all_objects(source_bucket, asset_id)
        missing_metadata = not any(c for c in contents if c['Key'] == f"{asset_id}.metadata")
        missing_file_objects = not any(c for c in contents if c['Key'] != f"{asset_id}.metadata")
        if missing_file_objects:
            raise Exception(f"Asset '{asset_id}' has no files in '{source_bucket}'")
        if missing_metadata:
            raise Exception(f"Object '{asset_id}.metadata' does not exist in '{source_bucket}'")
        return contents
    except botocore.exceptions.ClientError as ex:
        raise Exception(f"Object '{asset_id}' does not exist in '{source_bucket}', underlying error is: '{ex}'")


def validate_metadata(bucket, s3_key):
    source_system = os.environ["SOURCE_SYSTEM"]
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    json_metadata = json.loads(response['Body'].read().decode('utf-8'))
    for metadata in json_metadata:
        validate_mandatory_fields_exist(f"common/preingest-{source_system}/metadata-schema.json", metadata)
        validate_formats(metadata, bucket, s3_key)


def validate_mandatory_fields_exist(schema_location, json_metadata):
    with open(schema_location, "r") as metadata_schema_file:
        metadata_schema = json.load(metadata_schema_file)
    try:
        validator = jsonschema.Draft202012Validator(schema=metadata_schema,
                                                    format_checker=jsonschema.draft202012_format_checker)
        validator.validate(json_metadata)
    except jsonschema.exceptions.ValidationError as err:
        raise Exception(err.message)

    return True


def validate_formats(json_metadata, bucket, s3_key):
    uuid_content = json_metadata["UUID"]
    try:
        uuid.UUID(uuid_content)
    except ValueError:
        raise Exception(
            f"Unable to parse UUID, '{uuid_content}' from file '{s3_key}' in bucket '{bucket}'. Invalid format")

    series = json_metadata["Series"]
    if not series.strip():
        raise Exception(f"Empty Series value in file '{s3_key}' in bucket '{bucket}'. Unable to proceed")

    pattern = "^([A-Z]{1,4} [1-9][0-9]{0,3}|Unknown|MOCK1 123)$"
    match = re.match(pattern, series)
    if not match:
        raise Exception(f"Invalid Series value, '{series}' in file '{s3_key}' in bucket '{bucket}'. Unable to proceed")

    return True


def copy_objects(destination_bucket, s3_keys, source_bucket):
    for s3_key in s3_keys:
        try:
            copy_source = {"Bucket": source_bucket, "Key": s3_key}
            s3_client.copy(copy_source, destination_bucket, s3_key)
        except Exception as e:
            print(f"Error during copy of '{s3_key}' from '{source_bucket}' to '{destination_bucket}': {e}")
            raise e
