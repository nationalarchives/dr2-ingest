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
    for record in event["Records"]:
        body: dict[str, str] = json.loads(record["body"])
        file_id = body["fileId"]
        metadata_file_id = f"{file_id}.metadata"
        source_bucket = body["bucket"]
        files = [file_id, metadata_file_id]
        assert_objects_exist_in_bucket(source_bucket, files)
        validate_metadata(source_bucket, metadata_file_id)
        file_location = copy_objects(destination_bucket, file_id, source_bucket)
        copy_objects(destination_bucket, metadata_file_id, source_bucket)
        potential_message_id = {key: value for key, value in body.items() if key == "messageId"}
        sqs_body = {"id": file_id, "location": file_location}
        sqs_body.update(potential_message_id)
        sqs_client.send_message(QueueUrl=destination_queue, MessageBody=json.dumps(sqs_body))


def assert_objects_exist_in_bucket(source_bucket, files):
    for file in files:
        try:
            s3_client.head_object(Bucket=source_bucket, Key=file)
        except botocore.exceptions.ClientError as ex:
            raise Exception(f"Object '{file}' does not exist in '{source_bucket}', underlying error is: '{ex}'")


def validate_metadata(bucket, s3_key):
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    json_metadata = json.loads(response['Body'].read().decode('utf-8'))
    validate_mandatory_fields_exist("metadata-schema.json", json_metadata)
    validate_formats(json_metadata, bucket, s3_key)


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


def copy_objects(destination_bucket, s3_key, source_bucket):
    try:
        copy_source = {"Bucket": source_bucket, "Key": s3_key}
        s3_client.copy(copy_source, destination_bucket, s3_key)
        return f"s3://{destination_bucket}/{s3_key}"
    except Exception as e:
        print(f"Error during copy of '{s3_key}' from '{source_bucket}' to '{destination_bucket}': {e}")
        raise e

