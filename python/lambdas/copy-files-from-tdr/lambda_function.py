import json
import os
import re
import uuid

import boto3
import botocore
import jsonschema
from botocore.exceptions import ClientError
from jsonschema import validate

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')


def lambda_handler(event, context):
    destination_bucket = os.environ["DESTINATION_BUCKET"]
    destination_queue = os.environ["DESTINATION_QUEUE"]
    for record in event['Records']:
        body = json.loads(record['body'])
        file_id = body['fileId']
        metadata_file_id = f"{file_id}.metadata"
        source_bucket = body['bucket']
        files = [file_id, metadata_file_id]
        assert_objects_exist_in_bucket(source_bucket, files)
        validate_metadata(source_bucket, metadata_file_id)
        file_location = copy_objects(destination_bucket, file_id, source_bucket)
        copy_objects(destination_bucket, metadata_file_id, source_bucket)
        sqs_body = json.dumps({'id': file_id, 'location': file_location})
        sqs_client.send_message(QueueUrl=destination_queue, MessageBody=sqs_body)


def assert_objects_exist_in_bucket(source_bucket, files):
    for file in files:
        try:
            s3_client.head_object(source_bucket, file)
        except botocore.exceptions.ClientError as ex:
            raise Exception(f"Object '{file}' does not exist in '{source_bucket}', underlying error is: '{ex}'")


def validate_metadata(bucket, s3_key):
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    validate_mandatory_fields_exist(response)
    validate_formats(response, bucket, s3_key)


def validate_mandatory_fields_exist(get_object_response):
    json_metadata = json.loads(get_object_response['Body'])
    try:
        validate(json_metadata, metadata_schema)
    except jsonschema.exceptions.ValidationError as err:
        raise Exception(err.message)

    return True


def validate_formats(get_object_response, bucket, s3_key):
    json_metadata = json.loads(get_object_response['Body'])
    uuid_content = json_metadata['UUID']
    try:
        uuid.UUID(uuid_content)
    except ValueError:
        raise Exception(
            f"Unable to parse UUID, '{uuid_content}' from file '{s3_key}' in bucket '{bucket}'. Invalid format")

    series = json_metadata['Series']
    if not series.strip():
        raise Exception(f"Empty Series value in file '{s3_key}' in bucket '{bucket}'. Unable to proceed")

    #FIXME, Can we use this regex pattern? The test series MOCK1 123 fails this pattern
    # pattern = "^([A-Z]{1,4} [1-9][0-9]{0,3})"
    # match = re.match(pattern, series)
    # if not match:
    #     raise Exception("FIXME")

    return True


def copy_objects(destination_bucket, s3_key, source_bucket):
    try:
        copy_source = {'Bucket': source_bucket, 'Key': s3_key}
        s3_client.copy(copy_source, destination_bucket, s3_key)
        return f"s3://{destination_bucket}/{s3_key}"
    except Exception as e:
        print(f"Error during copy of {s3_key} from {source_bucket} to {destination_bucket}: {e}")
        raise e


metadata_schema = {
    "type": "object",
    "properties": {
        "Series": {"type": "string"},
        "UUID": {"type": "string"},
        "TransferInitiatedDatetime": {"type": "string"}
    },
    "required": ["Series", "UUID", "TransferInitiatedDatetime"],
}
