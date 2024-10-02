import os
import json
import uuid
from datetime import datetime

import boto3
import botocore
import jsonschema
from botocore.exceptions import ClientError
from samba.dcerpc.dcerpc import response
from jsonschema import validate
from tdr_json_schema import incoming_schema

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

def lambda_handler(event, context):
    destination_bucket = os.environ["DESTINATION_BUCKET"]
    destination_queue = os.environ["DESTINATION_QUEUE"]
    for record in event['Records']:
        body = json.loads(record['body'])
        file_id = body['fileId']
        source_bucket = body['bucket']
        assert_objects_exist(source_bucket, file_id)
        validate_metadata(source_bucket, f"{file_id}.metadata")
        file_location = copy_object_to_s3(destination_bucket, file_id, source_bucket)
        copy_object_to_s3(destination_bucket, f"{file_id}.metadata", source_bucket)
        sqs_body = json.dumps({'id': file_id, 'location': file_location})
        sqs_client.send_message(QueueUrl=destination_queue, MessageBody=sqs_body)

def assert_objects_exist(source_bucket, file_id):
    try:
        s3_client.head_object(source_bucket, file_id)
    except botocore.exceptions.ClientError as ex:
        raise Exception(f"Object {file_id} does not exist, underlying error is: {ex}")

    try:
        s3_client.head_object(source_bucket, f"{file_id}.metadata")
    except botocore.exceptions.ClientError as ex:
        raise Exception(f"Object {file_id}.metadata does not exist, underlying error is: {ex}")


def validate_formats(bucket, s3_key):
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    json_content = json.loads(response['Body'])
    uuid_content = json_content['UUID']
    try:
        uuid_object = uuid.UUID(uuid_content)
    except ValueError:
        raise Exception(f"Unable to parse UUID, '{uuid_content}'. Invalid format")

    transfer_initiated_date_content = json_content['TransferInitiatedDatetime']
    try:
        transfer_initiated_date =  datetime.strptime(transfer_initiated_date_content, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise Exception(f"Unable to parse date, '{transfer_initiated_date_content}'. Invalid format")

    series = json_content['Series']
    if not series.strip():
        raise Exception(f"Empty series value, unable to proceed")

    return True

def validate_metadata(bucket, s3_key):
    validate_mandatory_fields_exist(bucket, s3_key)
    validate_formats(bucket, s3_key)

def validate_mandatory_fields_exist(bucket, s3_key):
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    json_content = response['Body']
    data = json.loads(json_content)
    try:
        validate(data, incoming_schema)
    except jsonschema.exceptions.ValidationError as err:
        raise Exception(err.message)

    return True

def copy_object_to_s3(destination_bucket, s3_key, source_bucket):
    try:
        copy_source = {'Bucket': source_bucket, 'Key': s3_key}
        s3_client.copy(copy_source, destination_bucket, s3_key)
        return f"s3://{destination_bucket}/{s3_key}"
    except Exception as e:
        print(f"Error during copy of {s3_key} from {source_bucket} to {destination_bucket}: {e}")
        raise e
