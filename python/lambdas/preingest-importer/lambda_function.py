import io
import itertools
import json
import os
import re
import uuid

import boto3
import jsonschema
from urllib import parse

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


def lambda_handler(event, context):
    destination_bucket = os.environ["OUTPUT_BUCKET_NAME"]
    destination_queue = os.environ["OUTPUT_QUEUE_URL"]
    delete_from_source = os.getenv("DELETE_FROM_SOURCE", "false") == "true"
    skip_validation = os.getenv("SKIP_VALIDATION", "false") == "true"
    records_metadata_bucket = os.getenv("RECORDS_METADATA_BUCKET")
    for record in event["Records"]:
        body: dict[str, str] = json.loads(record["body"])
        asset_id = body["assetId"] if "assetId" in body else body["fileId"]
        parsed_metadata_url = parse.urlparse(body["metadataLocation"])
        metadata_file_id = parsed_metadata_url.path[1:]
        metadata_source_bucket = parsed_metadata_url.netloc
        files_source_bucket = body["bucket"]
        files_prefix = body.get("filesPrefix", asset_id)
        response = s3_client.get_object(Bucket=metadata_source_bucket, Key=metadata_file_id)
        json_metadata = json.loads(response['Body'].read().decode('utf-8'))
        try:
            if not skip_validation:
                json_metadata = validate_metadata(json_metadata)
                if records_metadata_bucket:
                    copy_records_metadata(metadata_source_bucket, records_metadata_bucket, json_metadata, metadata_file_id)
            transfer_files = [f"{files_prefix}/{m['fileId']}" for m in json_metadata]
            copy_objects(destination_bucket, transfer_files, files_source_bucket)
            copy_objects(destination_bucket, [metadata_file_id], metadata_source_bucket)
            potential_message_id = {key: value for key, value in body.items() if key == "messageId"}
            sqs_body = {"id": asset_id, "location": f"s3://{destination_bucket}/{metadata_file_id}", "filesPrefix": files_prefix}
            sqs_body.update(potential_message_id)
            if delete_from_source:
                for batch in itertools.batched(transfer_files, 1000):
                    keys_to_delete = [{'Key': key} for key in batch]
                    s3_client.delete_objects(Bucket=files_source_bucket, Delete={'Objects': keys_to_delete})
            sqs_client.send_message(QueueUrl=destination_queue, MessageBody=json.dumps(sqs_body))
        except Exception as e:
            print(json.dumps({"error": str(e), "assetId": asset_id}))
            raise Exception(e)


def copy_records_metadata(source_bucket, records_metadata_bucket, json_metadata, metadata_file_key):
    for metadata in json_metadata:
        series = metadata["Series"]
        file_reference = metadata["FileReference"].replace("/", "-")
        key = f"live/{series}-{file_reference}.json"
        response = s3_client.get_object(Bucket=records_metadata_bucket, Key=key)
        migrated_metadata = json.loads(response['Body'].read().decode('utf-8'))
        metadata["migratedMetadata"] = migrated_metadata
    json_bytes = io.BytesIO(json.dumps(json_metadata).encode("utf-8"))
    s3_client.upload_fileobj(json_bytes, source_bucket, metadata_file_key)


def validate_metadata(json_metadata):
    source_system = os.environ["SOURCE_SYSTEM"]

    for metadata in json_metadata:
        validate_mandatory_fields_exist(f"common/preingest-{source_system}/metadata-schema.json", metadata)
        validate_formats(metadata)
    return json_metadata


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


def validate_formats(json_metadata):
    uuid_content = json_metadata["UUID"]
    try:
        uuid.UUID(uuid_content)
    except ValueError:
        raise Exception(
            f"Unable to parse UUID, '{uuid_content}' from file. Invalid format")

    series = json_metadata["Series"]
    if not series.strip():
        raise Exception("Empty Series value in file. Unable to proceed")

    pattern = "^([A-Z]{1,4} [1-9][0-9]{0,3}|Unknown|MOCK1 123)$"
    match = re.match(pattern, series)
    if not match:
        raise Exception(f"Invalid Series value, '{series}' in file. Unable to proceed")

    return True


def copy_objects(destination_bucket, s3_keys, source_bucket):
    for s3_key in s3_keys:
        try:
            copy_source = {"Bucket": source_bucket, "Key": s3_key}
            s3_client.copy(copy_source, destination_bucket, s3_key)
        except Exception as e:
            print(f"Error during copy of '{s3_key}' from '{source_bucket}' to '{destination_bucket}': {e}")
            raise e
