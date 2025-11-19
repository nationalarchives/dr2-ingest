import boto3
import os
import json
import jsonschema
from urllib.parse import urlparse
import uuid
import io

sqs_client = boto3.client("sqs")
s3_client = boto3.client("s3")
sts_client = boto3.client("sts")


def lambda_handler(event, context):
    destination_bucket = os.environ["OUTPUT_BUCKET_NAME"]
    destination_queue = os.environ["OUTPUT_QUEUE_URL"]
    files_s3_client = get_files_s3_client(os.environ["ROLE_TO_ASSUME"])
    for record in event["Records"]:
        body: dict[str, str] = json.loads(record["body"])
        metadata_location = body['metadataLocation']
        metadata_uri = urlparse(metadata_location)
        bucket = metadata_uri.netloc
        key = metadata_uri.path[1:]

        files_bucket = os.environ["FILES_BUCKET"]
        json_metadata = get_json_metadata(bucket, key)
        asset_id = json_metadata[0]['UUID']
        for metadata in json_metadata:
            file_id = metadata['fileId']

            validate_mandatory_fields_exist(f"common/preingest-pa/metadata-schema.json", metadata)
            validate_formats(metadata, bucket, key)
            files_copy_source = {"Bucket": files_bucket, "Key": file_id}
            files_s3_client.copy(files_copy_source, destination_bucket, f"{asset_id}/{file_id}")
            process_series_and_catalogue_reference(metadata)

        metadata_body = io.BytesIO(json.dumps(json_metadata).encode("utf-8"))
        s3_client.upload_fileobj(metadata_body, destination_bucket, f"{asset_id}.metadata")
        sqs_body = {"id": asset_id, "location": f"s3://{destination_bucket}/{asset_id}.metadata"}
        sqs_client.send_message(QueueUrl=destination_queue, MessageBody=json.dumps(sqs_body))


def process_series_and_catalogue_reference(metadata):
    series = metadata["Series"]
    file_reference = metadata["FileReference"]
    metadata["FileReference"] = modify_reference(file_reference)
    metadata["Series"] = modify_reference(series)

def modify_reference(field):
    if " " in field:
        field_elements = field.split(" ")
    else:
        field_elements = field.split("/")
    first_element = field_elements[0]
    if len(first_element) == 4:
        first_element = first_element[:-1]
    field_elements[0] = f"Y{first_element}"
    return "/".join(field_elements)


def get_json_metadata(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    json_metadata = json.loads(response['Body'].read().decode('utf-8'))
    return json_metadata


def get_files_s3_client(role_to_assume):
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_to_assume,
        RoleSessionName="ParliamentImport"
    )
    credentials = assumed_role_object['Credentials']
    return boto3.client("s3",
                        aws_access_key_id=credentials['AccessKeyId'],
                        aws_secret_access_key=credentials['SecretAccessKey'],
                        aws_session_token=credentials['SessionToken'])


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

