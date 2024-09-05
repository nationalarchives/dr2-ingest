import os
import json
import boto3

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')


def lambda_handler(event, context):
    destination_bucket = os.environ["DESTINATION_BUCKET"]
    destination_queue = os.environ["DESTINATION_QUEUE"]
    for record in event['Records']:
        body = json.loads(record['body'])
        file_id = body['fileId']
        source_bucket = body['bucket']
        file_location = copy_object_to_s3(destination_bucket, file_id, source_bucket)
        copy_object_to_s3(destination_bucket, f"{file_id}.metadata", source_bucket)
        sqs_body = json.dumps({'id': file_id, 'location': file_location})
        sqs_client.send_message(QueueUrl=destination_queue, MessageBody=sqs_body)


def copy_object_to_s3(destination_bucket, s3_key, source_bucket):
    try:
        copy_source = {'Bucket': source_bucket, 'Key': s3_key}
        s3_client.copy(copy_source, destination_bucket, s3_key)
        return f"s3://{destination_bucket}/{s3_key}"
    except Exception as e:
        print(f"Error during copy of {s3_key}: {e}")
        raise e
