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

        sqs_client.send_message(QueueUrl=destination_queue, MessageBody=json.dumps({'location': file_location}))


def copy_object_to_s3(destination_bucket, s3_key, source_bucket):
    try:
        response = s3_client.head_object(Bucket=source_bucket, Key=s3_key)
        file_size = response['ContentLength']

        if file_size >= 5 * 1024 * 1024 * 1024:
            multipart_copy(source_bucket, destination_bucket, s3_key, file_size)
        else:
            standard_copy(source_bucket, destination_bucket, s3_key)

        return f"s3://{destination_bucket}/{s3_key}"
    except Exception as e:
        print(f"Error processing file {s3_key} from bucket {source_bucket}: {e}")
        raise e


def standard_copy(source_bucket, destination_bucket, s3_key):
    try:
        copy_source = {'Bucket': source_bucket, 'Key': s3_key}
        s3_client.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=s3_key)
        print(f"Standard copy of {s3_key} completed successfully.")
    except Exception as e:
        print(f"Error during standard copy of {s3_key}: {e}")
        raise e


def multipart_copy(source_bucket, destination_bucket, s3_key, file_size):
    multipart_upload = s3_client.create_multipart_upload(Bucket=destination_bucket, Key=s3_key)
    upload_id = multipart_upload['UploadId']
    try:
        copy_source = {'Bucket': source_bucket, 'Key': s3_key}
        part_size = int(file_size / 100)
        part_number = 1
        parts = []

        for offset in range(0, file_size, part_size):
            end = min(offset + part_size, file_size)
            part = s3_client.upload_part_copy(
                Bucket=destination_bucket,
                Key=s3_key,
                PartNumber=part_number,
                UploadId=upload_id,
                CopySourceRange=f'bytes={offset}-{end - 1}',
                CopySource=copy_source
            )
            parts.append({'PartNumber': part_number, 'ETag': part['CopyPartResult']['ETag']})
            part_number += 1

        # Complete the multipart upload
        s3_client.complete_multipart_upload(
            Bucket=destination_bucket,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        print(f"Multipart copy of {s3_key} completed successfully.")
    except Exception as e:
        print(f"Error during multipart copy of {s3_key}: {e}")
        # If there is an error, abort the multipart upload
        s3_client.abort_multipart_upload(Bucket=destination_bucket, Key=s3_key, UploadId=upload_id)
        raise e
