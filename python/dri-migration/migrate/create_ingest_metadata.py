import io
import json
import re
from os import listdir
import oracledb
import os
import boto3
from botocore.config import Config
import hashlib


def create_skeleton_suite_lookup(prefixes):
    puid_lookup = {}
    for prefix in prefixes:
        path = f"{os.environ['DROID_PATH']}\\{prefix}"

        pattern = re.compile(r'((x-)?fmt-\d{1,5})-signature-id-\d{1,5}(\.[A-Za-z]{1,10})')

        directory_list = listdir(path)

        for name in directory_list:
            match = pattern.search(name)
            if match:
                puid = match.group(1).replace(f'{prefix}-', f'{prefix}/')
                puid_lookup[puid] = {'file_path': f"{path}\\{name}"}

    return puid_lookup


page_size = 100

config = Config(region_name="eu-west-2")

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs", config=config)


def calculate_checksum(file_path: str, algorithm: str) -> str:
    try:
        hasher = getattr(hashlib, algorithm)()
    except AttributeError:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hasher.update(chunk)

    return hasher.hexdigest()


def if_none_empty(value):
    return value if value else ''


def migrate():
    account_number = os.environ["ACCOUNT_NUMBER"]
    environment = os.environ["ENVIRONMENT"]
    bucket = f"{environment}-dr2-ingest-raw-cache"
    queue_url = f"https://sqs.eu-west-2.amazonaws.com/{account_number}/{environment}-dr2-copy-files-from-dri"

    puid_lookup = create_skeleton_suite_lookup(['fmt', 'x-fmt'])

    oracledb.init_oracle_client(lib_dir=os.environ['CLIENT_LOCATION'])
    conn = oracledb.connect(dsn='localhost/SDB4', user="STORE", password=os.environ['STORE_PASSWORD'])
    cur = conn.cursor()

    with open("ingest_query.sql") as query:
        sql = query.read()
        cur.execute(sql)
    column_indexes = {keys[0]: idx for idx, keys in enumerate(cur.description)}

    while True:
        rows = cur.fetchmany(page_size)
        if not rows:
            break
        for row in rows:
            puid = row[column_indexes["PUID"]]
            asset_uuid = row[column_indexes["UUID"]]
            file_id = row[column_indexes["FILEID"]]
            file_path = row[column_indexes["FILE_PATH"]]
            checksums = json.loads(row[column_indexes["FIXITIES"]])
            metadata = {
                "Series": row[column_indexes["SERIES"]],
                "UUID": asset_uuid,
                "fileId": file_id,
                "description": row[column_indexes["DESCRIPTION"]],
                "TransferInitiatedDatetime": str(row[column_indexes["TRANSFERINITIATEDDATETIME"]]),
                "ConsignmentReference": if_none_empty(row[column_indexes["CONSIGNMENTREFERENCE"]]),
                "driBatchReference": if_none_empty(row[column_indexes["DRIBATCHREFERENCE"]]),
                "Filename": row[column_indexes["FILENAME"]],
                "FileReference": row[column_indexes["FILEREFERENCE"]],
                "metadata": str(row[column_indexes["METADATA"]]),
                "ClientSideOriginalFilepath": file_path
            }
            file_path = puid_lookup[puid]['file_path']
            for each_checksum in checksums:
                for algorithm in each_checksum:
                    algorithm_lower = algorithm.lower().replace("-", "")
                    fingerprint = calculate_checksum(file_path, algorithm_lower)
                    metadata[f"checksum_{algorithm_lower}"] = fingerprint

            s3_client.upload_file(file_path, bucket, asset_uuid)

            json_bytes = io.BytesIO(json.dumps(metadata).encode("utf-8"))
            s3_client.upload_fileobj(json_bytes, bucket, f"{asset_uuid}.metadata")
            sqs_client.send_message(QueueUrl=queue_url,
                                    MessageBody=json.dumps({'fileId': asset_uuid, 'bucket': bucket}))


if __name__ == "__main__":
    migrate()
