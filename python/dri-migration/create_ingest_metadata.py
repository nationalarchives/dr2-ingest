import io
import json
import re
from os import listdir
import oracledb
import os
import boto3
import hashlib

oracledb.init_oracle_client(lib_dir=os.environ['CLIENT_LOCATION'])
conn = oracledb.connect(dsn='localhost/SDB4', user="STORE", password=os.environ['STORE_PASSWORD'])
cur = conn.cursor()

puid_lookup = {}


def create_skeleton_suite_lookup(prefix):
    path = f"{os.environ['DROID_PATH']}\\{prefix}"

    pattern = re.compile(r'((x-)?fmt-\d{1,5})-signature-id-\d{1,5}(\.[A-Za-z]{1,10})')

    directory_list = listdir(path)

    for name in directory_list:
        match = pattern.search(name)
        if match:
            puid = match.group(1).replace('fmt-', 'fmt/')
            puid_lookup[puid] = {'file_path': f"{path}\\{name}"}


create_skeleton_suite_lookup('fmt')
create_skeleton_suite_lookup('x-fmt')

with open("ingest_query.sql") as query:
    sql = query.read()
    cur.execute(sql)


account_number = os.environ["ACCOUNT_NUMBER"]
environment = os.environ["ENVIRONMENT"]
bucket = f"{environment}-dr2-ingest-raw-cache"
queue_url = f"https://sqs.eu-west-2.amazonaws.com/{account_number}/{environment}-dr2-copy-files-from-dri"

page_size = 100

s3_client = boto3.client("s3")
sqs_client = boto3.client("s3")


def calculate_checksum(file_path: str, algorithm: str) -> str:
    try:
        hasher = getattr(hashlib, algorithm)()
    except AttributeError:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hasher.update(chunk)

    return hasher.hexdigest()


key_indexes = {}
for idx, keys in enumerate(cur.description):
    key_indexes[keys[0]] = idx

while True:
    rows = cur.fetchmany(page_size)
    if not rows:
        break
    for row in rows:
        puid = row[key_indexes["PUID"]]
        asset_uuid = row[key_indexes["UUID"]]
        file_id = row[key_indexes["FILEID"]]
        file_path = row[key_indexes["FILE_PATH"]]
        full_path = row[key_indexes["FULLPATH"]]
        checksum_data = json.loads(row[key_indexes["FIXITIES"]])
        metadata = {
            "Series": row[key_indexes["SERIES"]],
            "UUID": asset_uuid,
            "fileId": file_id,
            "description": row[key_indexes["DESCRIPTION"]],
            "TransferInitiatedDatetime": str(row[key_indexes["TRANSFERINITIATEDDATETIME"]]),
            "ConsignmentReference": row[key_indexes["CONSIGNMENTREFERENCE"]],
            "Filename": row[key_indexes["FILENAME"]],
            "FileReference": row[key_indexes["FILEREFERENCE"]],
            "metadata": str(row[key_indexes["METADATA"]]),
            "ClientSideOriginalFilepath": file_path
        }
        for each_checksum in checksum_data:
            for algorithm in each_checksum:
                file_path = puid_lookup[puid]['file_path']
                algorithm_lower = algorithm.lower().replace("-", "")
                fingerprint = calculate_checksum(file_path, algorithm_lower)
                metadata[f"checksum_{algorithm_lower}"] = fingerprint

        s3_client.upload_file(puid_lookup[puid]['file_path'], bucket, asset_uuid)

        json_bytes = io.BytesIO(json.dumps(metadata).encode("utf-8"))
        s3_client.upload_fileobj(json_bytes, bucket, f"{asset_uuid}.metadata")
        sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps({'fileId': asset_uuid, 'bucket': bucket}))
