import io
import json
import re
from collections import defaultdict
from os import listdir
import oracledb
import os
import boto3
from botocore.config import Config
import hashlib
import uuid


def create_skeleton_suite_lookup(prefixes):
    puid_lookup = {}
    for prefix in prefixes:
        path = os.path.join(os.environ['DROID_PATH'], prefix)

        pattern = re.compile(r'((x-)?fmt-\d{1,5})-.*')

        directory_list = listdir(path)

        for name in directory_list:
            match = pattern.search(name)
            if match:
                puid = match.group(1).replace(f'{prefix}-', f'{prefix}/')
                puid_lookup[puid] = {'file_path': os.path.join(path, name)}

    return puid_lookup


page_size = 100

assets = []

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


def group_assets(assets_list):
    grouped = defaultdict(list)
    for asset in assets_list:
        grouped[asset['metadata']['UUID']].append(asset)
    return dict(grouped)


def process_redacted():
    grouped_assets = group_assets(assets)
    for asset_uuid, assets_for_uuid in grouped_assets.items():
        if len(assets_for_uuid) > 1:
            for redacted_asset in assets_for_uuid:
                if redacted_asset['type_ref'] == 100:
                    file_reference = redacted_asset['metadata']['FileReference']
                    rel_ref = redacted_asset['rel_ref']
                    redacted_asset['metadata']['UUID'] = str(uuid.uuid4())
                    redacted_asset['metadata']['FileReference'] = f"{file_reference}/{rel_ref - 1}"
    return [asset for assets_for_uuid in grouped_assets.values() for asset in assets_for_uuid]


def migrate():
    account_number = os.environ["ACCOUNT_NUMBER"]
    environment = os.environ["ENVIRONMENT"]
    bucket = f"{environment}-dr2-ingest-raw-cache"
    queue_url = f"https://sqs.eu-west-2.amazonaws.com/{account_number}/{environment}-dr2-copy-files-from-dri"

    puid_lookup = create_skeleton_suite_lookup(['fmt', 'x-fmt'])
    oracledb.defaults.fetch_lobs = False
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
            consignment_reference = row[column_indexes["CONSIGNMENTREFERENCE"]]
            dri_batch_reference = row[column_indexes["DRIBATCHREFERENCE"]]
            rel_ref = row[column_indexes["MANIFESTATIONRELREF"]]
            type_ref = row[column_indexes["TYPEREF"]]
            description_one = row[column_indexes["DESC1"]]
            description_two = row[column_indexes["DESC2"]]
            description = description_one if description_one else description_two
            metadata = {
                "Series": row[column_indexes["SERIES"]],
                "UUID": asset_uuid,
                "fileId": file_id,
                "description": description,
                "TransferInitiatedDatetime": str(row[column_indexes["TRANSFERINITIATEDDATETIME"]]),
                "Filename": row[column_indexes["FILENAME"]],
                "FileReference": row[column_indexes["FILEREFERENCE"]],
                "metadata": str(row[column_indexes["METADATA"]]),
                "ClientSideOriginalFilepath": file_path
            }
            if consignment_reference:
                metadata["ConsignmentReference"] = consignment_reference
            if dri_batch_reference:
                metadata["driBatchReference"] = dri_batch_reference

            if not consignment_reference and not dri_batch_reference:
                raise ValueError("We need either a consignment reference or a dri batch reference")

            file_path = puid_lookup[puid]['file_path']
            for each_checksum in checksums:
                for algorithm in each_checksum:
                    algorithm_lower = algorithm.lower().replace("-", "")
                    fingerprint = calculate_checksum(file_path, algorithm_lower)
                    metadata[f"checksum_{algorithm_lower}"] = fingerprint

            assets.append({'file_path': file_path, 'metadata': metadata, 'rel_ref': rel_ref, 'type_ref': type_ref})

    assets_with_redacted = process_redacted()

    grouped_assets = group_assets(assets_with_redacted)

    all_sqs_messages = []

    for asset_uuid, assets_list in grouped_assets.items():
        all_metadata = []
        for asset in assets_list:
            file_path = asset['file_path']
            metadata = asset['metadata']
            file_id = metadata['fileId']
            all_metadata.append(metadata)
            s3_client.upload_file(file_path, bucket, f'{asset_uuid}/{file_id}')

        json_bytes = io.BytesIO(json.dumps(all_metadata).encode("utf-8"))
        s3_client.upload_fileobj(json_bytes, bucket, f"{asset_uuid}.metadata")
        all_sqs_messages.append(json.dumps({'assetId': asset_uuid, 'bucket': bucket}))

    for sqs_message in all_sqs_messages:
        sqs_client.send_message(QueueUrl=queue_url, MessageBody=sqs_message)



if __name__ == "__main__":
    migrate()