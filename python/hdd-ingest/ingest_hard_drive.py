import argparse
import hashlib
import io
import json
import os
import sys
import uuid
from pathlib import Path

import boto3
import pandas
import pandas as pd
from botocore.config import Config

config = Config(region_name="eu-west-2")

import discovery_client
import dataset_validator
from dataset_validator import Js8Validator


def build_argument_parser():
    parser = argparse.ArgumentParser(
        description="Process an input file to schedule corresponding ingests ",
        add_help=False
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="A CSV file containing details of the records to ingest, it must have columns (catRef, fileName, checksum)"
    )
    parser.add_argument(
        "-e", "--environment",
        help="Environment where the ingest is taking place (e.g. INTG or PRD)",
        default="INTG"
    )
    parser.add_argument(
        "-d", "--dry_run",
        help="Value of 'True' indicates that the tool will only validate inputs, without actually running an ingest",
        default=False
    )
    return parser

# Validations of the parameters passed to the script.
def validate_arguments(args):
    input_file_path = Path(args.input)
    if not (input_file_path.exists() and input_file_path.is_file()):
        raise Exception(f"The input file [{input_file_path}] does not exist or it is not a valid file\n")
    else:
        return

def create_metadata(row):
    catalog_ref = row["catRef"].strip()
    file_path = row["fileName"].strip()
    title, description = discovery_client.get_title_and_description(catalog_ref)
    description_to_use = title if title is not None else description
    metadata = {
        "Series": row["catRef"].split("/")[0].strip(),
        "UUID": str(uuid.uuid4()),
        "fileId": str(uuid.uuid4()),
        "description": description_to_use,
        "fileName": file_path.split("\\")[-1].strip(),
        "FileReference": catalog_ref,
        "ClientSideOriginalFilePath": file_path
    }
    sha256_checksum = row["checksum"].strip()
    if not sha256_checksum:
        metadata["checksum_md5"] = create_md5_hash(file_path)
    else:
        metadata["checksum_sha256"] = sha256_checksum
    return metadata

def create_md5_hash(file_path, chunk_size=8192):
    md5 = hashlib.md5()
    with open(file_path, "rb") as the_file:   # open in binary mode
        for chunk in iter(lambda: the_file.read(chunk_size), b""):
            md5.update(chunk)
    return md5.hexdigest()

def upload_files(metadata, file_path, args):
    account_number = os.environ["ACCOUNT_NUMBER"]
    environment = args.environment
    bucket = f"{environment}-dr2-ingest-raw-cache"
    queue_url = f"https://sqs.eu-west-2.amazonaws.com/{account_number}/{environment}-dr2-preingest-dri-importer"

    asset_id = metadata["UUID"]
    file_id = metadata["fileId"]

    s3_client = boto3.client("s3")
    s3_client.upload_file(file_path, bucket, f'{asset_id}/{file_id}')
    json_bytes = io.BytesIO(json.dumps(metadata).encode("utf-8"))
    s3_client.upload_fileobj(json_bytes, bucket, f"{asset_id}.metadata")

    sqs_client = boto3.client("sqs", config=config)
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps({'assetId': asset_id, 'bucket': bucket}))

def main():
    args = build_argument_parser().parse_args()
    validate_arguments(args)

    input_file_path = Path(args.input)
    if input_file_path.suffix.lower() == ".csv":
        # read all values as str and empty values to be kept as str
        data_set = pd.read_csv(input_file_path, dtype=str, keep_default_na=False)
    elif input_file_path.suffix.lower() in [".xls", ".xlsx"]:
        data_set = pd.read_excel(input_file_path)
    else:
        raise Exception("Unsupported input file format. Only CSV and Excel (xls, xlsx) files are supported for input")
    try:
        is_dry_run = False if args.dry_run == "False" else True
        is_valid = dataset_validator.validate_dataset(Js8Validator(), data_set, is_dry_run)
    except Exception as e:
        raise Exception(f"Inputs supplied to the process are invalid, please fix errors before continuing: {e}")

    if is_dry_run:
        if not is_valid:
            print("Please fix errors before continuing")
            sys.exit(1)
        else:
            print("Validation finished successfully. Please proceed with ingest")
            sys.exit(0)
    else:
        data_set: pandas.DataFrame
        for index, row in data_set.iterrows():
            metadata = create_metadata(row)
            file_path = row["fileName"].strip()
            print(f"Uploading {file_path} and corresponding metadata to S3")
            upload_files(metadata, file_path, args)



if __name__ == "__main__":
    main()