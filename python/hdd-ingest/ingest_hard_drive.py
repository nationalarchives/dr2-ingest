import argparse
import hashlib
import io
import json
import os
import sys
import uuid
from pathlib import Path, PureWindowsPath, PurePosixPath

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

    if not title and not description:
        raise Exception(f"Title and Description both are empty for '{catalog_ref}', unable to proceed with this record")

    description_to_use = title if title is not None else description
    series = row["catRef"].split("/")[0].strip()
    metadata = {
        "Series": series,
        "UUID": str(uuid.uuid4()),
        "fileId": str(uuid.uuid4()),
        "description": description_to_use,
        "Filename": get_filename_from_cross_platform_path(file_path),
        "FileReference": catalog_ref.removeprefix(f"{series}/"),
        "ClientSideOriginalFilepath": file_path
    }
    sha256_checksum = row["checksum"].strip()
    if not sha256_checksum:
        metadata["checksum_md5"] = create_md5_hash(file_path)
    else:
        metadata["checksum_sha256"] = sha256_checksum
    return metadata

def get_filename_from_cross_platform_path(path_str):
    if "\\" in path_str or ":" in path_str: #maybe Windows path
        return PureWindowsPath(path_str).name.strip()
    else :
        return PurePosixPath(path_str).name.strip()

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
    queue_url = f"https://sqs.eu-west-2.amazonaws.com/{account_number}/{environment}-dr2-preingest-hdd-importer"

    asset_id = metadata["UUID"]
    file_id = metadata["fileId"]
    print(f"Asset ID: {asset_id} and File ID: {file_id}")
    s3_client = boto3.client("s3")
    s3_client.upload_file(get_absolute_file_path(args.input, file_path), bucket, f'{asset_id}/{file_id}')
    json_bytes = io.BytesIO(json.dumps([metadata]).encode("utf-8"))
    s3_client.upload_fileobj(json_bytes, bucket, f"{asset_id}.metadata")

    sqs_client = boto3.client("sqs", config=config)
    sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps({'assetId': asset_id, 'bucket': bucket}))

# the path in the input file may be relative to the input csv
def get_absolute_file_path(input_path, relative_or_absolute_file_path):
    input_file_path = Path(input_path).resolve()

    if Path(relative_or_absolute_file_path).is_absolute():
        return str(relative_or_absolute_file_path)
    else:
        return str((input_file_path.parent / relative_or_absolute_file_path).resolve())

def run_ingest(data_set, args, is_upstream_valid):
    data_set: pandas.DataFrame
    is_dry_run = False if args.dry_run == "False" else True

    is_discovery_available = discovery_client.is_discovery_api_reachable()
    if not is_discovery_available:
        print("Discovery API is not available for getting metadata information, terminating process")
        sys.exit(1)

    is_metadata_valid = is_upstream_valid
    for index, row in data_set.iterrows():
        try:
            metadata = create_metadata(row)
            if not is_dry_run:  # upload the files to s3 and send message only if it is not a dry run
                file_path = row["fileName"].strip()
                print(f"Uploading {file_path} and corresponding metadata to S3")
                upload_files(metadata, file_path, args)
        except Exception as e:
            is_metadata_valid = False
            print(f"Error creating metadata: {e}")
            if not is_dry_run:
                sys.exit(1)


    if is_dry_run:
        if is_metadata_valid:
            print("Validations completed successfully, please proceed to ingest")
        else:
            print("Please fix the errors identified during validation before continuing further")
            sys.exit(1)


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

    is_valid = True
    is_dry_run = False if args.dry_run == "False" else True
    try:
        is_valid = dataset_validator.validate_dataset(Js8Validator(), data_set, str(input_file_path), is_dry_run)
    except Exception as e:
        raise Exception(f"Inputs supplied to the process are invalid, please fix errors before continuing: {e}")

    run_ingest(data_set, args, is_valid)

    if not is_dry_run:
        print("Upload finished successfully")

if __name__ == "__main__":
    main()