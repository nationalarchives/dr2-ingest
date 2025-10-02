import argparse
import io
import json
import os.path
import uuid
from pathlib import Path

import boto3
import pandas
import pandas as pd
from botocore.config import Config

config = Config(region_name="eu-west-2")

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs", config=config)


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
        "-d", "--dry-run",
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
        "checksum_sha256": row["checksum"].strip(), # generate if not present?
        "FileReference": catalog_ref,
        "ClientSideOriginalFilePath": file_path
    }
    return metadata

def upload_files(metadata, file_path, args):
    account_number = "123456789" #os.environ["ACCOUNT_NUMBER"]
    environment = args.environment
    bucket = f"{environment}-dr2-ingest-raw-cache"
    queue_url = f"https://sqs.eu-west-2.amazonaws.com/{account_number}/{environment}-dr2-preingest-dri-importer"

    asset_id = metadata["UUID"]
    file_id = metadata["fileId"]

    s3_client.upload_file(file_path, bucket, f'{asset_id}/{file_id}')
    json_bytes = io.BytesIO(json.dumps(metadata).encode("utf-8"))
    s3_client.upload_fileobj(json_bytes, bucket, f"{asset_id}.metadata")

    sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps({'assetId': asset_id, 'bucket': bucket}))



def main():
    args = build_argument_parser().parse_args()
    validate_arguments(args)

    input_file_path = Path(args.input)
    if input_file_path.suffix.lower() == ".csv":
        data_set = pd.read_csv(input_file_path)
    elif input_file_path.suffix.lower() in [".xls", ".xlsx"]:
        data_set = pd.read_excel(input_file_path)
    else:
        raise Exception("Unsupported input file format. Only CSV and Excel (xls, xlsx) files are supported for input")
    try:
        dataset_validator.validate_dataset(Js8Validator(), data_set)
    except Exception as e:
        raise Exception(f"Invalid  input file: {e}")

    data_set: pandas.DataFrame
    for index, row in data_set.iterrows():
        metadata = create_metadata(row)
        file_path = row["fileName"].strip()
        #     # FIXME: in case of dry run, just report and continue checking further, don't raise an exception.
        #     # for regular upload, raise exception
        #     raise Exception(f"The file, '{file_path}' does not exist")
        print(f"Uploading {file_path} and corresponding metadata to S3")
        upload_files(metadata, file_path, args)



if __name__ == "__main__":
    main()