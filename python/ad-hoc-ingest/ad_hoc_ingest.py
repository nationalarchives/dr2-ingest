import argparse
import csv
import hashlib
import os
import sys
import tempfile
import uuid
from datetime import datetime
from pathlib import Path, PureWindowsPath, PurePosixPath

import pandas
import pandas as pd
from botocore.exceptions import ClientError
from moto.utilities.utils import str2bool

import aws_interactions
import dataset_validator
import discovery_client
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
        help="Environment where the ingest is taking place (e.g. intg or prd)",
        default="intg"
    )
    parser.add_argument(
        "-d", "--dry_run",
        nargs="?",
        const=True,
        type=str2bool,
        help="Value of 'True' indicates that the tool will only validate inputs, without actually running an ingest",
        default=False
    )
    parser.add_argument(
        "-o", "--output",
        help="Name of the folder to store a CSV file representing generated metadata for this ingest",
        default=tempfile.gettempdir()
    )
    return parser

# Validations of the parameters passed to the script.
def validate_arguments(args):
    input_file_path = Path(args.input)
    if not (input_file_path.exists() and input_file_path.is_file()):
        raise Exception(f"The input file [{input_file_path}] does not exist or it is not a valid file\n")

    output_metadata_folder = Path(args.output)
    if not (output_metadata_folder.exists() and output_metadata_folder.is_dir()):
        raise Exception(f"The output metadata location [{output_metadata_folder}] does not exist or it is not a valid folder\n")

    return True

def create_metadata(row, args):
    catalog_ref = row["catRef"].strip()
    file_path = row["fileName"].strip()
    collection_info = discovery_client.get_title_and_description(catalog_ref)
    former_references = discovery_client.get_former_references(collection_info.identifier)

    if not collection_info.title and not collection_info.description:
        raise Exception(f"Title and Description both are empty for '{catalog_ref}', unable to proceed with this record")

    description_to_use = collection_info.title if collection_info.title is not None else collection_info.description
    series = row["catRef"].split("/")[0].strip()
    metadata = {
        "Series": series,
        "UUID": str(uuid.uuid4()),
        "fileId": str(uuid.uuid4()),
        "description": description_to_use,
        "Filename": get_filename_from_cross_platform_path(file_path),
        "FileReference": catalog_ref.removeprefix(f"{series}/"),
        "ClientSideOriginalFilepath": file_path,
    }
    former_ref_dept = former_references.formerRefDept
    if former_ref_dept is None:
        metadata["formerRefDept"] = ""
    else:
        metadata["formerRefDept"] = former_ref_dept

    former_ref_tna = former_references.formerRefTNA
    if former_ref_tna is None:
        metadata["formerRefTNA"] = ""
    else:
        metadata["formerRefTNA"] = former_ref_tna


    sha256_checksum = row["checksum"].strip()
    if not sha256_checksum:
        metadata["checksum_md5"] = create_md5_hash(get_absolute_file_path(args.input, file_path))
        metadata["checksum_sha256"] = ""
    else:
        metadata["checksum_md5"] = ""
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

def get_confirmation_to_proceed(prompt="Are you sure?"):
    confirmation = input(f"{prompt}").strip().lower()
    if confirmation in ("y", "yes"):
        return True
    else:
        return False

def upload_files(output_file, account_number, args):
    environment = args.environment
    bucket = f"{environment}-dr2-ingest-raw-cache"
    queue_url = f"https://sqs.eu-west-2.amazonaws.com/{account_number}/{environment}-dr2-preingest-adhoc-importer"

    upload_data_set = pd.read_csv(output_file, dtype=str, keep_default_na=False)
    total = len(upload_data_set)

    for counter, (index, row) in enumerate(upload_data_set.iterrows(), start=1):
        asset_id = row["UUID"]
        file_id = row["fileId"]
        client_side_path = row["ClientSideOriginalFilepath"]
        metadata = {
            "Series": row["Series"],
            "UUID": asset_id,
            "fileId": file_id,
            "description": row["description"],
            "Filename": row["Filename"],
            "FileReference": row["FileReference"],
            "ClientSideOriginalFilepath": client_side_path
        }
        if row["formerRefDept"] != "":
            metadata["formerRefDept"] = row["formerRefDept"]
        if row["formerRefTNA"] != "":
            metadata["formerRefTNA"] = row["formerRefTNA"]
        if row["checksum_sha256"] == "":
            metadata["checksum_md5"] = row["checksum_md5"]
        else:
            metadata["checksum_sha256"] = row["checksum_sha256"]

        for attempt in range(0,4):
            try:
                aws_interactions.upload_file(asset_id, bucket, file_id, get_absolute_file_path(args.input, client_side_path))
                aws_interactions.upload_metadata(asset_id, bucket, metadata)
                aws_interactions.send_message(asset_id, bucket, queue_url)
                break
            except ClientError as client_error:
                if attempt == 3:
                    print(f"Exceeded, number of attempts to recover from error, terminating at file: '{file_id}' from location: '{client_side_path}'")
                    raise Exception(f"Unable to proceed because: {client_error}. Terminating the process.")
                else:
                    print(f"An error caused due to: {client_error}")
                    input("Fix the error and press any key to continue")
                    aws_interactions.refresh_session()

        if counter % 10 == 0:
            print(f"Uploaded ${counter} of ${total}")

# the path in the input file may be relative to the input csv
def get_absolute_file_path(input_path, relative_or_absolute_file_path):
    input_file_path = Path(input_path).resolve()

    if Path(relative_or_absolute_file_path).is_absolute():
        return str(relative_or_absolute_file_path)
    else:
        normalised_relative_path = os.path.normpath(relative_or_absolute_file_path.replace("\\", "/"))
        full_path = Path(input_file_path.parent / normalised_relative_path).resolve()
        return str(full_path)

def is_folder_writable(output_folder):
    try:
        testfile = tempfile.TemporaryFile(dir=output_folder)
        testfile.close()
        return True
    except (OSError, PermissionError):
        return False


def run_ingest(data_set, args, is_upstream_valid):
    data_set: pandas.DataFrame
    is_dry_run = False if args.dry_run == False else True

    is_discovery_available = discovery_client.is_discovery_api_reachable()
    if not is_discovery_available:
        print("Discovery API is not available for getting metadata information, terminating process")
        sys.exit(1)

    output_folder = args.output
    if not is_folder_writable(output_folder):
        print(f"Unable to write to the output location: '{output_folder}', please make sure that you have necessary permissions for that folder")
        sys.exit(1)

    prefix = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_folder, f"{prefix}_proposed_ingest.csv")

    row_count = 0
    with open(f"{output_file}", mode="a", newline="", encoding="utf-8") as metadata_csv:
        is_metadata_valid = is_upstream_valid
        fieldnames=["Series", "UUID", "fileId", "description", "Filename", "FileReference", "ClientSideOriginalFilepath", "formerRefDept", "formerRefTNA", "checksum_md5", "checksum_sha256"]
        writer = csv.DictWriter(metadata_csv, fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for index, row in data_set.iterrows():
            row_count += 1
            try:
                metadata = create_metadata(row, args)
                writer.writerow(metadata)
                if row_count % 100 == 0:
                    metadata_csv.flush()
            except Exception as e:
                is_metadata_valid = False
                print(f"Error creating metadata: {e}")
                if not is_dry_run:
                    sys.exit(1)

        metadata_csv.flush()

    if is_dry_run:
        if is_metadata_valid:
            print("Validations completed successfully, please proceed to ingest")
        else:
            print("Please fix the errors identified during validation before continuing further")
            sys.exit(1)
    else:
        print(f"The metadata to be uploaded is saved to '{output_file}'.")
        try:
            account_number = get_account_number()
            confirmation = get_confirmation_to_proceed(
                f"Uploading {row_count} records to account: {account_number}, Continue? [y/n]: ")
            if confirmation:
                upload_files(output_file, account_number, args)
            else:
                sys.exit(0)
        except Exception as e:
            print(e)
            sys.exit(1)

def get_account_number():
    account_number = ""
    for attempt in range(0, 4):
        try:
            account_number = aws_interactions.get_account_number()
            break
        except ClientError as client_error:
            if attempt == 3:
                raise Exception(f"Unable to proceed because: {client_error}. Terminating the process.")
            else:
                print(f"An error caused due to: {client_error}")
                input("Fix the error and press any key to continue")
                aws_interactions.refresh_session()
    return account_number

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
    is_dry_run = False if args.dry_run == False else True
    try:
        is_valid = dataset_validator.validate_dataset(Js8Validator(), data_set, str(input_file_path), is_dry_run)
    except Exception as e:
        raise Exception(f"Inputs supplied to the process are invalid, please fix errors before continuing: {e}")

    run_ingest(data_set, args, is_valid)

    if not is_dry_run:
        print("Upload finished successfully")

if __name__ == "__main__":
    main()