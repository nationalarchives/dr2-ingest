import csv
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas
import pandas as pd
from botocore.exceptions import ClientError

import argument_parser_builder
import aws_interactions
import dataset_validator
import discovery_client
import metadata_creator
import message_printer


def validate_arguments(args):
    input_file_path = Path(args.input)
    if not (input_file_path.exists() and input_file_path.is_file()):
        raise Exception(f"Either the input file [{input_file_path}] does not exist or it is not a valid file\n")

    output_metadata_folder = Path(args.output)
    if not (output_metadata_folder.exists() and output_metadata_folder.is_dir()):
        raise Exception(f"Either the output metadata location [{output_metadata_folder}] does not exist or it is not a valid folder\n")

def upload_files(output_file, account_number, args):
    environment = args.environment
    region = aws_interactions.get_region()
    bucket = f"{environment}-dr2-ingest-adhoc-cache"
    queue_url = f"https://sqs.{region}.amazonaws.com/{account_number}/{environment}-dr2-preingest-adhoc-importer"

    upload_data_set = pd.read_csv(output_file, dtype=str, keep_default_na=False)
    total = len(upload_data_set)

    for counter, (index, row) in enumerate(upload_data_set.iterrows(), start=1):
        asset_id = row["UUID"]
        file_id = row["fileId"]
        client_side_path = row["ClientSideOriginalFilepath"]
        metadata = metadata_creator.create_metadata_for_upload(row)

        for attempt in range(0,4):
            try:
                aws_interactions.upload_file(asset_id, bucket, file_id, metadata_creator.get_absolute_file_path(args.input, client_side_path))
                aws_interactions.upload_metadata(asset_id, bucket, metadata)
                aws_interactions.send_sqs_message(asset_id, bucket, queue_url)
                break
            except ClientError as client_error:
                if attempt == 3:
                    message_printer.message(f"Exceeded number of attempts to recover from error; terminating at file: '{file_id}' from location: '{client_side_path}'")
                    raise Exception(f"Unable to proceed because: {client_error}. Terminating the process.")
                else:
                    message_printer.message(f"An error occurred due to: {client_error}")
                    input("Fix the error and press 'Enter' to continue")
                    aws_interactions.refresh_session()

        if counter % 5 == 0:
            message_printer.progress(f"Uploaded ${counter} of ${total}")


def upload_files_to_ingest_bucket(data_set, args, is_upstream_valid):
    data_set: pandas.DataFrame
    output_folder = args.output
    if not is_folder_writable(output_folder):
        message_printer.message(f"Unable to write to the output location: '{output_folder}', please make sure that you have necessary permissions for that folder")
        sys.exit(1)

    prefix = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_metadata_file = os.path.join(output_folder, f"{prefix}_proposed_ingest.csv")
    is_metadata_valid, row_count = write_intermediate_csv(args, data_set, is_upstream_valid, output_metadata_file)

    if args.dry_run:
        if is_metadata_valid:
            message_printer.message("Validations completed successfully, please proceed to ingest")
        else:
            message_printer.message("Please fix the errors identified during validation before continuing further")
            sys.exit(1)
    else:
        message_printer.message(f"The metadata to be uploaded is saved to '{output_metadata_file}'.")
        try:
            account_number = get_account_number()
            confirmation = get_confirmation_to_proceed(
                f"Uploading {row_count} records to environment: '{args.environment}', Continue? [y/n]: ")
            if confirmation:
                upload_files(output_metadata_file, account_number, args)
            else:
                sys.exit(0)
        except Exception as e:
            message_printer.message(e)
            sys.exit(1)


def write_intermediate_csv(args, data_set, is_upstream_valid, output_metadata_file):
    row_count = 0
    with open(output_metadata_file, mode="a", newline="", encoding="utf-8") as intermediate_metadata_csv:
        is_metadata_valid = is_upstream_valid
        fieldnames = metadata_creator.get_field_names()
        writer = csv.DictWriter(intermediate_metadata_csv, fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for _, row in data_set.iterrows():
            row_count += 1
            try:
                metadata_dict = metadata_creator.create_intermediate_metadata_dict(row, args)
                writer.writerow(metadata_dict)
                if row_count % 100 == 0:
                    intermediate_metadata_csv.flush()
            except Exception as e:
                is_metadata_valid = False
                message_printer.message(f"Error creating metadata: {e}")
                if not args.dry_run:
                    sys.exit(1)
    return is_metadata_valid, row_count

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
                message_printer.message(f"An error caused due to: {client_error}")
                input("Fix the error and press enter to continue")
                aws_interactions.refresh_session()
    return account_number

def get_confirmation_to_proceed(prompt="Are you sure?"):
    confirmation = input(prompt).strip().lower()
    return confirmation in {"y", "yes"}

def is_folder_writable(output_folder):
    try:
        testfile = tempfile.TemporaryFile(dir=output_folder)
        testfile.close()
        return True
    except (OSError, PermissionError):
        return False


def main():
    args = argument_parser_builder.build().parse_args()
    validate_arguments(args)

    input_file_path = Path(args.input)
    if input_file_path.suffix.lower() == ".csv":
        data_set = pd.read_csv(input_file_path, dtype=str, keep_default_na=False)
    elif input_file_path.suffix.lower() in {".xls", ".xlsx"}:
        data_set = pd.read_excel(input_file_path)
    else:
        raise Exception("Unsupported input file format. Only CSV and Excel (xls, xlsx) files are supported for input")

    try:
        is_valid = dataset_validator.validate_dataset(data_set, str(input_file_path), args.dry_run)
    except Exception as e:
        raise Exception(f"Inputs supplied to the process are invalid, please fix errors before continuing: {e}")

    is_discovery_available = discovery_client.is_discovery_api_reachable()
    if not is_discovery_available:
        message_printer.message("Discovery API is not available for getting metadata information, terminating process")
        sys.exit(1)

    upload_files_to_ingest_bucket(data_set, args, is_valid)

    if not args.dry_run:
        message_printer.message("Upload finished successfully")

if __name__ == "__main__":
    main()