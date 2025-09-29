import argparse
import uuid
import json
from pathlib import Path

import pandas
import pandas as pd
from pandas import Series
from pandas.core.interchange.dataframe_protocol import DataFrame

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
    metadata = {
        "Series": row["catRef"].split("/")[0].strip(),
        "UUID": str(uuid.uuid4()),
        "fileId": str(uuid.uuid4()),
        "description": discovery_client.get_description(catalog_ref),  # need to get it from discovery
        "fileName": row["fileName"].split("\\")[-1].strip(),
        "checksum_sha256": row["checksum"].strip(),
        "FileReference": catalog_ref
    }
    return metadata

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
        raise Exception("Boom")

    data_set: pandas.DataFrame
    for index, row in data_set.iterrows():
        metadata = create_metadata(row)
        print(metadata)








if __name__ == "__main__":
    main()