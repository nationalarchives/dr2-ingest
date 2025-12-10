import os
import sys
from pathlib import Path

import pandas

import message_printer as mp

REQUIRED_COLUMNS = ("catRef", "fileName", "checksum")
UNIQUE_COLUMNS = ("catRef", "fileName")
UNIQUE_COLUMNS_WARN_ONLY = ("checksum")
NON_EMPTY_COLUMNS = ("catRef", "fileName")


def validate_dataset(data_set, input_file_path, is_dry_run=False):
    is_valid = True
    data_set: pandas.DataFrame
    columns = data_set.columns

    if not set(REQUIRED_COLUMNS).issubset(columns):
        is_valid = False
        throw_or_report(f"Input file is missing one or more of the required columns: {REQUIRED_COLUMNS}", is_dry_run)

    for col in columns:
        if col in UNIQUE_COLUMNS:
            if not data_set[col].is_unique:
                is_valid = False
                throw_or_report(f"The column '{col}' has duplicate entries", is_dry_run)

        if col in UNIQUE_COLUMNS_WARN_ONLY:
            if not data_set[col].is_unique:
                mp.print_message(f"The column '{col}' has duplicate entries")

        if col in NON_EMPTY_COLUMNS:
            if data_set[col].isnull().any():
                is_valid = False
                throw_or_report(f"The column '{col}' has empty entries", is_dry_run)

    # return from here for fundamental failures, otherwise carry on and validate data in each row
    if not is_valid:
        mp.print_message("Please fix the errors identified during validation before continuing further")
        sys.exit(1)

    data_set: pandas.DataFrame
    all_files_exist = True
    missing_files = []
    empty_files = []
    total_rows = len(data_set)
    for counter, (index, row) in enumerate(data_set.iterrows(), start=1):
        mp.print_progress(f"Validating {counter} of {total_rows} rows")
        file_path = get_absolute_file_path(input_file_path, row["fileName"].strip())
        if not os.path.exists(file_path):
            missing_files.append(file_path)
            all_files_exist = False
        else:
            if os.path.getsize(file_path) == 0:
                empty_files.append(file_path)

    mp.print_progress(f"Validation finished for {total_rows} rows")
    if empty_files:
        mp.print_message("Following files are empty: " + ",".join(empty_files))

    if not all_files_exist:
        is_valid = False
        throw_or_report("Failed to locate following files: " + ",".join(missing_files), is_dry_run)

    return is_valid


def throw_or_report(param, is_dry_run):
    if is_dry_run:
        mp.print_message(param)
    else:
        raise Exception(param)


def get_absolute_file_path(input_path, relative_or_absolute_file_path):
    input_file_path = Path(input_path).resolve()

    if Path(relative_or_absolute_file_path).is_absolute():
        return str(relative_or_absolute_file_path)
    else:
        normalised_relative_path = os.path.normpath(relative_or_absolute_file_path.replace("\\", "/"))
        full_path = Path(input_file_path.parent / normalised_relative_path).resolve()
        return str(full_path)
