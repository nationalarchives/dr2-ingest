import os
from pathlib import Path

import pandas

REQUIRED_COLUMNS = ("catRef", "fileName", "checksum")
UNIQUE_COLUMNS = ("catRef", "fileName", "checksum")
NON_EMPTY_COLUMNS = ("catRef", "fileName")

def validate_dataset(data_set, input_file_path, is_dry_run = False):
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

        if col in NON_EMPTY_COLUMNS:
            if data_set[col].isnull().any():
                is_valid = False
                throw_or_report(f"The column '{col}' has empty entries", is_dry_run)

    # return from here for fundamental failures, otherwise carry on and validate data in each row
    if not is_valid:
        return False

    data_set: pandas.DataFrame
    all_files_exist = True
    missing_files = []
    for _, row in data_set.iterrows():
        file_path = get_absolute_file_path(input_file_path, row["fileName"].strip())
        if not os.path.exists(file_path):
            missing_files.append(file_path)
            all_files_exist = False

    if not all_files_exist:
        is_valid = False
        throw_or_report("Failed to locate following files: " + ",".join(missing_files), is_dry_run)

    return is_valid

def throw_or_report(param, is_dry_run):
    if is_dry_run:
        print(param)
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

