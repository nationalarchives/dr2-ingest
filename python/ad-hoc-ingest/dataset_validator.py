import os
from pathlib import Path

import pandas

####
# This class validates the data as per requirements of the series.
# The requirements for input file to ingest JS 8 series are as follows:
# 1) It must have at least 3 columns -> catRef, fileName, checksum
# 2) There should be no duplicates in columns which are supposed to have unique values
# 3) There should be no empty values in columns that are not allowed empty values
# 4) Files given at filePath must exist
####

class Js8Validator:
    REQUIRED_COLUMNS = ["catRef", "fileName", "checksum"]
    UNIQUE_COLUMNS = ["catRef", "fileName", "checksum"] #what if there is same file for 2 different records?
    NON_EMPTY_COLUMNS = ["catRef", "fileName"]

    def validate(self, data_set, input_file_path, is_dry_run):
        validation_fail = False
        data_set: pandas.DataFrame
        columns = data_set.columns

        #Validate the overall spreadsheet correctness
        if not set(self.REQUIRED_COLUMNS).issubset(columns):
            validation_fail = True
            throw_or_report(f"Input spreadsheet is missing one or more of the required columns: {self.REQUIRED_COLUMNS}", is_dry_run)

        for col in data_set.columns:
            if col in self.UNIQUE_COLUMNS:
                if not data_set[col].is_unique:
                        validation_fail = True
                        throw_or_report(f"The column '{col}' has duplicate entries", is_dry_run)
            else:
                continue

        for col in data_set.columns:
            if col in self.NON_EMPTY_COLUMNS:
                if data_set[col].isnull().any():
                    validation_fail = True
                    throw_or_report(f"The column '{col}' has empty entries", is_dry_run)

        if validation_fail:
            return False

        # Validate the correctness of data in each individual row (e.g. does the file exist?)
        data_set: pandas.DataFrame
        all_files_exist = True
        missing_files = []
        for index, row in data_set.iterrows():
            file_path =  get_absolute_file_path(input_file_path, row["fileName"].strip())
            if not os.path.exists(file_path):
                missing_files.append(file_path)
                all_files_exist = False

        if not all_files_exist:
            validation_fail = True
            throw_or_report(f"Failed to locate following files: " + ",".join(missing_files), is_dry_run)

        return not validation_fail



####
# This method validates a data in the CSV file, this simply offloads the validation to a series specific validator
# that is passed into the method as a parameter. Thus, we can continue to use the same method from ingest script
# and implement different validators based on series to be ingested
####
def validate_dataset(series_validator, data_set, input_file_path, is_dry_run=False):
    return series_validator.validate(data_set, input_file_path, is_dry_run)


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

