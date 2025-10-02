import os
from collections import Counter

import pandas


####
# This class validates the data as per requirements of the series.
# The requirements for input file to ingest JS 8 series are as follows:
# 1) It must have at least 3 columns -> catRef, fileName, checksum
# 2) There should be no duplicates in columns which are supposed to have unique values
# 3) There should be no empty values in columns that are not allowed empty values
####
class Js8Validator:
    REQUIRED_COLUMNS = ["catRef", "fileName", "checksum"]
    UNIQUE_COLUMNS = ["catRef", "fileName", "checksum"]
    NON_EMPTY_COLUMNS = ["catRef", "fileName"]

    def validate(self, data_set):
        data_set: pandas.DataFrame
        columns = data_set.columns
        if not set(self.REQUIRED_COLUMNS).issubset(columns):
            raise Exception(f"Input spreadsheet is missing one or more of the required columns: {self.REQUIRED_COLUMNS}")

        for col in self.UNIQUE_COLUMNS:
            if not data_set[col].is_unique:
                raise Exception(f"The column '{col}' has duplicate entries")

        for col in self.NON_EMPTY_COLUMNS:
            if data_set[col].isnull().any():
                raise Exception(f"The column '{col}' has empty entries")

        data_set: pandas.DataFrame
        all_files_exist = True
        missing_files = []
        for index, row in data_set.iterrows():
            file_path = row["fileName"].strip()
            if not os.path.exists(file_path):
                missing_files.append(file_path)
                all_files_exist = False

        if not all_files_exist:
            raise Exception(f"Failed to locate following files: " + ",".join(missing_files))

        return True

####
# This method validates a data in the CSV file, this simply offloads the validation to a specific validator passed into
# the method as a parameter. Thus, we can continue to use the same method where validations differ for different series
####
def validate_dataset(series_validator, data_set):
    return series_validator.validate(data_set)
