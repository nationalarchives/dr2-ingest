from collections import Counter

####
# This class validates the data as per requirements of the series.
####
class Js8Validator:
    REQUIRED_COLUMNS = ["catRef", "fileName", "checksum"]
    UNIQUE_COLUMNS = ["catRef", "fileName"]

    def validate(self, data_set):
        columns = data_set.columns
        if not set(self.REQUIRED_COLUMNS).issubset(columns):
            raise Exception(f"Input spreadsheet is missing one or more of the required columns: {self.REQUIRED_COLUMNS}")

        for col in self.UNIQUE_COLUMNS:
            if not data_set[col].is_unique:
                raise Exception(f"The column '{col}' has duplicate entries")

        for col in self.REQUIRED_COLUMNS:
            if data_set[col].isnull().any():
                raise Exception(f"The column '{col}' has empty entries")

        return True

####
# This method validates a data in the CSV file, this simply offloads the validation to a specific validator passed into
# the method as a parameter. Thus, we can continue to use the same method where validations differ for different series
####
def validate_dataset(series_validator, data_set):
    return series_validator.validate(data_set)
