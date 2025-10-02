import os
import shutil
import tempfile
from unittest import TestCase
import pandas as pd
from io import StringIO

import dataset_validator
from dataset_validator import Js8Validator


class Test(TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        tmp1 = os.path.join(self.test_dir, "hdd_ingest_test_file1.txt")
        with open(tmp1, "w") as f:
            f.write("temporary file one")
        tmp2 = os.path.join(self.test_dir, "hdd_ingest_test_file2.txt")
        with open(tmp2, "w") as f:
            f.write("temporary file one")
        tmp3 = os.path.join(self.test_dir, "hdd_ingest_test_file3.txt")
        with open(tmp3, "w") as f:
            f.write("temporary file one")

        csv_data = f"""catRef,someOtherColumn,fileName,checksum,anotherColumn
        JS 8/3,duplicate_value_allowed_here,{tmp1},9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc,another
        JS 8/4,duplicate_value_allowed_here,{tmp2},checksum_1234567890,
        JS 8/5,duplicate_value_allowed_here,{tmp3},c74daf9d9a4063bdfbf1fd234ac529d120203e04af7c4e60b3236c76f37fff90,something_else"""
        self.valid_data_set = pd.read_csv(StringIO(csv_data))

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_should_contain_required_columns(self):
        is_valid = dataset_validator.validate_dataset(Js8Validator(), self.valid_data_set)
        self.assertEqual(True, is_valid)

    def test_should_throw_an_exception_when_one_of_the_required_columns_is_missing(self):
        csv_data = """catRef,fileName
        JS 8/3,d:\\js\\3\\1\\evid0001.pdf
        JS 8/4,d:\\js\\3\\1\\evid0002.pdf"""
        data_set = pd.read_csv(StringIO(csv_data))
        with self.assertRaises(Exception) as e:
            dataset_validator.validate_dataset(Js8Validator(), data_set)

        self.assertEqual("Input spreadsheet is missing one or more of the required columns: ['catRef', 'fileName', 'checksum']", str(e.exception))

    def test_should_throw_an_exception_when_the_columns_have_duplicate_entries(self):
        csv_data = """catRef,fileName,checksum
        JS 8/3,d:\\js\\3\\1\\evid0001.pdf,9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc
        JS 8/4,d:\\js\\3\\1\\evid0002.pdf,
        JS 8/5,d:\\js\\3\\1\\evid0001.pdf,c74daf9d9a4063bdfbf1fd234ac529d120203e04af7c4e60b3236c76f37fff90"""
        data_set = pd.read_csv(StringIO(csv_data))
        with self.assertRaises(Exception) as e:
            dataset_validator.validate_dataset(Js8Validator(), data_set)

        self.assertEqual("The column 'fileName' has duplicate entries", str(e.exception))

    def test_should_not_allow_empty_values_in_columns_that_should_not_have_empty_values(self):
        csv_data = """catRef,fileName,checksum,duplicate_column
        JS 8/3,d:\\js\\3\\1\\evid0001.pdf,checksum_one,same_value
        JS 8/4,,,same_value
        JS 8/5,d:\\js\\3\\1\\evid0003.pdf,checksum_three,same_value"""
        data_set = pd.read_csv(StringIO(csv_data))
        with self.assertRaises(Exception) as e:
            dataset_validator.validate_dataset(Js8Validator(), data_set)

        self.assertEqual("The column 'fileName' has empty entries", str(e.exception))

    def test_should_throw_an_exception_when_one_or_more_files_are_missing(self):
        additional_row = pd.DataFrame([
            {"catRef": "JS 8/6", "someOtherColumn": "","fileName": "/tmp/non-existent-file1.txt","checksum": "checksum_four","anotherColumn": "some_data"},
            {"catRef": "JS 8/7", "someOtherColumn": "","fileName": "/tmp/non-existent-file2.txt","checksum": "checksum_five","anotherColumn": "no_data"}])
        erroneous_dataset = pd.concat([self.valid_data_set, additional_row], ignore_index=True)
        with self.assertRaises(Exception) as e:
            dataset_validator.validate_dataset(Js8Validator(), erroneous_dataset)

        self.assertEqual("Failed to locate following files: /tmp/non-existent-file1.txt,/tmp/non-existent-file2.txt", str(e.exception))

