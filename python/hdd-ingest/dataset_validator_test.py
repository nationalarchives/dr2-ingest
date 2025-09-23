from unittest import TestCase
import pandas as pd
from io import StringIO

import dataset_validator
from dataset_validator import Js8Validator


class Test(TestCase):
    def setUp(self):
        csv_data = """catRef,someOtherColumn,fileName,checksum,anotherColumn
        JS 8/3,some_thing,d:\\js\\3\\1\\evid0001.pdf,9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc
        JS 8/4,some_thing,d:\\js\\3\\1\\evid0002.pdf,checksum_1234567890,
        JS 8/5,some_thing,d:\\js\\3\\1\\evid0003.pdf,c74daf9d9a4063bdfbf1fd234ac529d120203e04af7c4e60b3236c76f37fff90"""
        self.valid_data_set = pd.read_csv(StringIO(csv_data))

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

    def test_should_allow_duplicate_values_in_columns_that_are_not_needed_to_have_unique_data(self):
        csv_data = """catRef,fileName,checksum,duplicate_column
        JS 8/3,d:\\js\\3\\1\\evid0001.pdf,9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc,same_value
        JS 8/4,d:\\js\\3\\1\\evid0002.pdf,some_random_checksum,same_value
        JS 8/5,d:\\js\\3\\1\\evid0003.pdf,c74daf9d9a4063bdfbf1fd234ac529d120203e04af7c4e60b3236c76f37fff90,same_value"""
        data_set = pd.read_csv(StringIO(csv_data))
        self.assertTrue(dataset_validator.validate_dataset(Js8Validator(), data_set))

    def test_should_not_allow_empty_values_in_a_mandatory_columns(self):
        csv_data = """catRef,fileName,checksum,duplicate_column
        JS 8/3,d:\\js\\3\\1\\evid0001.pdf,checksum_one,same_value
        JS 8/4,d:\\js\\3\\1\\evid0002.pdf,,same_value
        JS 8/5,d:\\js\\3\\1\\evid0003.pdf,checksum_three,same_value"""
        data_set = pd.read_csv(StringIO(csv_data))
        with self.assertRaises(Exception) as e:
            dataset_validator.validate_dataset(Js8Validator(), data_set)

        self.assertEqual("The column 'checksum' has empty entries", str(e.exception))
