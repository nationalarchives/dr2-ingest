import argparse
from io import StringIO
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

import ingest_hard_drive


class Test(TestCase):
    def setUp(self):
        self.parser = ingest_hard_drive.build_argument_parser()

    def test_should_fail_when_mandatory_argument_is_missing(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args([])

        with self.assertRaises(SystemExit):
            self.parser.parse_args(["-i"])

    def test_should_parse_mandatory_arguments_and_return_default_for_optional_arguments(self):
        args = self.parser.parse_args(["-i", "some_file.csv"])
        self.assertEqual("some_file.csv", args.input)
        self.assertEqual(False, args.dry_run)
        self.assertEqual("INTG", args.environment)

    def test_should_parse_arguments_and_set_correct_parameters_for_arguments_passed_on_command_line(self):
        args = self.parser.parse_args(["-i", "some_file.csv", "-e", "not_prod", "-d", "True"])
        self.assertEqual("some_file.csv", args.input)
        self.assertTrue(args.dry_run)
        self.assertEqual("not_prod", args.environment)

    def test_should_error_when_the_input_file_does_not_exist(self):
        args = argparse.Namespace(input='non_existent_file.csv', environment='not_prod', dry_run='True')
        with self.assertRaises(Exception) as e:
            ingest_hard_drive.validate_arguments(args)

        self.assertEqual("The input file [non_existent_file.csv] does not exist or it is not a valid file\n", str(e.exception))

    @patch("discovery_client.get_title_and_description")
    def test_create_metadata_should_create_a_metadata_object_from_csv_rows(self, mock_description):

        mock_description.return_value = None, "Some description from discovery"

        csv_data = """catRef,someOtherColumn,fileName,checksum,anotherColumn
        JS 8/3,some_thing,d:\\js\\3\\1\\evid0001.pdf,9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc"""
        data_set = pd.read_csv(StringIO(csv_data))
        for index, row in data_set.iterrows():
            metadata = ingest_hard_drive.create_metadata(row)
            self.assertEqual("JS 8", metadata["Series"])
            self.assertEqual("evid0001.pdf", metadata["fileName"])
            self.assertEqual("JS 8/3", metadata["FileReference"])
            self.assertEqual("9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc", metadata["checksum_sha256"])
            self.assertEqual("Some description from discovery", metadata["description"])
            self.assertEqual("d:\\js\\3\\1\\evid0001.pdf", metadata["ClientSideOriginalFilePath"])

    @patch("discovery_client.get_title_and_description")
    def test_create_metadata_should_use_title_when_title_is_available_from_discovery(self, mock_description):

        mock_description.return_value = "Some title", "Some description from discovery"

        csv_data = """catRef,someOtherColumn,fileName,checksum,anotherColumn
            JS 8/3,some_thing,d:\\js\\3\\1\\evid0001.pdf,9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc"""
        data_set = pd.read_csv(StringIO(csv_data))
        for index, row in data_set.iterrows():
            metadata = ingest_hard_drive.create_metadata(row)
            self.assertEqual("Some title", metadata["description"])


