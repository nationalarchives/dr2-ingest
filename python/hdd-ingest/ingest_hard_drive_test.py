import argparse
import os
import shutil
import tempfile
from io import StringIO
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch, MagicMock

import pandas as pd

import ingest_hard_drive


class Test(TestCase):
    def setUp(self):
        self.parser = ingest_hard_drive.build_argument_parser()
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)
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
            self.assertEqual("evid0001.pdf", metadata["Filename"])
            self.assertEqual("3", metadata["FileReference"])
            self.assertEqual("9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc", metadata["checksum_sha256"])
            self.assertEqual("Some description from discovery", metadata["description"])
            self.assertEqual("d:\\js\\3\\1\\evid0001.pdf", metadata["ClientSideOriginalFilepath"])

    @patch("discovery_client.get_title_and_description")
    def test_create_metadata_should_create_a_filename_from_various_paths_independent_of_platform(self, mock_description):

        mock_description.return_value = None, "Some description from discovery"

        csv_data = """catRef,fileName,checksum
        JS 8/3,d:\\js\\3\\1\\evid0001.pdf,windows_absolute_path 
        JS 8/4,c:\\evid0001.pdf,windows_absolute_path_at_root
        JS 8/5,c:\old_folder\evid0001.pdf,windows_absolute_path_single_slash
        JS 8/6,/home/users/evid0001.pdf,unix_absolute_path
        JS 8/7,c:evid0001.pdf,windows_no_slashes
        JS 8/7,\a\b\evid0001.pdf,windows_relative_single_slash
        JS 8/8,c:/abcd/evid0001.pdf,windows_absolute_path_forward_slash"""
        data_set = pd.read_csv(StringIO(csv_data))
        for index, row in data_set.iterrows():
            metadata = ingest_hard_drive.create_metadata(row)
            self.assertEqual("evid0001.pdf", metadata["Filename"])

    @patch("discovery_client.get_title_and_description")
    def test_create_metadata_should_use_title_when_title_is_available_from_discovery(self, mock_description):

        mock_description.return_value = "Some title", "Some description from discovery"

        csv_data = """catRef,someOtherColumn,fileName,checksum,anotherColumn
            JS 8/3,some_thing,d:\\js\\3\\1\\evid0001.pdf,9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc"""
        data_set = pd.read_csv(StringIO(csv_data))
        for index, row in data_set.iterrows():
            metadata = ingest_hard_drive.create_metadata(row)
            self.assertEqual("Some title", metadata["description"])


    def test_create_metadata_should_create_an_md5_hash_if_checksum_is_missing_from_the_input(self):
        tmp1 = os.path.join(self.test_dir, "hdd_ingest_test_file1.txt")
        with open(tmp1, "w") as f:
            f.write("temporary file one")

        csv_data = f"""catRef,someOtherColumn,fileName,checksum,anotherColumn
        JS 8/3,duplicate_value_allowed_here,{tmp1},,another"""
        data_set = pd.read_csv(StringIO(csv_data), dtype={"checksum": str}, keep_default_na=False)
        for index, row in data_set.iterrows():
            metadata = ingest_hard_drive.create_metadata(row)
            self.assertEqual("3a16291a00172e7af139cef48d1fe2f7", metadata["checksum_md5"])

    @patch.dict(os.environ, {"ACCOUNT_NUMBER": "123456789"})
    @patch("boto3.client")
    def test_should_send_the_files_to_the_s3_bucket_and_send_a_message_to_the_queue(self, mock_boto):
        mock_client = MagicMock()
        mock_boto.return_value = mock_client
        tmp1 = os.path.join(self.test_dir, "hdd_ingest_test_file1.txt")
        with open(tmp1, "w") as f:
            f.write("temporary file one")

        metadata = {"UUID": "someRecordId", "fileId": "someFileId"}
        args = SimpleNamespace(environment="test")
        ingest_hard_drive.upload_files(metadata, tmp1, args)

        mock_client.upload_file.assert_called_once_with(tmp1, "test-dr2-ingest-raw-cache", "someRecordId/someFileId")
        mock_client.send_message.assert_called_once_with(QueueUrl="https://sqs.eu-west-2.amazonaws.com/123456789/test-dr2-preingest-hdd-importer", MessageBody="""{"assetId": "someRecordId", "bucket": "test-dr2-ingest-raw-cache"}""")

    @patch("discovery_client.get_title_and_description")
    def test_create_metadata_should_throw_exception_when_it_cannot_find_title_or_description_from_discovery(self, mock_description):

        mock_description.return_value = "", ""

        csv_data = """catRef,someOtherColumn,fileName,checksum,anotherColumn
            someTestCatRef,some_thing,d:\\js\\3\\1\\evid0001.pdf,9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc"""
        data_set = pd.read_csv(StringIO(csv_data))
        first_row = data_set.iloc[0]

        with self.assertRaises(Exception) as e:
            metadata = ingest_hard_drive.create_metadata(first_row)

        self.assertEqual("Title and Description both are empty for 'someTestCatRef', unable to proceed with this record", str(e.exception))
