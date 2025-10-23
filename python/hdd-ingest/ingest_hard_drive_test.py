import argparse
import os
import shutil
import tempfile
from io import StringIO
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

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
        self.assertEqual("intg", args.environment)

    def test_should_treat_dry_run_param_as_true_when_no_option_is_provided(self):
        args = self.parser.parse_args(["-i", "some_file.csv", "-e", "not_prod", "-d"])
        self.assertEqual(True, args.dry_run)

        args = self.parser.parse_args(["-i", "some_file.csv", "-e", "not_prod", "--dry_run"])
        self.assertEqual(True, args.dry_run)

    def test_should_set_the_output_folder_as_temp_location_if_not_passed_as_a_parameter(self):
        args = self.parser.parse_args(["-i", "some_file.csv", "-e", "not_prod", "-d"])
        self.assertEqual(tempfile.gettempdir(), args.output)


    def test_should_parse_arguments_and_set_correct_parameters_for_arguments_passed_on_command_line(self):
        args = self.parser.parse_args(["-i", "some_file.csv", "-e", "not_prod", "-d", "True", "-o" "/home/Users"])
        self.assertEqual("some_file.csv", args.input)
        self.assertTrue(args.dry_run)
        self.assertEqual("not_prod", args.environment)
        self.assertEqual("/home/Users", args.output)

    def test_should_error_when_the_input_file_does_not_exist(self):
        args = argparse.Namespace(input='non_existent_file.csv', environment='not_prod', dry_run='True')
        with self.assertRaises(Exception) as e:
            ingest_hard_drive.validate_arguments(args)

        self.assertEqual("The input file [non_existent_file.csv] does not exist or it is not a valid file\n", str(e.exception))

    def test_should_error_when_the_output_location_is_not_a_folder(self):
        tmp1 = os.path.join(self.test_dir, "hdd_ingest_test_file1.txt")
        with open(tmp1, "w") as f:
            f.write("temporary file one")

        args = argparse.Namespace(input=tmp1, environment='not_prod', dry_run='True', output="some/random/file.pdf")
        with self.assertRaises(Exception) as e:
            ingest_hard_drive.validate_arguments(args)
        self.assertEqual("The output metadata location [some/random/file.pdf] does not exist or it is not a valid folder\n", str(e.exception))

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

    @patch("aws_interactions.send_message")
    @patch("aws_interactions.upload_metadata")
    @patch("aws_interactions.upload_file")
    def test_should_send_the_files_to_the_s3_bucket_and_send_a_message_to_the_queue(self, mock_upload_file, mock_upload_metadata, mock_send_message):
        tmp1 = os.path.join(self.test_dir, "hdd_ingest_test_file1.txt")
        with open(tmp1, "w") as f:
            f.write("temporary file one")

        metadata_csv_data = f"""Series,UUID,fileId,description,Filename,FileReference,ClientSideOriginalFilepath,checksum_md5,checksum_sha256
JS 8,someRecordId,someFileId,SomeDescription,JS-8-3.pdf,3,{tmp1},,some_checksum""".strip()

        tmp2 = os.path.join(self.test_dir, "metadata_to_ingest.csv")
        with open(tmp2, "w") as f:
            f.write(metadata_csv_data)

        args = SimpleNamespace(environment="test", input="/home/users/input-file.csv")
        ingest_hard_drive.upload_files(tmp2, "123456789", args)

        expected_metadata = {
            "Series": "JS 8",
            "UUID": "someRecordId",
            "fileId": "someFileId",
            "description": "SomeDescription",
            "Filename": "JS-8-3.pdf",
            "FileReference": "3",
            "ClientSideOriginalFilepath": tmp1,
            "checksum_sha256": "some_checksum"
        }

        mock_upload_file.assert_called_once_with("someRecordId", "test-dr2-ingest-raw-cache", "someFileId",  tmp1)
        mock_upload_metadata.assert_called_once_with("someRecordId", "test-dr2-ingest-raw-cache", expected_metadata)
        mock_send_message.assert_called_once_with("someRecordId", "test-dr2-ingest-raw-cache", "https://sqs.eu-west-2.amazonaws.com/123456789/test-dr2-preingest-hdd-importer")

    @patch("aws_interactions.send_message")
    @patch("aws_interactions.upload_metadata")
    @patch("aws_interactions.upload_file")
    def test_should_send_the_files_to_the_s3_bucket_when_the_data_path_is_relative_to_the_csv_file_and_send_a_message_to_the_queue(self, mock_upload_file, mock_upload_metadata, mock_send_message):
        tmp1 = os.path.join(self.test_dir, "hdd_ingest_test_file1.txt")
        with open(tmp1, "w") as f:
            f.write("temporary file one")

        metadata_csv_data = f"""Series,UUID,fileId,description,Filename,FileReference,ClientSideOriginalFilepath,checksum_md5,checksum_sha256
JS 8,someRecordId,someFileId,SomeDescription,JS-8-3.pdf,3,hdd_ingest_test_file1.txt,,some_checksum"""

        tmp2 = os.path.join(self.test_dir, "metadata_to_ingest.csv")
        with open(tmp2, "w") as f:
            f.write(metadata_csv_data)

        args = SimpleNamespace(environment="test", input=f"{tmp2}")
        ingest_hard_drive.upload_files(tmp2, "123456789", args)

        expected_metadata = {
            "Series": "JS 8",
            "UUID": "someRecordId",
            "fileId": "someFileId",
            "description": "SomeDescription",
            "Filename": "JS-8-3.pdf",
            "FileReference": "3",
            "ClientSideOriginalFilepath": "hdd_ingest_test_file1.txt",
            "checksum_sha256": "some_checksum"
        }

        mock_upload_file.assert_called_once_with("someRecordId", "test-dr2-ingest-raw-cache", "someFileId",  tmp1)
        mock_upload_metadata.assert_called_once_with("someRecordId", "test-dr2-ingest-raw-cache", expected_metadata)
        mock_send_message.assert_called_once_with("someRecordId", "test-dr2-ingest-raw-cache", "https://sqs.eu-west-2.amazonaws.com/123456789/test-dr2-preingest-hdd-importer")

    @patch("discovery_client.get_title_and_description")
    def test_create_metadata_should_throw_exception_when_it_cannot_find_title_or_description_from_discovery(self, mock_description):

        mock_description.return_value = "", ""

        csv_data = """catRef,someOtherColumn,fileName,checksum,anotherColumn
            someTestCatRef,some_thing,d:\\js\\3\\1\\evid0001.pdf,9584816fad8b38a8057a4bb90d5998b8679e6f7652bbdc71fc6a9d07f73624fc"""
        data_set = pd.read_csv(StringIO(csv_data))
        first_row = data_set.iloc[0]

        with self.assertRaises(Exception) as e:
            ingest_hard_drive.create_metadata(first_row)

        self.assertEqual("Title and Description both are empty for 'someTestCatRef', unable to proceed with this record", str(e.exception))

