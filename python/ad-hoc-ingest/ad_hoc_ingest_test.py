import argparse
import os
import shutil
import tempfile
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
from botocore.exceptions import ClientError

import ad_hoc_ingest


class Test(TestCase):
    def setUp(self):
        self.test_dir = os.path.realpath(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_validate_arguments_should_error_when_the_input_file_does_not_exist(self):
        args = argparse.Namespace(input='non_existent_file.csv', environment='not_prod', dry_run='True')
        with self.assertRaises(Exception) as e:
            ad_hoc_ingest.validate_arguments(args)

        self.assertEqual("Either the input file [non_existent_file.csv] does not exist or it is not a valid file\n", str(e.exception))

        args = argparse.Namespace(input=self.test_dir, environment='not_prod', dry_run='True')
        with self.assertRaises(Exception) as e:
            ad_hoc_ingest.validate_arguments(args)

        self.assertEqual(f"Either the input file [{self.test_dir}] does not exist or it is not a valid file\n",
                         str(e.exception))

    def test_validate_arguments_should_error_when_the_output_location_is_not_a_folder(self):
        tmp1 = os.path.join(self.test_dir, "ad_hoc_ingest_test_file1.txt")
        with open(tmp1, "w") as f:
            f.write("temporary file one")

        args = argparse.Namespace(input=tmp1, environment='not_prod', dry_run='True', output="some/random/folder")
        with self.assertRaises(Exception) as e:
            ad_hoc_ingest.validate_arguments(args)
        self.assertEqual("Either the output metadata location [some/random/folder] does not exist or it is not a valid folder\n", str(e.exception))

        tmp2 = os.path.join(self.test_dir, "output.txt")
        with open(tmp2, "w") as f:
            f.write("temporary output one")

        args = argparse.Namespace(input=tmp1, environment='not_prod', dry_run='True', output=tmp2)
        with self.assertRaises(Exception) as e:
            ad_hoc_ingest.validate_arguments(args)
        self.assertEqual(f"Either the output metadata location [{tmp2}] does not exist or it is not a valid folder\n", str(e.exception))


    @patch("aws_interactions.send_sqs_message")
    @patch("aws_interactions.upload_metadata")
    @patch("aws_interactions.upload_file")
    @patch("aws_interactions.get_region")
    def test_should_send_the_files_to_the_s3_bucket_and_send_a_message_to_the_queue(self, mock_region, mock_upload_file, mock_upload_metadata, mock_send_message):
        mock_region.return_value = "london-town"
        tmp1 = os.path.join(self.test_dir, "ad_hoc_ingest_test_file1.txt")
        with open(tmp1, "w") as f:
            f.write("temporary file one")

        metadata_csv_data = f"""Series,UUID,fileId,description,Filename,FileReference,ClientSideOriginalFilepath,formerRefDept,formerRefTNA,checksum_md5,checksum_sha256,IAID,digitalAssetSource
JS 8,someRecordId,someFileId,SomeDescription,JS-8-3.pdf,3,{tmp1},dept_ref,tna_ref,,some_checksum,some_iaid,Surrogate""".strip()

        tmp2 = os.path.join(self.test_dir, "metadata_to_ingest.csv")
        with open(tmp2, "w") as f:
            f.write(metadata_csv_data)

        args = SimpleNamespace(environment="test", input="/home/users/input-file.csv")
        ad_hoc_ingest.upload_files(tmp2, "123456789", args)

        expected_metadata = {
            "Series": "JS 8",
            "UUID": "someRecordId",
            "fileId": "someFileId",
            "description": "SomeDescription",
            "Filename": "JS-8-3.pdf",
            "FileReference": "3",
            "ClientSideOriginalFilepath": tmp1,
            "formerRefDept": "dept_ref",
            "IAID": "some_iaid",
            "digitalAssetSource": "Surrogate",
            "formerRefTNA": "tna_ref",
            "checksum_sha256": "some_checksum"
        }

        mock_upload_file.assert_called_once_with("someRecordId", "test-dr2-ingest-adhoc-cache", "someFileId",  tmp1)
        mock_upload_metadata.assert_called_once_with("someRecordId", "test-dr2-ingest-adhoc-cache", expected_metadata)
        mock_send_message.assert_called_once_with("someRecordId", "test-dr2-ingest-adhoc-cache", "https://sqs.london-town.amazonaws.com/123456789/test-dr2-preingest-adhoc-importer")

    @patch("aws_interactions.send_sqs_message")
    @patch("aws_interactions.upload_metadata")
    @patch("aws_interactions.upload_file")
    @patch("aws_interactions.get_region")
    def test_should_send_the_files_to_the_s3_bucket_when_the_data_path_is_relative_to_the_csv_file_and_send_a_message_to_the_queue(self, mock_region, mock_upload_file, mock_upload_metadata, mock_send_message):
        mock_region.return_value = "london-town"
        tmp1 = os.path.join(self.test_dir, "ad_hoc_ingest_test_file1.txt")
        with open(tmp1, "w") as f:
            f.write("temporary file one")

        metadata_csv_data = f"""Series,UUID,fileId,description,Filename,FileReference,ClientSideOriginalFilepath,formerRefDept,formerRefTNA,checksum_md5,checksum_sha256,IAID,digitalAssetSource
JS 8,someRecordId,someFileId,SomeDescription,JS-8-3.pdf,3,ad_hoc_ingest_test_file1.txt,dept_ref,tna_ref,,some_checksum,some_iaid,Surrogate"""

        tmp2 = os.path.join(self.test_dir, "metadata_to_ingest.csv")
        with open(tmp2, "w") as f:
            f.write(metadata_csv_data)

        args = SimpleNamespace(environment="test", input=f"{tmp2}")
        ad_hoc_ingest.upload_files(tmp2, "123456789", args)

        expected_metadata = {
            "Series": "JS 8",
            "UUID": "someRecordId",
            "fileId": "someFileId",
            "description": "SomeDescription",
            "Filename": "JS-8-3.pdf",
            "FileReference": "3",
            "ClientSideOriginalFilepath": "ad_hoc_ingest_test_file1.txt",
            "IAID": "some_iaid",
            "digitalAssetSource": "Surrogate",
            "formerRefDept": "dept_ref",
            "formerRefTNA": "tna_ref",
            "checksum_sha256": "some_checksum"
        }

        resolved_path = str(Path(tmp1).resolve())
        mock_upload_file.assert_called_once_with("someRecordId", "test-dr2-ingest-adhoc-cache", "someFileId",  resolved_path)
        mock_upload_metadata.assert_called_once_with("someRecordId", "test-dr2-ingest-adhoc-cache", expected_metadata)
        mock_send_message.assert_called_once_with("someRecordId", "test-dr2-ingest-adhoc-cache", "https://sqs.london-town.amazonaws.com/123456789/test-dr2-preingest-adhoc-importer")

    @patch("aws_interactions.send_sqs_message")
    @patch("aws_interactions.upload_metadata")
    @patch("aws_interactions.upload_file")
    @patch("aws_interactions.get_region")
    def test_should_send_the_files_to_the_s3_bucket_when_the_data_path_is_relative_with_mixed_forward_and_back_slashes(
            self, mock_region, mock_upload_file, mock_upload_metadata, mock_send_message):
        mock_region.return_value = "london-town"
        tmp1 = os.path.join(self.test_dir, "folder1/folder2/folder3/ad_hoc_ingest_test_file1.txt")
        os.makedirs(os.path.dirname(tmp1), exist_ok=True)
        with open(tmp1, "w") as f:
            f.write("temporary file one")

        metadata_csv_data = f"""Series,UUID,fileId,description,Filename,FileReference,ClientSideOriginalFilepath,formerRefDept,formerRefTNA,checksum_md5,checksum_sha256,IAID,digitalAssetSource
JS 8,someRecordId,someFileId,SomeDescription,JS-8-3.pdf,3,folder1\\folder2/folder3\\ad_hoc_ingest_test_file1.txt,dept_ref,tna_ref,,some_checksum,some_iaid,Surrogate"""

        tmp2 = os.path.join(self.test_dir, "metadata_to_ingest.csv")
        with open(tmp2, "w") as f:
            f.write(metadata_csv_data)

        args = SimpleNamespace(environment="test", input=f"{tmp2}")
        ad_hoc_ingest.upload_files(tmp2, "123456789", args)

        expected_metadata = {
            "Series": "JS 8",
            "UUID": "someRecordId",
            "fileId": "someFileId",
            "description": "SomeDescription",
            "Filename": "JS-8-3.pdf",
            "FileReference": "3",
            "ClientSideOriginalFilepath": "folder1\\folder2/folder3\\ad_hoc_ingest_test_file1.txt",
            "IAID": "some_iaid",
            "digitalAssetSource": "Surrogate",
            "formerRefDept": "dept_ref",
            "formerRefTNA": "tna_ref",
            "checksum_sha256": "some_checksum"
        }

        resolved_path = str(Path(tmp1).resolve())
        mock_upload_file.assert_called_once_with("someRecordId", "test-dr2-ingest-adhoc-cache", "someFileId", resolved_path)
        mock_upload_metadata.assert_called_once_with("someRecordId", "test-dr2-ingest-adhoc-cache", expected_metadata)
        mock_send_message.assert_called_once_with("someRecordId", "test-dr2-ingest-adhoc-cache",
                                                  "https://sqs.london-town.amazonaws.com/123456789/test-dr2-preingest-adhoc-importer")

    @patch("aws_interactions.send_sqs_message")
    @patch("aws_interactions.upload_metadata")
    @patch("aws_interactions.upload_file")
    @patch("aws_interactions.get_region")
    def test_should_create_metadata_with_description_having_comma_in_a_quoted_field(
            self, mock_region, mock_upload_file, mock_upload_metadata, mock_send_message):
        mock_region.return_value = "london-town"
        tmp1 = os.path.join(self.test_dir, "ad_hoc_ingest_test_file1.txt")
        with open(tmp1, "w") as f:
            f.write("temporary file one")

        metadata_csv_data = f"""Series,UUID,fileId,description,Filename,FileReference,ClientSideOriginalFilepath,formerRefDept,formerRefTNA,checksum_md5,checksum_sha256,IAID,digitalAssetSource
JS 8,someRecordId,someFileId,"Description of Kew, Richmond, London",JS-8-3.pdf,3,ad_hoc_ingest_test_file1.txt,,AB 1/2/3,,some_checksum,some_iaid,Born Digital"""

        tmp2 = os.path.join(self.test_dir, "metadata_to_ingest.csv")
        with open(tmp2, "w") as f:
            f.write(metadata_csv_data)

        args = SimpleNamespace(environment="test", input=f"{tmp2}")
        ad_hoc_ingest.upload_files(tmp2, "123456789", args)

        expected_metadata = {
            "Series": "JS 8",
            "UUID": "someRecordId",
            "fileId": "someFileId",
            "description": "Description of Kew, Richmond, London",
            "Filename": "JS-8-3.pdf",
            "FileReference": "3",
            "ClientSideOriginalFilepath": "ad_hoc_ingest_test_file1.txt",
            "IAID": "some_iaid",
            "digitalAssetSource": "Born Digital",
            "formerRefTNA": "AB 1/2/3",
            "checksum_sha256": "some_checksum"
        }

        resolved_path = str(Path(tmp1).resolve())
        mock_upload_file.assert_called_once_with("someRecordId", "test-dr2-ingest-adhoc-cache", "someFileId", resolved_path)
        mock_upload_metadata.assert_called_once_with("someRecordId", "test-dr2-ingest-adhoc-cache", expected_metadata)
        mock_send_message.assert_called_once_with("someRecordId", "test-dr2-ingest-adhoc-cache",
                                                  "https://sqs.london-town.amazonaws.com/123456789/test-dr2-preingest-adhoc-importer")


    @patch("aws_interactions.get_account_number")
    @patch("aws_interactions.refresh_session")
    @patch("builtins.input", return_value="")
    def test_should_call_refresh_session_when_a_client_error_is_thrown_when_getting_an_account_number(self, mock_input, mock_refresh_session, mock_get_account_number):
        mock_get_account_number.return_value = "123456789"
        ad_hoc_ingest.get_account_number()
        mock_refresh_session.assert_not_called()

        error_response = {"Error": {"Code": "TokenExpired", "Message": "Token has expired"}}
        mock_get_account_number.side_effect = [ClientError(error_response, "sts get identity"), "987654321"]
        account_number_after_refresh = ad_hoc_ingest.get_account_number()
        mock_refresh_session.assert_called_once()
        self.assertEqual("987654321", account_number_after_refresh)

    @patch("version_check.is_latest_version", return_value=False)
    @patch("builtins.input", return_value="n")
    def test_should_exit_when_not_at_latest_version_and_user_declines_to_continue(self, mock_input, _):
        with self.assertRaises(SystemExit) as exc:
            ad_hoc_ingest.main()
        self.assertEqual(0, exc.exception.code)
        mock_input.assert_called_once_with("Adhoc ingest is not at the latest version. Do you want to continue? y/n")

    @patch("version_check.is_latest_version", return_value=False)
    @patch("builtins.input", return_value="no")
    def test_should_exit_when_not_at_latest_version_and_user_types_no(self, _, __):
        with self.assertRaises(SystemExit) as exc:
            ad_hoc_ingest.main()
        self.assertEqual(0, exc.exception.code)

    @patch("argument_parser_builder.build")
    @patch("version_check.is_latest_version", return_value=False)
    @patch("builtins.input", return_value="y")
    def test_should_continue_when_not_at_latest_version_and_user_confirms_with_y(self, _, __, mock_build):
        mock_build.side_effect = Exception("Version check passed")
        with self.assertRaises(Exception) as e:
            ad_hoc_ingest.main()
        self.assertEqual("Version check passed", str(e.exception))

    @patch("argument_parser_builder.build")
    @patch("version_check.is_latest_version", return_value=False)
    @patch("builtins.input", return_value="Y")
    def test_should_continue_when_not_at_latest_version_and_user_confirms_with_y_uppercase(self, _, __, mock_build):
        mock_build.side_effect = Exception("Version check passed")
        with self.assertRaises(Exception) as e:
            ad_hoc_ingest.main()
        self.assertEqual("Version check passed", str(e.exception))

    @patch("argument_parser_builder.build")
    @patch("version_check.is_latest_version", return_value=False)
    @patch("builtins.input", return_value="yes")
    def test_should_continue_when_not_at_latest_version_and_user_confirms_with_yes(self, _, __, mock_build):
        mock_build.side_effect = Exception("Version check passed")
        with self.assertRaises(Exception) as e:
            ad_hoc_ingest.main()
        self.assertEqual("Version check passed", str(e.exception))

    @patch("argument_parser_builder.build")
    @patch("version_check.is_latest_version", return_value=False)
    @patch("builtins.input", return_value="YES")
    def test_should_continue_when_not_at_latest_version_and_user_confirms_with_yes(self, _, __, mock_build):
        mock_build.side_effect = Exception("Version check passed")
        with self.assertRaises(Exception) as e:
            ad_hoc_ingest.main()
        self.assertEqual("Version check passed", str(e.exception))

    @patch("argument_parser_builder.build")
    @patch("version_check.is_latest_version", return_value=True)
    def test_should_skip_version_prompt_when_already_at_latest_version(self, _, mock_build):
        mock_build.side_effect = Exception("Version check passed")
        with patch("builtins.input") as mock_input:
            with self.assertRaises(Exception) as e:
                ad_hoc_ingest.main()
        self.assertEqual("Version check passed", str(e.exception))
        mock_input.assert_not_called()

    @patch("aws_interactions.get_account_number")
    @patch("aws_interactions.refresh_session")
    @patch("builtins.input", return_value="")
    def test_should_call_refresh_session_three_times_before_terminating(self, mock_input, mock_refresh_session, mock_get_account_number):
        error_response = {"Error": {"Code": "TokenExpired", "Message": "Token has expired"}}
        mock_get_account_number.side_effect = [ClientError(error_response, "sts get identity"),
                                               ClientError(error_response, "sts get identity"),
                                               ClientError(error_response, "sts get identity"),
                                               ClientError(error_response, "sts get identity")]
        with self.assertRaises(Exception) as e:
            ad_hoc_ingest.get_account_number()

        self.assertEqual(3, mock_refresh_session.call_count)

    @patch("discovery_client.is_discovery_api_reachable")
    @patch("ad_hoc_ingest.upload_files_to_ingest_bucket")
    @patch("ad_hoc_ingest.get_input_dataset")
    @patch("dataset_validator.validate_dataset")
    @patch("ad_hoc_ingest.validate_arguments")
    @patch("version_check.is_latest_version")
    def test_should_call_discovery_when_description_is_not_supplied_within_the_input(self, mock_version_check,  mock_validate_arguments, mock_validate_dataset, mock_get_input_dataset, mock_upload_to_ingest_bucket, mock_is_discovery_reachable):
        csv_data = f"""catRef,Filename,checksum
        JS 8/3,JS-8-3.pdf,some_checksum"""
        data_set = pd.read_csv(StringIO(csv_data))

        mock_version_check.return_value = True
        mock_validate_arguments.return_value = True
        mock_get_input_dataset.return_value = data_set
        mock_validate_dataset.return_value = True

        with patch(
            "sys.argv",
            [
                "ad_hoc_ingest.py",
                "--environment",
                "test",
                "--input",
                "/home/users/input-file.csv",
                "--asset-source",
                "Surrogate",
                "--dry-run",
                "--output",
                "/home/some/folder",
            ],
        ):
            ad_hoc_ingest.main()

        mock_is_discovery_reachable.assert_called_once()
        actual_args = mock_upload_to_ingest_bucket.call_args.args
        self.assertEqual(False, actual_args[3])

    @patch("discovery_client.is_discovery_api_reachable")
    @patch("ad_hoc_ingest.upload_files_to_ingest_bucket")
    @patch("ad_hoc_ingest.get_input_dataset")
    @patch("dataset_validator.validate_dataset")
    @patch("ad_hoc_ingest.validate_arguments")
    @patch("version_check.is_latest_version")
    def test_should_not_call_discovery_when_description_is_supplied_within_the_input(self, mock_version_check,  mock_validate_arguments, mock_validate_dataset, mock_get_input_dataset, mock_upload_to_ingest_bucket, mock_is_discovery_reachable):
        csv_data = f"""catRef,description,Filename,checksum
        JS 8/3,"Description of Kew, Richmond, London",JS-8-3.pdf,some_checksum"""
        data_set = pd.read_csv(StringIO(csv_data))

        mock_version_check.return_value = True
        mock_validate_arguments.return_value = True
        mock_get_input_dataset.return_value = data_set
        mock_validate_dataset.return_value = True

        with patch(
            "sys.argv",
            [
                "ad_hoc_ingest.py",
                "--environment",
                "test",
                "--input",
                "/home/users/input-file.csv",
                "--asset-source",
                "Surrogate",
                "--dry-run",
                "--output",
                "/home/some/folder",
            ],
        ):
            ad_hoc_ingest.main()

        mock_is_discovery_reachable.assert_not_called()
        actual_args = mock_upload_to_ingest_bucket.call_args.args
        self.assertEqual(True, actual_args[3])

    @patch("discovery_client.is_discovery_api_reachable")
    @patch("ad_hoc_ingest.upload_files_to_ingest_bucket")
    @patch("ad_hoc_ingest.get_input_dataset")
    @patch("dataset_validator.validate_dataset")
    @patch("ad_hoc_ingest.validate_arguments")
    @patch("version_check.is_latest_version")
    def test_should_report_error_when_discovery_is_not_reachable(self, mock_version_check,  mock_validate_arguments, mock_validate_dataset, mock_get_input_dataset, mock_upload_to_ingest_bucket, mock_is_discovery_reachable):
        csv_data = f"""catRef,Filename,checksum
        JS 8/3,JS-8-3.pdf,some_checksum"""
        data_set = pd.read_csv(StringIO(csv_data))

        mock_is_discovery_reachable.return_value = False
        mock_version_check.return_value = True
        mock_validate_arguments.return_value = True
        mock_get_input_dataset.return_value = data_set
        mock_validate_dataset.return_value = True

        with patch(
            "sys.argv",
            [
                "ad_hoc_ingest.py",
                "--environment",
                "test",
                "--input",
                "/home/users/input-file.csv",
                "--asset-source",
                "Surrogate",
                "--dry-run",
                "--output",
                "/home/some/folder",
            ],
        ):
            with self.assertRaises(SystemExit) as ex:
                ad_hoc_ingest.main()

            self.assertEqual(1, ex.exception.code)
