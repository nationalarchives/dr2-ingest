import io

from lambda_function import lambda_handler, get_files_s3_client, process_series_and_catalogue_reference
import unittest
from unittest.mock import patch, MagicMock
import os
import json
import uuid

import sys

sys.modules['boto3'] = MagicMock()
sys.modules['jsonschema'] = MagicMock()


class TestLambdaHandler(unittest.TestCase):

    def setUp(self):
        self.event = {
            "Records": [
                {
                    "body": json.dumps({
                        "metadataLocation": "s3://test-metadata-bucket/test-key"
                    })
                }
            ]
        }
        os.environ["OUTPUT_BUCKET_NAME"] = "output-bucket"
        os.environ["REPLICA_BUCKET_NAME"] = "replica-bucket"
        os.environ["OUTPUT_QUEUE_URL"] = "queue-url"
        os.environ["ROLE_TO_ASSUME"] = "assumed-role-arn"
        os.environ["FILES_BUCKET"] = "files-bucket"

        self.valid_metadata = [{
            "UUID": str(uuid.uuid4()),
            "fileId": "file-123",
            "Filename": "file-123.txt",
            "Series": "A/B",
            "FileReference": "A/B"
        }]

    @patch("lambda_function.sqs_client")
    @patch("lambda_function.s3_client")
    @patch("lambda_function.get_files_s3_client")
    @patch("lambda_function.get_json_metadata")
    @patch("lambda_function.validate_mandatory_fields_exist")
    @patch("lambda_function.validate_formats")
    def test_import(
            self, mock_validate_formats, mock_validate_fields, mock_get_json, mock_get_files_s3, mock_s3,
            mock_sqs
    ):
        mock_get_json.return_value = self.valid_metadata
        files_s3 = MagicMock()
        mock_get_files_s3.return_value = files_s3
        asset_uuid = self.valid_metadata[0]['UUID']

        lambda_handler(self.event, None)

        files_s3.copy.assert_called_with(
            {"Bucket": "files-bucket", "Key": "file-123"},
            "output-bucket",
            f"{self.valid_metadata[0]['UUID']}/file-123"
        )
        expected_metadata = [{
            "UUID": asset_uuid,
            "fileId": "file-123",
            "Filename": "file-123.txt",
            "Series": "YA B",
            "FileReference": "YA/B"
        }]

        file_upload_args = mock_s3.upload_fileobj.call_args_list[0].args
        self.assertEqual(json.loads(file_upload_args[0].getvalue().decode("utf-8")), expected_metadata)
        self.assertEqual(file_upload_args[1], "output-bucket")
        self.assertEqual(file_upload_args[2], f"{asset_uuid}.metadata")

        mock_sqs.send_message.assert_called()
        mock_validate_fields.assert_called()
        mock_validate_formats.assert_called()

    @patch("lambda_function.sqs_client")
    @patch("lambda_function.s3_client")
    @patch("lambda_function.get_files_s3_client")
    @patch("lambda_function.get_json_metadata")
    @patch("lambda_function.validate_mandatory_fields_exist")
    @patch("lambda_function.validate_formats")
    def test_sqs_failure(
            self, _, __, mock_get_json, mock_get_files_s3, ___, mock_sqs
    ):
        mock_get_json.return_value = self.valid_metadata
        mock_get_files_s3.return_value = MagicMock()
        mock_sqs.send_message.side_effect = Exception("SQS Send Failed")
        with self.assertRaises(Exception) as cm:
            lambda_handler(self.event, None)
        self.assertIn("SQS Send Failed", str(cm.exception))

    @patch("lambda_function.s3_client")
    @patch("lambda_function.get_files_s3_client")
    @patch("lambda_function.get_json_metadata")
    @patch("lambda_function.validate_mandatory_fields_exist")
    @patch("lambda_function.validate_formats")
    def test_s3_failure(
            self, _, __, mock_get_json, mock_get_files_s3, mock_s3

    ):
        mock_get_json.return_value = self.valid_metadata
        mock_get_files_s3.return_value = MagicMock()
        mock_s3.upload_fileobj.side_effect = Exception("S3 Metadata Upload Failed")
        with self.assertRaises(Exception) as cm:
            lambda_handler(self.event, None)
        self.assertIn("S3 Metadata Upload Failed", str(cm.exception))

    @patch("lambda_function.get_files_s3_client")
    @patch("lambda_function.get_json_metadata")
    @patch("lambda_function.validate_mandatory_fields_exist")
    @patch("lambda_function.validate_formats")
    def test_files_s3_failure(
            self, _, __, mock_get_json, mock_get_files_s3
    ):
        mock_get_json.return_value = self.valid_metadata
        bad_files_s3 = MagicMock()
        bad_files_s3.copy.side_effect = Exception("FILES S3 Copy Failed")
        mock_get_files_s3.return_value = bad_files_s3
        with self.assertRaises(Exception) as cm:
            lambda_handler(self.event, None)
        self.assertIn("FILES S3 Copy Failed", str(cm.exception))

    @patch("lambda_function.get_files_s3_client")
    @patch("lambda_function.get_json_metadata")
    @patch("lambda_function.validate_mandatory_fields_exist")
    @patch("lambda_function.validate_formats")
    def test_validate_mandatory_fields_exist_failure(
            self, _, mock_validate_fields, mock_get_json, mock_get_files_s3
    ):
        mock_get_json.return_value = self.valid_metadata
        mock_get_files_s3.return_value = MagicMock()
        mock_validate_fields.side_effect = Exception("Validation Error")
        with self.assertRaises(Exception) as cm:
            lambda_handler(self.event, None)
        self.assertIn("Validation Error", str(cm.exception))

    @patch("lambda_function.get_files_s3_client")
    @patch("lambda_function.get_json_metadata")
    @patch("lambda_function.validate_mandatory_fields_exist")
    @patch("lambda_function.validate_formats")
    def test_validate_formats_failure(
            self, mock_validate_formats, _, mock_get_json, mock_get_files_s3
    ):
        mock_get_json.return_value = self.valid_metadata
        mock_get_files_s3.return_value = MagicMock()
        mock_validate_formats.side_effect = Exception("Formats Validation Error")
        with self.assertRaises(Exception) as cm:
            lambda_handler(self.event, None)
        self.assertIn("Formats Validation Error", str(cm.exception))

    @patch("lambda_function.get_files_s3_client")
    @patch("lambda_function.get_json_metadata")
    @patch("lambda_function.validate_mandatory_fields_exist")
    @patch("lambda_function.validate_formats")
    def test_get_json_metadata_failure(
            self, _, __, mock_get_json, mock_get_files_s3
    ):
        mock_get_json.side_effect = Exception("Get Metadata Failed")
        mock_get_files_s3.return_value = MagicMock()
        with self.assertRaises(Exception) as cm:
            lambda_handler(self.event, None)
        self.assertIn("Get Metadata Failed", str(cm.exception))

    @patch("lambda_function.get_files_s3_client")
    @patch("lambda_function.get_json_metadata")
    @patch("lambda_function.validate_mandatory_fields_exist")
    @patch("lambda_function.validate_formats")
    def test_get_files_s3_client_failure(
            self, _, __, mock_get_json, mock_get_files_s3
    ):
        mock_get_json.return_value = self.valid_metadata
        mock_get_files_s3.side_effect = Exception("Assume Role Failed")
        with self.assertRaises(Exception) as cm:
            lambda_handler(self.event, None)
        self.assertIn("Assume Role Failed", str(cm.exception))

    @patch("lambda_function.sts_client")
    def test_get_assumed_role_s3_client(self, sts_client):
        creds = {
            'Credentials': {'AccessKeyId': 'test-id', 'SecretAccessKey': 'test-secret', 'SessionToken': 'test-token'}
        }
        sts_client.assume_role.return_value = creds
        s3_client = get_files_s3_client("test-role")
        response_creds = s3_client._get_credentials()
        self.assertEqual(response_creds.access_key, 'test-id')
        self.assertEqual(response_creds.secret_key, 'test-secret')
        self.assertEqual(response_creds.token, 'test-token')

    def test_series_and_reference_modification(self):
        def check(series, reference, series_result, reference_result):
            metadata = {'Series': series, 'FileReference': reference}
            process_series_and_catalogue_reference(metadata)
            self.assertEqual(metadata, {'Series': series_result, 'FileReference': reference_result})

        check("AB/1", "AB/1/2/3", "YAB 1", "YAB/1/2/3")
        check("AB 1", "AB/1/2/3", "YAB 1", "YAB/1/2/3")
        check("ABCD/1", "ABCD/1/2/3", "YABC 1", "YABC/1/2/3")


if __name__ == "__main__":
    unittest.main()
