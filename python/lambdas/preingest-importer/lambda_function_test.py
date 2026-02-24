import json
import os
import unittest
import uuid
from unittest.mock import patch

from botocore.exceptions import ClientError

import lambda_function
from test_utils import copy_helper


@patch.dict('os.environ', {'OUTPUT_BUCKET_NAME': 'destination-bucket'})
@patch.dict('os.environ', {'OUTPUT_QUEUE_URL': 'destination-queue'})
@patch.dict('os.environ', {'SOURCE_SYSTEM': 'dri'})
class TestLambdaFunction(unittest.TestCase):
    @staticmethod
    def valid_metadata():
        return {
            "Filename": "name",
            "ConsignmentReference": "TDR-2024-PQXN",
            "FileReference": "ZDSCFC",
            "Series": "MOCK1 123",
            "TransferInitiatedDatetime": "2024-09-19 07:21:57",
            "UUID": "0000c951-b332-4d45-93e7-8c24eec4b1f1",
            "TransferringBody": "Body",
            "SHA256ServerSideChecksum": "checksum"
        }

    @patch('lambda_function.s3_client.list_objects')
    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.copy_objects')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.s3_client.get_object')
    @patch('lambda_function.s3_client.delete_objects')
    @patch('lambda_function.validate_mandatory_fields_exist')
    @patch('lambda_function.validate_formats')
    @patch.dict(os.environ, {'OUTPUT_BUCKET_NAME': 'destination-bucket'})
    @patch.dict(os.environ, {'SOURCE_SYSTEM': 'dri'})
    def test_copy(self, mock_validate_formats, mock_validate_mandatory_fields_exist, mock_delete_object, mock_get_object,
                  mock_send_message, mock_copy, mock_head_object, mock_list_objects):
        copy_helper(self, mock_validate_formats, mock_validate_mandatory_fields_exist, mock_get_object, mock_delete_object,
                    mock_send_message, mock_copy, mock_head_object, mock_list_objects)

    @patch('lambda_function.s3_client.list_objects')
    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.copy_objects')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.s3_client.get_object')
    @patch('lambda_function.s3_client.delete_objects')
    @patch('lambda_function.validate_mandatory_fields_exist')
    @patch('lambda_function.validate_formats')
    @patch.dict(os.environ, {'OUTPUT_BUCKET_NAME': 'destination-bucket'})
    @patch.dict(os.environ, {'SOURCE_SYSTEM': 'dri'})
    @patch.dict(os.environ, {'DELETE_FROM_SOURCE': 'true'})
    def test_copy_with_source_delete(self, mock_validate_formats, mock_validate_mandatory_fields_exist, mock_delete_object,
                  mock_get_object, mock_send_message, mock_copy, mock_head_object, mock_list_objects):
        copy_helper(self, mock_validate_formats, mock_validate_mandatory_fields_exist, mock_get_object, mock_delete_object,
                    mock_send_message, mock_copy, mock_head_object, mock_list_objects, should_delete=True)

    @patch('lambda_function.s3_client.list_objects')
    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.copy_objects')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.s3_client.get_object')
    @patch('lambda_function.s3_client.delete_objects')
    @patch('lambda_function.validate_mandatory_fields_exist')
    @patch('lambda_function.validate_formats')
    @patch.dict(os.environ, {'DESTINATION_BUCKET': 'destination-bucket'})
    @patch.dict(os.environ, {'SOURCE_SYSTEM': 'dri'})
    def test_copy_returns_messageId_when_in_body(self, mock_validate_formats, mock_validate_mandatory_fields_exist,
                                                 mock_delete_objects, mock_get_object, mock_send_message, mock_copy_objects,
                                                 mock_head_object, mock_list_objects):
        copy_helper(self, mock_validate_formats, mock_validate_mandatory_fields_exist, mock_get_object,
                    mock_delete_objects, mock_send_message, mock_copy_objects,
                    mock_head_object, mock_list_objects, potential_message_id="message-id")

    @patch('lambda_function.s3_client.list_objects')
    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.copy_object')
    @patch('lambda_function.validate_metadata')
    def test_copy_object_failure(self, mock_validate_metadata, mock_copy_object, mock_head_object, mock_list_objects):
        content_length = 1024
        asset_id, file_id = str(uuid.uuid4()), str(uuid.uuid4())
        key = f'{asset_id}/{file_id}'
        contents = [{'Size': content_length, 'Key': key}, {'Size': 1, 'Key': f'{asset_id}.metadata'}]
        mock_list_objects.return_value = {'Contents': contents, 'IsTruncated': False}
        mock_head_object.return_value = {'ContentLength': content_length}  # 1 KB
        mock_validate_metadata.return_value = True
        mock_copy_object.side_effect = Exception("S3 copy_object failed")
        event = {
            'Records': [
                {'body': f'{{"bucket": "source-bucket","fileId":"{asset_id}"}}'}
            ]
        }
        context = {}
        with self.assertRaises(Exception) as cm:
            lambda_function.lambda_handler(event, context)
        self.assertEqual("S3 copy_object failed", str(cm.exception))

    @patch('lambda_function.s3_client.list_objects')
    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.create_multipart_upload')
    @patch('lambda_function.s3_client.upload_part_copy')
    @patch('lambda_function.s3_client.complete_multipart_upload')
    @patch('lambda_function.s3_client.abort_multipart_upload')
    @patch('lambda_function.validate_metadata')
    def test_multipart_upload_failure(self, mock_validate_metadata, abort_multipart_upload, __,
                                      mock_upload_part_copy, mock_create_multipart_upload,
                                      mock_head_object, mock_list_objects):
        content_length = 5 * 1024 * 1024 * 1024
        asset_id, file_id = str(uuid.uuid4()), str(uuid.uuid4())
        key = f'{asset_id}/{file_id}'
        contents = [{'Size': content_length, 'Key': key}, {'Size': 1, 'Key': f'{asset_id}.metadata'}]
        mock_list_objects.return_value = {'Contents': contents, 'IsTruncated': False}
        mock_head_object.return_value = {'ContentLength': content_length}
        mock_create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}

        mock_upload_part_copy.side_effect = Exception("S3 upload_part_copy failed")
        mock_validate_metadata.return_value = True

        event = {
            'Records': [
                {'body': f'{{"bucket": "source-bucket","fileId":"{asset_id}"}}'}
            ]
        }
        context = {}
        with self.assertRaises(Exception) as cm:
            lambda_function.lambda_handler(event, context)
        self.assertEqual("S3 upload_part_copy failed", str(cm.exception))
        expected_abort_call = {'Bucket': 'destination-bucket', 'Key': key, 'UploadId': 'test-upload-id'}
        self.assertEqual(expected_abort_call, abort_multipart_upload.call_args_list[0][1])

    @patch('lambda_function.s3_client.list_objects')
    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.create_multipart_upload')
    @patch('lambda_function.s3_client.upload_part_copy')
    @patch('lambda_function.s3_client.complete_multipart_upload')
    @patch('lambda_function.s3_client.abort_multipart_upload')
    @patch('lambda_function.validate_metadata')
    def test_complete_multipart_upload_failure(self, mock_validate_metadata, abort_multipart_upload,
                                               mock_complete_multipart_upload,
                                               mock_upload_part_copy, mock_create_multipart_upload, mock_head_object, mock_list_objects):

        asset_id, file_id = str(uuid.uuid4()), str(uuid.uuid4())
        key = f'{asset_id}/{file_id}'
        content_length = 5 * 1024 * 1024 * 1024
        contents = [{'Size': content_length, 'Key': key}, {'Size': 1, 'Key': f'{asset_id}.metadata'}]
        mock_list_objects.return_value = {'Contents': contents, 'IsTruncated': False}
        mock_head_object.return_value = {'ContentLength': content_length}
        mock_create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
        mock_upload_part_copy.return_value = {'CopyPartResult': {'ETag': 'etag'}}
        mock_validate_metadata.return_value = True
        mock_complete_multipart_upload.side_effect = Exception("S3 complete_multipart_upload failed")

        event = {
            'Records': [
                {'body': f'{{"bucket": "source-bucket","fileId":"{asset_id}"}}'}
            ]
        }
        context = {}
        with self.assertRaises(Exception) as cm:
            lambda_function.lambda_handler(event, context)
        self.assertEqual("S3 complete_multipart_upload failed", str(cm.exception))
        expected_abort_call = {'Bucket': 'destination-bucket', 'Key': key, 'UploadId': 'test-upload-id'}
        self.assertEqual(expected_abort_call, abort_multipart_upload.call_args_list[0][1])

    @patch('lambda_function.s3_client.list_objects')
    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.copy_object')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.validate_metadata')
    def test_send_message_failure(self, mock_validate_metadata, mock_send_message, _, mock_head_object,
                                  mock_list_objects):
        content_length = 1024
        asset_id, file_id = str(uuid.uuid4()), str(uuid.uuid4())
        key = f'{asset_id}/{file_id}'
        contents = [{'Size': content_length, 'Key': key}, {'Size': 1, 'Key': f'{asset_id}.metadata'}]
        mock_list_objects.return_value = {'Contents': contents, 'IsTruncated': False}
        mock_head_object.return_value = {'ContentLength': content_length}
        mock_validate_metadata.return_value = True
        event = {
            'Records': [
                {'body': f'{{"bucket": "source-bucket","fileId":"{asset_id}"}}'}
            ]
        }
        context = {}
        mock_send_message.side_effect = Exception("SQS Send message failed")

        with self.assertRaises(Exception) as cm:
            lambda_function.lambda_handler(event, context)
        self.assertEqual("SQS Send message failed", str(cm.exception))

    @patch('lambda_function.s3_client.delete_objects')
    @patch('lambda_function.s3_client.list_objects')
    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.copy_object')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.validate_metadata')
    @patch.dict(os.environ, {'DELETE_FROM_SOURCE': 'true'})
    def test_delete_failure_should_not_send_message(self, mock_validate_metadata, mock_send_message, _, mock_head_object,
                                  mock_list_objects, mock_delete_objects):
        content_length = 1024
        asset_id, file_id = str(uuid.uuid4()), str(uuid.uuid4())
        key = f'{asset_id}/{file_id}'
        contents = [{'Size': content_length, 'Key': key}, {'Size': 1, 'Key': f'{asset_id}.metadata'}]
        mock_list_objects.return_value = {'Contents': contents, 'IsTruncated': False}
        mock_head_object.return_value = {'ContentLength': content_length}
        mock_validate_metadata.return_value = True
        event = {
            'Records': [
                {'body': f'{{"bucket": "source-bucket","fileId":"{asset_id}"}}'}
            ]
        }
        context = {}
        mock_delete_objects.side_effect = Exception("Delete object failed")

        with self.assertRaises(Exception) as cm:
            lambda_function.lambda_handler(event, context)
        self.assertEqual("Delete object failed", str(cm.exception))

        self.assertEqual(0, mock_send_message.call_count)

    def test_should_successfully_validate_when_the_fields_are_valid(self):
        mock_response_body = json.dumps(self.valid_metadata())
        schema_location = "common/preingest-tdr/metadata-schema.json"
        result = lambda_function.validate_mandatory_fields_exist(schema_location, json.loads(mock_response_body))
        self.assertEqual(True, result)

    @patch('lambda_function.s3_client.list_objects')
    def test_should_raise_an_exception_when_the_file_does_not_exist_in_source_bucket(self, mock_list_objects):
        error_response = {
            'Error': {
                'Code': '404',
                'Message': 'Not Found'
            }
        }

        client_error = ClientError(error_response, 'ListObjects')

        mock_list_objects.side_effect = client_error
        with self.assertRaises(Exception) as ex:
            lambda_function.assert_objects_exist_in_bucket('some_bucket', 'some_key')

        self.assertEqual(
            "Object 'some_key' does not exist in 'some_bucket', underlying error is: 'An error occurred (404) when "
            "calling the ListObjects operation: Not Found'",
            str(ex.exception))

    @patch('lambda_function.s3_client.list_objects')
    def test_should_raise_exception_when_file_exists_but_corresponding_metadata_file_does_not_exist_in_source_bucket(
            self, mock_list_objects):
        asset_id, file_id = str(uuid.uuid4()), str(uuid.uuid4())
        key = f'{asset_id}/{file_id}'
        contents = [{'Size': 1, 'Key': key}]
        mock_list_objects.return_value = {'Contents': contents, 'IsTruncated': False}

        with self.assertRaises(Exception) as ex:
            lambda_function.assert_objects_exist_in_bucket('some_bucket', 'some_key')

        self.assertEqual(
            "Object 'some_key.metadata' does not exist in 'some_bucket'",
            str(ex.exception))

    def test_should_raise_an_exception_if_fields_are_missing(self):
        schema_location = "common/preingest-tdr/metadata-schema.json"
        with open(schema_location, "r") as metadata_schema_file:
            metadata_schema = json.load(metadata_schema_file)
        required_fields = metadata_schema["required"]
        for field in required_fields:

            invalid_metadata = self.valid_metadata().copy()
            invalid_metadata.pop(field)

            with self.assertRaises(Exception) as ex:

                lambda_function.validate_mandatory_fields_exist(schema_location, invalid_metadata)
            self.assertEqual(f"'{field}' is a required property", str(ex.exception))

    def test_should_raise_an_exception_if_fields_are_wrong_type(self):
        schema_location = "common/preingest-tdr/metadata-schema.json"
        with open(schema_location, "r") as metadata_schema_file:
            metadata_schema = json.load(metadata_schema_file)
        properties = metadata_schema["properties"]

        for json_property in properties:
            property_value = properties[json_property]
            property_value_type = property_value["type"]
            if property_value_type == "string":
                test_values = [1, False, None, ["test"], {"test": "value"}]
            elif property_value.get("format") == "uuid":
                test_values = ["test", 1, None, False, ["test"], {"test": "value"}]
            elif type(property_value_type) is list and "string" in property_value_type and "null" in property_value_type:
                test_values = [1, False, ["test"], {"test": "value"}]
            else:
                raise "Unexpected property value"

            for test_value in test_values:
                invalid_metadata = self.valid_metadata().copy()
                invalid_metadata[json_property] = test_value
                with self.assertRaises(Exception) as ex:
                    lambda_function.validate_mandatory_fields_exist(schema_location, invalid_metadata)

                if "format" in property_value and test_value is str:
                    format_type = property_value["format"]
                    err_msg = f"{test_value} is not a '{format_type}'"
                elif type(property_value_type) is list:
                    format_type = "', '".join(property_value_type)
                    err_msg = f"{test_value} is not of type '{format_type}'"
                else:
                    err_msg = f"{test_value} is not of type '{property_value_type}'"
                self.assertEqual(err_msg, str(ex.exception))

    def test_should_raise_an_exception_when_UUID_is_invalid(self):
        mock_response_body = """{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series":"MOCK1 123", 
                "TransferInitiatedDatetime": "2024-09-19 07:21:57", "UUID": "invalid-uuid-format"}"""

        with self.assertRaises(Exception) as ex:
            lambda_function.validate_formats(json.loads(mock_response_body), 'any_bucket_patched', 'any_key_patched')
        self.assertEqual("Unable to parse UUID, 'invalid-uuid-format' from file 'any_key_patched' in "
                         "bucket 'any_bucket_patched'. Invalid format", str(ex.exception))

    def test_should_raise_an_exception_when_series_is_empty(self):
        mock_response_body = """{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series": " ", "TransferInitiatedDatetime": "2024-09-19 07:21:57", "UUID": "bb7bb923-b82c-4203-a3c1-ea3f362ef4da"}"""
        with self.assertRaises(Exception) as ex:
            lambda_function.validate_formats(json.loads(mock_response_body), 'any_bucket_patched', 'any_key_patched')
        self.assertEqual(
            "Empty Series value in file 'any_key_patched' in bucket 'any_bucket_patched'. Unable to proceed",
            str(ex.exception))

    def test_should_raise_an_exception_when_series_format_is_invalid(self):
        invalid_series_names = ["MOCK1 12345", "1LEV 12", "abc 12", "MOCKT 1", "UNKNOWN", "LEV 2   ", "FLM 012",
                                "SDF 12345", " WER 12"]
        for invalid_series in invalid_series_names:
            mock_response_body = f"""{{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series": "{invalid_series}", "TransferInitiatedDatetime": "2024-09-19 07:21:57", "UUID": "bb7bb923-b82c-4203-a3c1-ea3f362ef4da"}}"""
            with self.assertRaises(Exception) as ex:
                lambda_function.validate_formats(json.loads(mock_response_body), 'any_bucket_patched',
                                                 'any_key_patched')
            self.assertEqual(
                f"Invalid Series value, '{invalid_series}' in file 'any_key_patched' in bucket 'any_bucket_patched'. Unable to proceed",
                str(ex.exception))

    def test_should_successfully_validate_various_allowed_formats_of_series_name(self):
        valid_series_names = ["MOCK1 123", "Unknown", "UKSC 1", "LEV 2", "PRO 234", "A 7"]
        for valid_series in valid_series_names:
            mock_response_body = f"""{{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series": "{valid_series}", "TransferInitiatedDatetime": "2024-09-19 07:21:57", "UUID": "bb7bb923-b82c-4203-a3c1-ea3f362ef4da"}}"""
            self.assertEqual(True,
                             lambda_function.validate_formats(json.loads(mock_response_body), 'any_bucket_patched',
                                                              'any_key_patched'))


if __name__ == '__main__':
    unittest.main()
