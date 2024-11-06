import json
import os
import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

import lambda_function
from test_utils import copy_helper


@patch.dict('os.environ', {'DESTINATION_BUCKET': 'destination-bucket'})
@patch.dict('os.environ', {'DESTINATION_QUEUE': 'destination-queue'})
class TestLambdaFunction(unittest.TestCase):
    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.create_multipart_upload')
    @patch('lambda_function.s3_client.upload_part_copy')
    @patch('lambda_function.s3_client.complete_multipart_upload')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.s3_client.get_object')
    @patch('lambda_function.validate_mandatory_fields_exist')
    @patch('lambda_function.validate_formats')
    @patch.dict(os.environ, {'DESTINATION_BUCKET': 'destination-bucket'})
    def test_copy(self, mock_validate_formats, mock_validate_mandatory_fields_exist, mock_get_object,
                  mock_send_message, mock_complete_multipart_upload, _,
                  mock_create_multipart_upload,
                  mock_head_object):
        copy_helper(self, mock_validate_formats, mock_validate_mandatory_fields_exist, mock_get_object,
                         mock_send_message, mock_complete_multipart_upload, _,
                         mock_create_multipart_upload,
                         mock_head_object, {'body': '{"bucket": "source-bucket","fileId":"test-file"}'},
                         '{"id": "test-file", "location": "s3://destination-bucket/test-file"}')

    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.create_multipart_upload')
    @patch('lambda_function.s3_client.upload_part_copy')
    @patch('lambda_function.s3_client.complete_multipart_upload')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.s3_client.get_object')
    @patch('lambda_function.validate_mandatory_fields_exist')
    @patch('lambda_function.validate_formats')
    @patch.dict(os.environ, {'DESTINATION_BUCKET': 'destination-bucket'})
    def test_copy_returns_messageId_when_in_body(self, mock_validate_formats, mock_validate_mandatory_fields_exist,
                                                 mock_get_object, mock_send_message, mock_complete_multipart_upload, _,
                                                 mock_create_multipart_upload, mock_head_object):

        body_with_message_id = {'body': '{"bucket": "source-bucket","fileId":"test-file", "messageId": "message-id"}'}
        expected_message_id = ('{"id": "test-file", "location": "s3://destination-bucket/test-file", "messageId": '
                               '"message-id"}')
        copy_helper(self, mock_validate_formats, mock_validate_mandatory_fields_exist, mock_get_object,
                         mock_send_message, mock_complete_multipart_upload, _,
                         mock_create_multipart_upload,
                         mock_head_object, body_with_message_id, expected_message_id)

    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.copy_object')
    @patch('lambda_function.validate_metadata')
    def test_copy_object_failure(self, mock_validate_metadata, mock_copy_object, mock_head_object):
        mock_head_object.return_value = {'ContentLength': 1024}  # 1 KB
        mock_validate_metadata.return_value = True
        mock_copy_object.side_effect = Exception("S3 copy_object failed")
        event = {
            'Records': [
                {'body': '{"bucket": "source-bucket","fileId":"test-file"}'}
            ]
        }
        context = {}
        with self.assertRaises(Exception) as cm:
            lambda_function.lambda_handler(event, context)
        self.assertEqual(str(cm.exception), "S3 copy_object failed")

    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.create_multipart_upload')
    @patch('lambda_function.s3_client.upload_part_copy')
    @patch('lambda_function.s3_client.complete_multipart_upload')
    @patch('lambda_function.s3_client.abort_multipart_upload')
    @patch('lambda_function.validate_metadata')
    def test_multipart_upload_failure(self, mock_validate_metadata, abort_multipart_upload, __,
                                      mock_upload_part_copy, mock_create_multipart_upload, mock_head_object):
        mock_head_object.return_value = {'ContentLength': 5 * 1024 * 1024 * 1024}
        mock_create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}

        mock_upload_part_copy.side_effect = Exception("S3 upload_part_copy failed")
        mock_validate_metadata.return_value = True

        event = {
            'Records': [
                {'body': '{"bucket": "source-bucket","fileId":"test-file"}'}
            ]
        }
        context = {}
        with self.assertRaises(Exception) as cm:
            lambda_function.lambda_handler(event, context)
        self.assertEqual(str(cm.exception), "S3 upload_part_copy failed")
        expected_abort_call = {'Bucket': 'destination-bucket', 'Key': 'test-file', 'UploadId': 'test-upload-id'}
        self.assertEqual(abort_multipart_upload.call_args_list[0][1], expected_abort_call)

    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.create_multipart_upload')
    @patch('lambda_function.s3_client.upload_part_copy')
    @patch('lambda_function.s3_client.complete_multipart_upload')
    @patch('lambda_function.s3_client.abort_multipart_upload')
    @patch('lambda_function.validate_metadata')
    def test_complete_multipart_upload_failure(self, mock_validate_metadata, abort_multipart_upload,
                                               mock_complete_multipart_upload,
                                               mock_upload_part_copy, mock_create_multipart_upload, mock_head_object):
        mock_head_object.return_value = {'ContentLength': 5 * 1024 * 1024 * 1024}
        mock_create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
        mock_upload_part_copy.return_value = {'CopyPartResult': {'ETag': 'etag'}}
        mock_validate_metadata.return_value = True
        mock_complete_multipart_upload.side_effect = Exception("S3 complete_multipart_upload failed")

        event = {
            'Records': [
                {'body': '{"bucket": "source-bucket","fileId":"test-file"}'}
            ]
        }
        context = {}
        with self.assertRaises(Exception) as cm:
            lambda_function.lambda_handler(event, context)
        self.assertEqual(str(cm.exception), "S3 complete_multipart_upload failed")
        expected_abort_call = {'Bucket': 'destination-bucket', 'Key': 'test-file', 'UploadId': 'test-upload-id'}
        self.assertEqual(abort_multipart_upload.call_args_list[0][1], expected_abort_call)

    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.copy_object')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.validate_metadata')
    def test_send_message_failure(self, mock_validate_metadata, mock_send_message, _, mock_head_object):
        mock_head_object.return_value = {'ContentLength': 1024}  # 1 KB
        mock_validate_metadata.return_value = True
        event = {
            'Records': [
                {'body': '{"bucket": "source-bucket","fileId":"test-file"}'}
            ]
        }
        context = {}
        mock_send_message.side_effect = Exception("SQS Send message failed")

        with self.assertRaises(Exception) as cm:
            lambda_function.lambda_handler(event, context)
        self.assertEqual(str(cm.exception), "SQS Send message failed")

    def test_should_successfully_validate_when_the_fields_are_valid(self):
        mock_response_body = """{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series": "MOCK1 123", "TransferInitiatedDatetime": "2024-09-19 07:21:57","UUID": "0000c951-b332-4d45-93e7-8c24eec4b1f1"}"""
        result = lambda_function.validate_mandatory_fields_exist(json.loads(mock_response_body))
        self.assertEqual(True, result)

    def test_should_raise_an_exception_when_series_does_not_exist(self):
        mock_get_object_response = {
            'Body':
                '{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", '
                '"TransferInitiatedDatetime": "2024-09-19 07:21:57","UUID": "0000c951-b332-4d45-93e7-8c24eec4b1f1"}'
        }
        with self.assertRaises(Exception) as ex:
            lambda_function.validate_mandatory_fields_exist(mock_get_object_response)
        self.assertEqual("'Series' is a required property", str(ex.exception))

    def test_should_raise_an_exception_when_transfer_initiated_datetime_does_not_exist(self):
        mock_response_body = """{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series":"MOCK1 123", "UUID": "0000c951-b332-4d45-93e7-8c24eec4b1f1"}"""
        with self.assertRaises(Exception) as ex:
            lambda_function.validate_mandatory_fields_exist(json.loads(mock_response_body))
        self.assertEqual("'TransferInitiatedDatetime' is a required property", str(ex.exception))

    def test_should_raise_an_exception_when_UUID_does_not_exist(self):
        mock_response_body = """{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series":"MOCK1 123", 
        "TransferInitiatedDatetime": "2024-09-19 07:21:57"}"""

        with self.assertRaises(Exception) as ex:
            lambda_function.validate_mandatory_fields_exist(json.loads(mock_response_body))
        self.assertEqual("'UUID' is a required property", str(ex.exception))

    @patch('lambda_function.s3_client.head_object')
    def test_should_raise_an_exception_when_the_file_does_not_exist_in_source_bucket(self, mock_head_object):
        error_response = {
            'Error': {
                'Code': '404',
                'Message': 'Not Found'
            }
        }

        client_error = ClientError(error_response, 'HeadObject')

        mock_head_object.side_effect = client_error
        with self.assertRaises(Exception) as ex:
            lambda_function.assert_objects_exist_in_bucket('some_bucket', ['some_key'])

        self.assertEqual(
            "Object 'some_key' does not exist in 'some_bucket', underlying error is: 'An error occurred (404) when calling the HeadObject operation: Not Found'",
            str(ex.exception))

    @patch('lambda_function.s3_client.head_object')
    def test_should_raise_an_exception_when_the_file_exists_but_corresponding_metadata_file_does_not_exist_in_source_bucket(
            self,
            mock_head_object):
        def selective_error_object(Bucket, Key):
            if Key == 'some_key.metadata':
                error_response = {
                    'Error': {
                        'Code': '404',
                        'Message': 'Not Found'
                    }
                }
                raise ClientError(error_response, 'HeadObject')
            else:
                return {'ContentLength': 1024}

        mock_head_object.side_effect = selective_error_object
        with self.assertRaises(Exception) as ex:
            lambda_function.assert_objects_exist_in_bucket('some_bucket', ['some_key.metadata', 'some_key'])

        self.assertEqual(
            "Object 'some_key.metadata' does not exist in 'some_bucket', underlying error is: 'An error occurred (404) when calling the HeadObject operation: Not Found'",
            str(ex.exception))

    def test_should_raise_an_exception_when_UUID_is_invalid(self):
        # mock_response_body = """{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series":"MOCK1 123", "TransferInitiatedDatetime": "2024-09-19 07:21:57", "UUID": "invalid-uuid-format"}"""

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
