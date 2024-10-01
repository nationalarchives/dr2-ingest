import os
import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

import lambda_function


@patch.dict('os.environ', {'DESTINATION_BUCKET': 'destination-bucket'})
@patch.dict('os.environ', {'DESTINATION_QUEUE': 'destination-queue'})
class TestLambdaFunction(unittest.TestCase):
    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.create_multipart_upload')
    @patch('lambda_function.s3_client.upload_part_copy')
    @patch('lambda_function.s3_client.complete_multipart_upload')
    @patch('lambda_function.sqs_client.send_message')
    @patch.dict(os.environ, {'DESTINATION_BUCKET': 'destination-bucket'})
    @patch('lambda_function.validate_metadata')
    def test_copy(self, mock_validate_metadata, mock_send_message, mock_complete_multipart_upload, _,
                  mock_create_multipart_upload,
                  mock_head_object):
        content_length = 5 * 1024 * 1024 * 1024
        mock_head_object.return_value = {'ContentLength': content_length}
        mock_create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
        mock_validate_metadata.return_value = True

        event = {
            'Records': [
                {'body': '{"bucket": "source-bucket","fileId":"test-file"}'}
            ]
        }

        context = {}
        lambda_function.lambda_handler(event, context)

        expected_body = '{"id": "test-file", "location": "s3://destination-bucket/test-file"}'
        expected_sqs_args = {'MessageBody': expected_body, 'QueueUrl': 'destination-queue'}


        self.assertEqual(mock_create_multipart_upload.call_count, 2)

        def assert_create_multipart_arguments(idx, key):
            self.assertEqual(mock_create_multipart_upload.call_args_list[idx][1], {'Bucket': 'destination-bucket',
                                                                                   'Key': key})

        assert_create_multipart_arguments(0, "test-file")
        assert_create_multipart_arguments(1, "test-file.metadata")
        self.assertEqual(mock_complete_multipart_upload.call_count, 2)
        self.assertEqual(mock_send_message.call_args_list[0][1], expected_sqs_args)

    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.get_object')
    def test_head_object_failure(self, mock_get_object, mock_head_object):
        mock_get_object.return_value = {
            'Body':
                '{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series":"MOCK1 123", "TransferInitiatedDatetime": "2024-09-19 07:21:57","UUID": "0000c951-b332-4d45-93e7-8c24eec4b1f1"}'
             }
        mock_head_object.side_effect = Exception("S3 head_object failed")
        event = {
            'Records': [
                {'body': '{"bucket": "source-bucket","fileId":"test-file"}'}
            ]
        }
        context = {}
        with self.assertRaises(Exception) as cm:
            lambda_function.lambda_handler(event, context)
        self.assertEqual(str(cm.exception), "S3 head_object failed")

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
    def test_complete_multipart_upload_failure(self, mock_validate_metadata, abort_multipart_upload, mock_complete_multipart_upload,
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
    def test_send_message_failure(self, mock_validate_metadata,  mock_send_message, _, mock_head_object):
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


    @patch('lambda_function.s3_client.get_object')
    def test_incoming_metadata_validation_success(self, mock_get_object):
        mock_get_object.return_value = {
            'Body':
                '{"FFID":[{"extension":null,"identificationBasis":"","puid":null,"extensionMismatch":false,"formatName":""}],'
                '"FileReference":"ZDSCFC","LegalStatus":"Public Record(s)","DescriptionClosed":"false",'
                '"antivirusSoftwareVersion":"4.4.0","Series":"MOCK1 123","LegalCustodyTransferConfirmed":"true",'
                '"UUID":"0000c951-b332-4d45-93e7-8c24eec4b1f1","ConsignmentReference":"TDR-2024-PQXN","Language":"English",'
                '"AppraisalSelectionSignOffConfirmed":"true","ClientSideFileSize":"1","HeldBy":"The National Archives, Kew",'
                '"SensitivityReviewSignOffConfirmed":"true","RightsCopyright":"Crown Copyright","ClosureType":"Open",'
                '"antivirusDatetime":"2024-09-19 07:07:00.996+00","TransferInitiatedDatetime":"2024-09-19 07:21:57",'
                '"SHA256ClientSideChecksum":"cbecda1c7d37d4c0aa5466243bb4a0018c31bf06d74fa7338290dd3068db4fed",'
                '"antivirusSoftware":"yara","ClientSideOriginalFilepath":"content/testfile1548","antivirusResult":"",'
                '"ClientSideFileLastModifiedDate":"2024-05-03 09:53:37.433","CrownCopyrightConfirmed":"true",'
                '"Filename":"testfile1548","ParentReference":"ZDSLTF",'
                '"SHA256ServerSideChecksum":"cbecda1c7d37d4c0aa5466243bb4a0018c31bf06d74fa7338290dd3068db4fed",'
                '"FileType":"File","TitleClosed":"false","PublicRecordsConfirmed":"true","TransferringBody":"Mock 1 Department"}'
             }
        result = lambda_function.validate_mandatory_fields_exist('any_bucket_patched', 'any_key_patched')
        self.assertEqual(True, result)


    @patch('lambda_function.s3_client.get_object')
    def test_should_raise_an_exception_when_series_does_not_exist(self, mock_get_object):
        mock_get_object.return_value = {
            'Body':
                '{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", '
                '"TransferInitiatedDatetime": "2024-09-19 07:21:57","UUID": "0000c951-b332-4d45-93e7-8c24eec4b1f1"}'
             }
        with self.assertRaises(Exception) as ex:
            lambda_function.validate_mandatory_fields_exist('any_bucket_patched', 'any_key_patched')
        self.assertEqual("'Series' is a required property" , str(ex.exception))

    @patch('lambda_function.s3_client.get_object')
    def test_should_raise_an_exception_when_consignment_reference_does_not_exist(self, mock_get_object):
        mock_get_object.return_value = {
            'Body':
                '{"no-consignment-ref": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series":"MOCK1 123", '
                '"TransferInitiatedDatetime": "2024-09-19 07:21:57","UUID": "0000c951-b332-4d45-93e7-8c24eec4b1f1"}'
             }
        with self.assertRaises(Exception) as ex:
            lambda_function.validate_mandatory_fields_exist('any_bucket_patched', 'any_key_patched')
        self.assertEqual("'ConsignmentReference' is a required property" , str(ex.exception))

    @patch('lambda_function.s3_client.get_object')
    def test_should_raise_an_exception_when_UUID_does_not_exist(self, mock_get_object):
        mock_get_object.return_value = {
            'Body':
                '{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series":"MOCK1 123", '
                '"TransferInitiatedDatetime": "2024-09-19 07:21:57"}'
             }
        with self.assertRaises(Exception) as ex:
            lambda_function.validate_mandatory_fields_exist('any_bucket_patched', 'any_key_patched')
        self.assertEqual("'UUID' is a required property" , str(ex.exception))

    @patch('lambda_function.s3_client.head_object')
    def test_should_raise_an_exception_when_the_object_does_not_exist_in_source_bucket(self, mock_head_object):
        error_response = {
            'Error': {
                'Code': '404',
                'Message': 'Not Found'
            }
        }

        client_error = ClientError(error_response, 'HeadObject')

        mock_head_object.side_effect = client_error
        with self.assertRaises(Exception) as ex:
            lambda_function.assert_objects_exist('some_bucket', 'some_key')

        self.assertEqual('Object some_key does not exist, underlying error is: '
                         'An error occurred (404) when calling the HeadObject operation: Not Found' , str(ex.exception))


    @patch('lambda_function.s3_client.head_object')
    def test_should_raise_an_exception_when_the_object_exists_but_metadata_does_not_exist_in_source_bucket(self, mock_head_object):
        def selective_error_object(bucket, key) :
            if key == 'some_key.metadata':
                error_response = {
                    'Error': {
                        'Code': '404',
                        'Message': 'Not Found'
                    }
                }
                raise ClientError(error_response, 'HeadObject')
            else:
                return {}

        mock_head_object.side_effect = selective_error_object
        with self.assertRaises(Exception) as ex:
            lambda_function.assert_objects_exist('some_bucket', 'some_key')

        self.assertEqual('Object some_key.metadata does not exist, underlying error is: '
                         'An error occurred (404) when calling the HeadObject operation: Not Found' , str(ex.exception))

    @patch('lambda_function.s3_client.get_object')
    def test_should_raise_an_exception_when_UUID_is_invalid(self, mock_get_object):
        mock_get_object.return_value = {
            'Body':
                '{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series":"MOCK1 123", '
                '"TransferInitiatedDatetime": "2024-09-19 07:21:57", "UUID": "invalid-uuid-format"}'
             }
        with self.assertRaises(Exception) as ex:
            lambda_function.validate_formats('any_bucket_patched', 'any_key_patched')
        self.assertEqual("Unable to parse UUID, 'invalid-uuid-format'. Invalid format" , str(ex.exception))


    @patch('lambda_function.s3_client.get_object')
    def test_should_raise_an_exception_when_transfer_initiated_date_is_invalid(self, mock_get_object):
        mock_get_object.return_value = {
            'Body':
                '{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series":"MOCK1 123", '
                '"TransferInitiatedDatetime": "2024-19-19 07:21:57", "UUID": "bb7bb923-b82c-4203-a3c1-ea3f362ef4da"}'
             }
        with self.assertRaises(Exception) as ex:
            lambda_function.validate_formats('any_bucket_patched', 'any_key_patched')
        self.assertEqual("Unable to parse date, '2024-19-19 07:21:57'. Invalid format" , str(ex.exception))


if __name__ == '__main__':
    unittest.main()
