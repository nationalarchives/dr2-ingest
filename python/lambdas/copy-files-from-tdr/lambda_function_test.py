import math
import os
import unittest
from unittest.mock import patch
import lambda_function


@patch.dict('os.environ', {'DESTINATION_BUCKET': 'destination-bucket'})
@patch.dict('os.environ', {'DESTINATION_QUEUE': 'destination-queue'})
@patch.dict('os.environ', {'AWS_REGION': 'eu-west-2'})
class TestLambdaFunction(unittest.TestCase):
    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.copy_object')
    @patch('lambda_function.sqs_client.send_message')
    def test_standard_copy(self, mock_send_message, mock_copy_object, mock_head_object):
        mock_head_object.return_value = {'ContentLength': 1024}  # 1 KB
        event = {
            'Records': [
                {'body': '{"bucket": "source-bucket","fileId":"test-file"}'}
            ]
        }
        context = {}
        lambda_function.lambda_handler(event, context)

        def assert_copy_object_arguments(idx, key):
            expected_response = \
                {'Bucket': 'destination-bucket', 'CopySource': {'Bucket': 'source-bucket', 'Key': key}, 'Key': key}
            self.assertEqual(mock_copy_object.call_args_list[idx][1], expected_response)

        expected_sqs_args = \
            {'MessageBody': '{"location": "s3://destination-bucket/test-file"}', 'QueueUrl': 'destination-queue'}

        self.assertEqual(mock_copy_object.call_count, 2)
        assert_copy_object_arguments(0, 'test-file')
        assert_copy_object_arguments(1, 'test-file.metadata')
        self.assertEqual(mock_send_message.call_args_list[0][1], expected_sqs_args)

    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.create_multipart_upload')
    @patch('lambda_function.s3_client.upload_part_copy')
    @patch('lambda_function.s3_client.complete_multipart_upload')
    @patch('lambda_function.sqs_client.send_message')
    @patch.dict(os.environ, {'DESTINATION_BUCKET': 'destination-bucket'})
    def test_multipart_copy(self, mock_send_message, mock_complete_multipart_upload, mock_upload_part_copy,
                            mock_create_multipart_upload,
                            mock_head_object):
        content_length = 6 * 1024 * 1024 * 1024
        part_size = 5 * 1024 * 1024
        mock_head_object.return_value = {'ContentLength': 6 * 1024 * 1024 * 1024}
        mock_create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}

        event = {
            'Records': [
                {'body': '{"bucket": "source-bucket","fileId":"test-file"}'}
            ]
        }
        context = {}
        lambda_function.lambda_handler(event, context)

        expected_sqs_args = \
            {'MessageBody': '{"location": "s3://destination-bucket/test-file"}', 'QueueUrl': 'destination-queue'}

        self.assertEqual(mock_create_multipart_upload.call_count, 2)

        def assert_create_multipart_arguments(idx, key):
            self.assertEqual(mock_create_multipart_upload.call_args_list[idx][1], {'Bucket': 'destination-bucket',
                                                                                   'Key': key})

        assert_create_multipart_arguments(0, "test-file")
        assert_create_multipart_arguments(1, "test-file.metadata")
        self.assertEqual(mock_upload_part_copy.call_count, math.ceil(content_length / part_size * 2))
        self.assertEqual(mock_complete_multipart_upload.call_count, 2)
        self.assertEqual(mock_send_message.call_args_list[0][1], expected_sqs_args)

    @patch('lambda_function.s3_client.head_object')
    def test_head_object_failure(self, mock_head_object):
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
    def test_copy_object_failure(self, mock_copy_object, mock_head_object):
        mock_head_object.return_value = {'ContentLength': 1024}  # 1 KB

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
    def test_multipart_upload_failure(self, _, __,
                                      mock_upload_part_copy, mock_create_multipart_upload, mock_head_object):
        mock_head_object.return_value = {'ContentLength': 6 * 1024 * 1024 * 1024}  # 6 GB
        mock_create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}

        mock_upload_part_copy.side_effect = Exception("S3 upload_part_copy failed")

        event = {
            'Records': [
                {'body': '{"bucket": "source-bucket","fileId":"test-file"}'}
            ]
        }
        context = {}
        with self.assertRaises(Exception) as cm:
            lambda_function.lambda_handler(event, context)
        self.assertEqual(str(cm.exception), "S3 upload_part_copy failed")

    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.create_multipart_upload')
    @patch('lambda_function.s3_client.upload_part_copy')
    @patch('lambda_function.s3_client.complete_multipart_upload')
    @patch('lambda_function.s3_client.abort_multipart_upload')
    def test_complete_multipart_upload_failure(self, _, mock_complete_multipart_upload,
                                               mock_upload_part_copy, mock_create_multipart_upload, mock_head_object):
        mock_head_object.return_value = {'ContentLength': 6 * 1024 * 1024 * 1024}  # 6 GB
        mock_create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
        mock_upload_part_copy.return_value = {'CopyPartResult': {'ETag': 'etag'}}

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

    @patch('lambda_function.s3_client.head_object')
    @patch('lambda_function.s3_client.copy_object')
    @patch('lambda_function.sqs_client.send_message')
    def test_send_message_failure(self, mock_send_message, _, mock_head_object):
        mock_head_object.return_value = {'ContentLength': 1024}  # 1 KB
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


if __name__ == '__main__':
    unittest.main()
