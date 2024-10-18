import io
import lambda_function


def copy_helper(self, mock_validate_formats, mock_validate_mandatory_fields_exist, mock_get_object,
                     mock_send_message, mock_complete_multipart_upload, _,
                     mock_create_multipart_upload,
                     mock_head_object, body, expected_message_body):
    content_length = 5 * 1024 * 1024 * 1024
    mock_head_object.return_value = {'ContentLength': content_length}
    mock_create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
    mock_validate_mandatory_fields_exist.return_value = True
    mock_validate_formats.return_value = True
    mock_get_object.return_value = {
        'Body': io.BytesIO(
            b'{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series": "SMTH 123", '
            b'"TransferInitiatedDatetime": "2024-09-19 07:21:57","UUID": "0000c951-b332-4d45-93e7-8c24eec4b1f1"}'),
    }

    event = {'Records': [body]}
    context = {}
    lambda_function.lambda_handler(event, context)

    expected_sqs_args = {'MessageBody': expected_message_body, 'QueueUrl': 'destination-queue'}

    self.assertEqual(mock_create_multipart_upload.call_count, 2)

    def assert_create_multipart_arguments(idx, key):
        self.assertEqual(mock_create_multipart_upload.call_args_list[idx][1], {'Bucket': 'destination-bucket',
                                                                               'Key': key})

    assert_create_multipart_arguments(0, "test-file")
    assert_create_multipart_arguments(1, "test-file.metadata")
    self.assertEqual(mock_complete_multipart_upload.call_count, 2)
    self.assertEqual(mock_send_message.call_args_list[0][1], expected_sqs_args)

    mock_validate_mandatory_fields_exist.assert_called_once()
    mock_validate_formats.assert_called_once()
