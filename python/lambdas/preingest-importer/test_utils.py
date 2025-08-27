import io
import json
import uuid

import lambda_function


def copy_helper(self, mock_validate_formats, mock_validate_mandatory_fields_exist, mock_get_object,
                mock_send_message, mock_complete_multipart_upload, _,
                mock_create_multipart_upload,
                mock_head_object, mock_list_objects, potential_message_id=None):
    content_length = 5 * 1024 * 1024 * 1024
    asset_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    key = f'{asset_id}/{file_id}'
    body_json = {"bucket": "source-bucket", "assetId": asset_id}

    expected_message_body = {"id": asset_id, "location": f"s3://destination-bucket/{asset_id}.metadata"}
    if potential_message_id:
        body_json['messageId'] = potential_message_id
        expected_message_body['messageId'] = potential_message_id

    body = {'body': json.dumps(body_json)}
    contents = [{'Size': content_length, 'Key': key}, {'Size': 1, 'Key': f'{asset_id}.metadata'}]
    empty_response = {'Contents': [], 'IsTruncated': False}
    mock_list_objects.side_effect = [{'Contents': contents, 'IsTruncated': True}, empty_response]
    mock_head_object.return_value = {'ContentLength': content_length}
    mock_create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
    mock_validate_mandatory_fields_exist.return_value = True
    mock_validate_formats.return_value = True
    mock_get_object.return_value = {
        'Body': io.BytesIO(
            b'[{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series": "SMTH 123", '
            b'"TransferInitiatedDatetime": "2024-09-19 07:21:57","UUID": "0000c951-b332-4d45-93e7-8c24eec4b1f1"}]'),
    }

    event = {'Records': [body]}
    context = {}
    lambda_function.lambda_handler(event, context)

    expected_sqs_args = {'MessageBody': json.dumps(expected_message_body), 'QueueUrl': 'destination-queue'}

    self.assertEqual(mock_create_multipart_upload.call_count, 2)
    self.assertEqual(mock_list_objects.call_count, 2)

    def assert_create_multipart_arguments(idx, key):
        self.assertEqual(mock_create_multipart_upload.call_args_list[idx][1], {'Bucket': 'destination-bucket',
                                                                               'Key': key})

    assert_create_multipart_arguments(0, key)
    assert_create_multipart_arguments(1, f"{asset_id}.metadata")
    self.assertEqual(mock_complete_multipart_upload.call_count, 2)
    self.assertEqual(mock_send_message.call_args_list[0][1], expected_sqs_args)

    mock_validate_mandatory_fields_exist.assert_called_once()
    mock_validate_formats.assert_called_once()
