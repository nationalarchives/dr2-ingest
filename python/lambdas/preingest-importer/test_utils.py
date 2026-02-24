import io
import json
import uuid

import lambda_function


def copy_helper(self, mock_validate_formats, mock_validate_mandatory_fields_exist, mock_get_object, mock_delete_object,
                mock_send_message, mock_copy,
                mock_head_object, mock_list_objects, potential_message_id=None, should_delete=False):
    content_length = 5 * 1024 * 1024 * 1024
    asset_id = str(uuid.uuid4())
    def key(): return f'{asset_id}/{str(uuid.uuid4())}'
    body_json = {"bucket": "source-bucket", "assetId": asset_id}

    expected_message_body = {"id": asset_id, "location": f"s3://destination-bucket/{asset_id}.metadata"}
    if potential_message_id:
        body_json['messageId'] = potential_message_id
        expected_message_body['messageId'] = potential_message_id

    body = {'body': json.dumps(body_json)}
    content_files = [{'Size': content_length, 'Key': key()} for _ in range(0, 1000)]
    content_files.append({'Size': 1, 'Key': f'{asset_id}.metadata'})
    empty_response = {'Contents': [], 'IsTruncated': False}
    mock_list_objects.side_effect = [{'Contents': content_files, 'IsTruncated': True}, empty_response]
    mock_head_object.return_value = {'ContentLength': content_length}
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

    self.assertEqual(1, mock_copy.call_count)
    self.assertEqual(2, mock_list_objects.call_count)

    self.assertEqual('destination-bucket', mock_copy.call_args_list[0].args[0])
    self.assertEqual(sorted([f['Key'] for f in content_files]), sorted(mock_copy.call_args_list[0].args[1]))


    self.assertEqual(1, mock_copy.call_count)
    self.assertEqual(expected_sqs_args, mock_send_message.call_args_list[0][1])

    mock_validate_mandatory_fields_exist.assert_called_once()
    mock_validate_formats.assert_called_once()

    if should_delete:
        self.assertEqual(2, mock_delete_object.call_count)
        self.assertEqual('source-bucket', mock_delete_object.call_args_list[0].kwargs['Bucket'])
        self.assertEqual('source-bucket', mock_delete_object.call_args_list[1].kwargs['Bucket'])
        self.assertEqual(1000, len(mock_delete_object.call_args_list[0].kwargs['Delete']['Objects']))
        self.assertEqual(1, len(mock_delete_object.call_args_list[1].kwargs['Delete']['Objects']))
    else:
        self.assertEqual(0, mock_delete_object.call_count)
