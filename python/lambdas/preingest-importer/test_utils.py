import io
import json
import uuid

import lambda_function


def copy_helper(self, mock_validate_formats, mock_validate_mandatory_fields_exist, mock_get_object, mock_delete_object,
                mock_send_message, mock_copy,
                mock_head_object, mock_list_objects, potential_message_id=None, should_delete=False, skip_validation=False):
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
    metadata_object = {
        "ConsignmentReference": "TDR-2024-PQXN",
        "FileReference": "ZDSCFC",
        "Series": "SMTH 123",
        "TransferInitiatedDatetime": "2024-09-19 07:21:57",
        "UUID": "0000c951-b332-4d45-93e7-8c24eec4b1f1"
    }
    mock_get_object.return_value = {
        'Body': io.BytesIO(
            b'[' + json.dumps(metadata_object).encode() + b']'),
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

    if skip_validation:
        mock_validate_mandatory_fields_exist.assert_not_called()
        mock_validate_formats.assert_not_called()
        mock_get_object.assert_not_called()
    else:
        mock_validate_mandatory_fields_exist.assert_called_once()
        validate_args = mock_validate_mandatory_fields_exist.call_args_list[0].args
        self.assertEqual('common/preingest-dri/metadata-schema.json', validate_args[0])
        self.assertEqual(metadata_object, validate_args[1])
        mock_validate_formats.assert_called_once()
        validate_format_args = mock_validate_formats.call_args_list[0].args
        self.assertEqual(metadata_object, validate_format_args[0])
        self.assertEqual('source-bucket', validate_format_args[1])
        self.assertEqual(f'{asset_id}.metadata', validate_format_args[2])
        mock_get_object.assert_called_once()
        get_object_args = mock_get_object.call_args_list[0].kwargs
        self.assertEqual('source-bucket', get_object_args['Bucket'])
        self.assertEqual(f'{asset_id}.metadata', get_object_args['Key'])

    if should_delete:
        self.assertEqual(2, mock_delete_object.call_count)
        self.assertEqual('source-bucket', mock_delete_object.call_args_list[0].kwargs['Bucket'])
        self.assertEqual('source-bucket', mock_delete_object.call_args_list[1].kwargs['Bucket'])
        self.assertEqual(1000, len(mock_delete_object.call_args_list[0].kwargs['Delete']['Objects']))
        self.assertEqual(1, len(mock_delete_object.call_args_list[1].kwargs['Delete']['Objects']))
    else:
        self.assertEqual(0, mock_delete_object.call_count)
