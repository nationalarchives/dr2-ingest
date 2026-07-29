import io
import json
import uuid

import lambda_function


def copy_helper(
        self,
        mock_validate_formats,
        mock_validate_mandatory_fields_exist,
        mock_get_object,
        mock_delete_object,
        mock_send_message,
        mock_copy,
        potential_message_id=None,
        should_delete=False,
        skip_validation=False,
        potential_files_prefix=None):
    asset_id = str(uuid.uuid4())
    body_json = {"bucket": "source-bucket", "assetId": asset_id, "metadataLocation": f"s3://metadata-bucket/{asset_id}.metadata"}
    if potential_files_prefix:
        body_json['filesPrefix'] = potential_files_prefix

    files_prefix = potential_files_prefix if potential_files_prefix else asset_id
    expected_message_body = {
        "id": asset_id,
        "location": f"s3://destination-bucket/{asset_id}.metadata",
        "filesPrefix": files_prefix
    }
    if potential_message_id:
        body_json['messageId'] = potential_message_id
        expected_message_body['messageId'] = potential_message_id

    body = {'body': json.dumps(body_json)}
    file_count = 1001 if should_delete else 2
    file_ids = [str(uuid.uuid4()) for _ in range(file_count)]
    mock_validate_mandatory_fields_exist.return_value = True
    mock_validate_formats.return_value = True
    metadata_object = lambda file_id: {
        "fileId": file_id,
        "ConsignmentReference": "TDR-2024-PQXN",
        "FileReference": "ZDSCFC",
        "Series": "SMTH 123",
        "TransferInitiatedDatetime": "2024-09-19 07:21:57",
        "UUID": "0000c951-b332-4d45-93e7-8c24eec4b1f1"
    }
    metadata_objects = [metadata_object(file_id) for file_id in file_ids]
    mock_get_object.return_value = {
        'Body': io.BytesIO(json.dumps(metadata_objects).encode('utf-8')),
    }

    event = {'Records': [body]}
    context = {}
    lambda_function.lambda_handler(event, context)

    expected_sqs_args = {'MessageBody': json.dumps(expected_message_body), 'QueueUrl': 'destination-queue'}

    self.assertEqual(2, mock_copy.call_count)
    expected_transfer_files = [f'{files_prefix}/{file_id}' for file_id in file_ids]
    self.assertEqual(('destination-bucket', expected_transfer_files, 'source-bucket'), mock_copy.call_args_list[0].args)
    self.assertEqual(('destination-bucket', [f'{asset_id}.metadata'], 'metadata-bucket'), mock_copy.call_args_list[1].args)
    self.assertEqual(expected_sqs_args, mock_send_message.call_args_list[0][1])

    if skip_validation:
        mock_validate_mandatory_fields_exist.assert_not_called()
        mock_validate_formats.assert_not_called()
    else:
        self.assertEqual(file_count, mock_validate_mandatory_fields_exist.call_count)
        self.assertEqual(file_count, mock_validate_formats.call_count)
        first_validate_args = mock_validate_mandatory_fields_exist.call_args_list[0].args
        self.assertEqual('common/preingest-dri/metadata-schema.json', first_validate_args[0])
        self.assertEqual(metadata_objects[0], first_validate_args[1])
        first_validate_format_args = mock_validate_formats.call_args_list[0].args
        self.assertEqual(metadata_objects[0], first_validate_format_args[0])

    mock_get_object.assert_called_once()
    get_object_args = mock_get_object.call_args_list[0].kwargs
    self.assertEqual('metadata-bucket', get_object_args['Bucket'])
    self.assertEqual(f'{asset_id}.metadata', get_object_args['Key'])

    if should_delete:
        self.assertEqual(2, mock_delete_object.call_count)
        self.assertEqual('source-bucket', mock_delete_object.call_args_list[0].kwargs['Bucket'])
        self.assertEqual('source-bucket', mock_delete_object.call_args_list[1].kwargs['Bucket'])
        self.assertEqual(1000, len(mock_delete_object.call_args_list[0].kwargs['Delete']['Objects']))
        self.assertEqual(1, len(mock_delete_object.call_args_list[1].kwargs['Delete']['Objects']))
    else:
        self.assertEqual(0, mock_delete_object.call_count)
