import io
import json
import os
import unittest
import uuid
from unittest.mock import patch

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

    @patch('lambda_function.copy_objects')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.s3_client.get_object')
    @patch('lambda_function.s3_client.delete_objects')
    @patch('lambda_function.validate_mandatory_fields_exist')
    @patch('lambda_function.validate_formats')
    def test_copy(
            self,
            mock_validate_formats,
            mock_validate_mandatory_fields_exist,
            mock_delete_object,
            mock_get_object,
            mock_send_message,
            mock_copy):
        copy_helper(
            self,
            mock_validate_formats,
            mock_validate_mandatory_fields_exist,
            mock_get_object,
            mock_delete_object,
            mock_send_message,
            mock_copy)

    @patch('lambda_function.copy_objects')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.s3_client.get_object')
    @patch('lambda_function.s3_client.delete_objects')
    @patch('lambda_function.validate_mandatory_fields_exist')
    @patch('lambda_function.validate_formats')
    @patch.dict(os.environ, {'DELETE_FROM_SOURCE': 'true'})
    def test_copy_with_source_delete(
            self,
            mock_validate_formats,
            mock_validate_mandatory_fields_exist,
            mock_delete_object,
            mock_get_object,
            mock_send_message,
            mock_copy):
        copy_helper(
            self,
            mock_validate_formats,
            mock_validate_mandatory_fields_exist,
            mock_get_object,
            mock_delete_object,
            mock_send_message,
            mock_copy,
            should_delete=True)

    @patch('lambda_function.copy_objects')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.s3_client.get_object')
    @patch('lambda_function.s3_client.delete_objects')
    @patch('lambda_function.validate_mandatory_fields_exist')
    @patch('lambda_function.validate_formats')
    @patch.dict(os.environ, {'SKIP_VALIDATION': 'true'})
    def test_copy_with_skip_validation(
            self,
            mock_validate_formats,
            mock_validate_mandatory_fields_exist,
            mock_delete_object,
            mock_get_object,
            mock_send_message,
            mock_copy):
        copy_helper(
            self,
            mock_validate_formats,
            mock_validate_mandatory_fields_exist,
            mock_get_object,
            mock_delete_object,
            mock_send_message,
            mock_copy,
            skip_validation=True)

    @patch('lambda_function.copy_objects')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.s3_client.get_object')
    @patch('lambda_function.copy_records_metadata')
    @patch('lambda_function.validate_mandatory_fields_exist')
    @patch('lambda_function.validate_formats')
    @patch.dict(os.environ, {'RECORDS_METADATA_BUCKET': 'records-metadata-bucket'})
    def test_copy_calls_copy_records_metadata_when_records_metadata_bucket_set(
            self,
            mock_validate_formats,
            mock_validate_mandatory_fields_exist,
            mock_copy_records_metadata,
            mock_get_object,
            _mock_send_message,
            _mock_copy):
        asset_id = str(uuid.uuid4())
        metadata_object = {
            "fileId": str(uuid.uuid4()),
            "ConsignmentReference": "TDR-2024-PQXN",
            "FileReference": "ZDSCFC",
            "Series": "SMTH 123",
            "TransferInitiatedDatetime": "2024-09-19 07:21:57",
            "UUID": "0000c951-b332-4d45-93e7-8c24eec4b1f1"
        }
        mock_validate_mandatory_fields_exist.return_value = True
        mock_validate_formats.return_value = True
        mock_get_object.return_value = {
            'Body': io.BytesIO(b'[' + json.dumps(metadata_object).encode() + b']')
        }
        event = {'Records': [{'body': json.dumps({
            "bucket": "source-bucket",
            "assetId": asset_id,
            "metadataLocation": f"s3://metadata-bucket/{asset_id}.metadata"
        })}]}

        lambda_function.lambda_handler(event, {})

        mock_copy_records_metadata.assert_called_once_with(
            'metadata-bucket', 'records-metadata-bucket', [metadata_object], f'{asset_id}.metadata')

    def test_copy_records_metadata_uploads_merged_metadata(self):
        with patch('lambda_function.s3_client.get_object') as mock_get_object, \
                patch('lambda_function.s3_client.upload_fileobj') as mock_upload_fileobj:
            migrated_metadata = {"foo": "bar"}
            mock_get_object.return_value = {'Body': io.BytesIO(json.dumps(migrated_metadata).encode('utf-8'))}
            json_metadata = [{"Series": "MOCK1 123", "FileReference": "AB/12"}]

            lambda_function.copy_records_metadata('source-bucket', 'records-metadata-bucket', json_metadata,
                                                  'asset-id.metadata')

            mock_get_object.assert_called_once_with(Bucket='records-metadata-bucket', Key='live/MOCK1 123-AB-12.json')
            self.assertEqual(migrated_metadata, json_metadata[0]['migratedMetadata'])

            mock_upload_fileobj.assert_called_once()
            call_args = mock_upload_fileobj.call_args.args
            self.assertEqual('source-bucket', call_args[1])
            self.assertEqual('asset-id.metadata', call_args[2])
            uploaded_content = json.loads(call_args[0].getvalue().decode('utf-8'))
            self.assertEqual(json_metadata, uploaded_content)

    @patch('lambda_function.copy_objects')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.s3_client.get_object')
    @patch('lambda_function.s3_client.delete_objects')
    @patch('lambda_function.validate_mandatory_fields_exist')
    @patch('lambda_function.validate_formats')
    def test_copy_returns_messageId_when_in_body(
            self,
            mock_validate_formats,
            mock_validate_mandatory_fields_exist,
            mock_delete_objects,
            mock_get_object,
            mock_send_message,
            mock_copy_objects):
        copy_helper(
            self,
            mock_validate_formats,
            mock_validate_mandatory_fields_exist,
            mock_get_object,
            mock_delete_objects,
            mock_send_message,
            mock_copy_objects,
            potential_message_id="message-id")

    @patch('lambda_function.copy_objects')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.s3_client.get_object')
    @patch('lambda_function.s3_client.delete_objects')
    @patch('lambda_function.validate_mandatory_fields_exist')
    @patch('lambda_function.validate_formats')
    def test_copy_returns_files_prefix_if_provided(
            self,
            mock_validate_formats,
            mock_validate_mandatory_fields_exist,
            mock_delete_objects,
            mock_get_object,
            mock_send_message,
            mock_copy_objects):
        copy_helper(
            self,
            mock_validate_formats,
            mock_validate_mandatory_fields_exist,
            mock_get_object,
            mock_delete_objects,
            mock_send_message,
            mock_copy_objects,
            potential_files_prefix="v1/test/prefix")

    @patch('lambda_function.copy_objects')
    @patch('lambda_function.s3_client.get_object')
    @patch('lambda_function.validate_mandatory_fields_exist')
    @patch('lambda_function.validate_formats')
    def test_copy_failure_raises_exception(
            self,
            mock_validate_formats,
            mock_validate_mandatory_fields_exist,
            mock_get_object,
            mock_copy_objects):
        asset_id = str(uuid.uuid4())
        metadata = [dict(self.valid_metadata(), fileId=str(uuid.uuid4()))]
        mock_get_object.return_value = {'Body': io.BytesIO(json.dumps(metadata).encode('utf-8'))}
        mock_validate_mandatory_fields_exist.return_value = True
        mock_validate_formats.return_value = True
        mock_copy_objects.side_effect = Exception("S3 copy failed")
        event = {'Records': [{'body': json.dumps({
            "bucket": "source-bucket",
            "assetId": asset_id,
            "metadataLocation": f"s3://metadata-bucket/{asset_id}.metadata"
        })}]}

        with self.assertRaises(Exception) as cm:
            lambda_function.lambda_handler(event, {})
        self.assertEqual("S3 copy failed", str(cm.exception))

    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.copy_objects')
    @patch('lambda_function.s3_client.get_object')
    @patch('lambda_function.validate_mandatory_fields_exist')
    @patch('lambda_function.validate_formats')
    def test_send_message_failure(
            self,
            mock_validate_formats,
            mock_validate_mandatory_fields_exist,
            mock_get_object,
            _mock_copy_objects,
            mock_send_message):
        asset_id = str(uuid.uuid4())
        metadata = [dict(self.valid_metadata(), fileId=str(uuid.uuid4()))]
        mock_get_object.return_value = {'Body': io.BytesIO(json.dumps(metadata).encode('utf-8'))}
        mock_validate_mandatory_fields_exist.return_value = True
        mock_validate_formats.return_value = True
        mock_send_message.side_effect = Exception("SQS Send message failed")
        event = {'Records': [{'body': json.dumps({
            "bucket": "source-bucket",
            "assetId": asset_id,
            "metadataLocation": f"s3://metadata-bucket/{asset_id}.metadata"
        })}]}

        with self.assertRaises(Exception) as cm:
            lambda_function.lambda_handler(event, {})
        self.assertEqual("SQS Send message failed", str(cm.exception))

    @patch('lambda_function.s3_client.delete_objects')
    @patch('lambda_function.s3_client.get_object')
    @patch('lambda_function.copy_objects')
    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.validate_mandatory_fields_exist')
    @patch('lambda_function.validate_formats')
    @patch.dict(os.environ, {'DELETE_FROM_SOURCE': 'true'})
    def test_delete_failure_should_not_send_message(
            self,
            mock_validate_formats,
            mock_validate_mandatory_fields_exist,
            mock_send_message,
            _mock_copy_objects,
            mock_get_object,
            mock_delete_objects):
        asset_id = str(uuid.uuid4())
        metadata = [dict(self.valid_metadata(), fileId=str(uuid.uuid4())) for _ in range(2)]
        mock_get_object.return_value = {'Body': io.BytesIO(json.dumps(metadata).encode('utf-8'))}
        mock_validate_mandatory_fields_exist.return_value = True
        mock_validate_formats.return_value = True
        mock_delete_objects.side_effect = Exception("Delete object failed")
        event = {'Records': [{'body': json.dumps({
            "bucket": "source-bucket",
            "assetId": asset_id,
            "metadataLocation": f"s3://metadata-bucket/{asset_id}.metadata"
        })}]}

        with self.assertRaises(Exception) as cm:
            lambda_function.lambda_handler(event, {})
        self.assertEqual("Delete object failed", str(cm.exception))
        self.assertEqual(0, mock_send_message.call_count)

    def test_should_successfully_validate_when_the_fields_are_valid(self):
        schema_location = "common/preingest-tdr/metadata-schema.json"
        result = lambda_function.validate_mandatory_fields_exist(schema_location, self.valid_metadata())
        self.assertEqual(True, result)

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
                raise Exception("Unexpected property value")

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
        mock_response_body = """{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series":"MOCK1 123", "TransferInitiatedDatetime": "2024-09-19 07:21:57", "UUID": "invalid-uuid-format"}"""

        with self.assertRaises(Exception) as ex:
            lambda_function.validate_formats(json.loads(mock_response_body))
        self.assertEqual("Unable to parse UUID, 'invalid-uuid-format' from file. Invalid format", str(ex.exception))

    def test_should_raise_an_exception_when_series_is_empty(self):
        mock_response_body = """{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series": " ", "TransferInitiatedDatetime": "2024-09-19 07:21:57", "UUID": "bb7bb923-b82c-4203-a3c1-ea3f362ef4da"}"""
        with self.assertRaises(Exception) as ex:
            lambda_function.validate_formats(json.loads(mock_response_body))
        self.assertEqual("Empty Series value in file. Unable to proceed", str(ex.exception))

    def test_should_raise_an_exception_when_series_format_is_invalid(self):
        invalid_series_names = ["MOCK1 12345", "1LEV 12", "abc 12", "MOCKT 1", "UNKNOWN", "LEV 2   ", "FLM 012",
                                "SDF 12345", " WER 12"]
        for invalid_series in invalid_series_names:
            mock_response_body = f"""{{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series": "{invalid_series}", "TransferInitiatedDatetime": "2024-09-19 07:21:57", "UUID": "bb7bb923-b82c-4203-a3c1-ea3f362ef4da"}}"""
            with self.assertRaises(Exception) as ex:
                lambda_function.validate_formats(json.loads(mock_response_body))
            self.assertEqual(
                f"Invalid Series value, '{invalid_series}' in file. Unable to proceed",
                str(ex.exception))

    def test_should_successfully_validate_various_allowed_formats_of_series_name(self):
        valid_series_names = ["MOCK1 123", "Unknown", "UKSC 1", "LEV 2", "PRO 234", "A 7"]
        for valid_series in valid_series_names:
            mock_response_body = f"""{{"ConsignmentReference": "TDR-2024-PQXN", "FileReference": "ZDSCFC", "Series": "{valid_series}", "TransferInitiatedDatetime": "2024-09-19 07:21:57", "UUID": "bb7bb923-b82c-4203-a3c1-ea3f362ef4da"}}"""
            self.assertEqual(True, lambda_function.validate_formats(json.loads(mock_response_body)))


if __name__ == '__main__':
    unittest.main()
