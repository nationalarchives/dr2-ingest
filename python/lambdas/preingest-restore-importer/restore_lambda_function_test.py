import io
import json
import unittest
from unittest.mock import patch

import lambda_function


@patch.dict('os.environ', {'OUTPUT_BUCKET_NAME': 'destination-bucket'})
@patch.dict('os.environ', {'OUTPUT_QUEUE_URL': 'destination-queue'})
class TestLambdaFunction(unittest.TestCase):

    @staticmethod
    def event(asset_id='asset-id', metadata_location='s3://source-bucket/path/asset.metadata'):
        return {
            'Records': [{
                'body': json.dumps({
                    'assetId': asset_id,
                    'metadataLocation': metadata_location
                })
            }]
        }

    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.s3_client.delete_objects')
    @patch('lambda_function.copy_objects')
    @patch('lambda_function.s3_client.get_object')
    def test_lambda_handler_copies_deletes_and_sends_message(
            self, mock_get_object, mock_copy_objects, mock_delete_objects, mock_send_message):
        mock_get_object.return_value = {
            'Body': io.BytesIO(
                b'<Metadata><Representation><ContentObjects>'
                b'<ContentObject>file-one</ContentObject>'
                b'<ContentObject>file-two</ContentObject>'
                b'</ContentObjects></Representation></Metadata>'
            )
        }

        lambda_function.lambda_handler(self.event(), {})

        mock_get_object.assert_called_once_with(Bucket='source-bucket', Key='path/asset.metadata')
        mock_copy_objects.assert_called_once_with(
            'destination-bucket',
            ['asset-id/file-one', 'asset-id/file-two', 'path/asset.metadata'],
            'source-bucket'
        )
        mock_delete_objects.assert_called_once_with(
            Bucket='source-bucket',
            Delete={'Objects': [
                {'Key': 'asset-id/file-one'},
                {'Key': 'asset-id/file-two'},
                {'Key': 'path/asset.metadata'}
            ]}
        )
        mock_send_message.assert_called_once_with(
            QueueUrl='destination-queue',
            MessageBody=json.dumps({
                'id': 'asset-id',
                'location': 's3://destination-bucket/path/asset.metadata',
                'filesPrefix': 'asset-id'
            })
        )

    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.s3_client.delete_objects')
    @patch('lambda_function.copy_objects')
    @patch('lambda_function.s3_client.get_object')
    def test_lambda_handler_deletes_objects_in_batches_of_1000(
            self, mock_get_object, _mock_copy_objects, mock_delete_objects, _mock_send_message):
        content_objects = ''.join(
            f'<ContentObject>file-{index}</ContentObject>' for index in range(1000)
        )
        mock_get_object.return_value = {
            'Body': io.BytesIO(
                f'<Metadata><Representation><ContentObjects>{content_objects}'
                '</ContentObjects></Representation></Metadata>'.encode('utf-8')
            )
        }

        lambda_function.lambda_handler(self.event(), {})

        self.assertEqual(2, mock_delete_objects.call_count)
        first_batch = mock_delete_objects.call_args_list[0].kwargs['Delete']['Objects']
        second_batch = mock_delete_objects.call_args_list[1].kwargs['Delete']['Objects']
        self.assertEqual(1000, len(first_batch))
        self.assertEqual(1, len(second_batch))
        self.assertEqual({'Key': 'path/asset.metadata'}, second_batch[0])

    @patch('lambda_function.sqs_client.send_message')
    @patch('lambda_function.s3_client.delete_objects')
    @patch('lambda_function.copy_objects')
    @patch('lambda_function.s3_client.get_object')
    def test_copy_failure_does_not_delete_or_send_message(
            self, mock_get_object, mock_copy_objects, mock_delete_objects, mock_send_message):
        mock_get_object.return_value = {
            'Body': io.BytesIO(
                b'<Metadata><Representation><ContentObjects>'
                b'<ContentObject>file-one</ContentObject>'
                b'</ContentObjects></Representation></Metadata>'
            )
        }
        mock_copy_objects.side_effect = Exception('S3 copy failed')

        with self.assertRaises(Exception) as context:
            lambda_function.lambda_handler(self.event(), {})

        self.assertEqual('S3 copy failed', str(context.exception))
        mock_delete_objects.assert_not_called()
        mock_send_message.assert_not_called()

    @patch('lambda_function.s3_client.copy')
    def test_copy_objects_passes_expected_arguments_to_s3(self, mock_s3_copy):
        lambda_function.copy_objects('destination-bucket', ['file-key'], 'source-bucket')

        mock_s3_copy.assert_called_once_with(
            {'Bucket': 'source-bucket', 'Key': 'file-key'},
            'destination-bucket',
            'file-key',
            {'TaggingDirective': 'REPLACE'}
        )


if __name__ == '__main__':
    unittest.main()
