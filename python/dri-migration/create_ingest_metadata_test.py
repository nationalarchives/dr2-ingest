import unittest
from unittest.mock import patch, MagicMock, mock_open
import json
from migrate import create_ingest_metadata
import os

class TestMigrate(unittest.TestCase):
    @patch('oracledb.connect')
    @patch('oracledb.init_oracle_client')
    @patch('builtins.open', new_callable=mock_open, read_data="SELECT * FROM TEST")
    @patch('migrate.create_ingest_metadata.create_skeleton_suite_lookup')
    @patch('migrate.create_ingest_metadata.calculate_checksum')
    @patch('migrate.create_ingest_metadata.s3_client')
    @patch('migrate.create_ingest_metadata.sqs_client')
    def test_migrate_s3_sqs(
            self, mock_sqs, mock_s3, mock_checksum,
            mock_create_skeleton, _, __, mock_connect,
    ):
        os.environ['CLIENT_LOCATION'] = '/test/client'
        os.environ['STORE_PASSWORD'] = 'password'
        os.environ['DROID_PATH'] = '/test/droid'
        os.environ['ACCOUNT_NUMBER'] = '123456789'
        os.environ['ENVIRONMENT'] = 'testenv'

        mock_create_skeleton.return_value = {
            "fmt/123": {"file_path": "/test/file1"}
        }

        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("PUID",), ("UUID",), ("FILEID",), ("FILE_PATH",), ("FIXITIES",),
            ("SERIES",), ("DESCRIPTION",), ("TRANSFERINITIATEDDATETIME",),
            ("CONSIGNMENTREFERENCE",), ("DRIBATCHREFERENCE",), ("FILENAME",),
            ("FILEREFERENCE",), ("METADATA",)
        ]
        row = [
            "fmt/123", "uuid-abc", "fileid-xyz", "/test/file1",
            json.dumps([{"SHA256": "test"}]),
            "series1", "desc", "2021-01-01", "consignment", "batch-ref",
            "filename.txt", "fileref", "meta"
        ]
        mock_cursor.fetchmany.side_effect = [[row], []]
        mock_connect.return_value.cursor.return_value = mock_cursor

        mock_checksum.return_value = "abc123"

        create_ingest_metadata.migrate()

        mock_s3.upload_file.assert_called_once_with("/test/file1", "testenv-dr2-ingest-raw-cache", "uuid-abc")
        args, kwargs = mock_s3.upload_fileobj.call_args
        metadata_bytes = args[0].getvalue()
        metadata = json.loads(metadata_bytes.decode("utf-8"))

        self.assertEqual(metadata["UUID"], "uuid-abc")
        self.assertEqual(metadata["Series"], "series1")
        self.assertEqual(metadata["checksum_sha256"], "abc123")
        self.assertEqual(args[1], "testenv-dr2-ingest-raw-cache")
        self.assertEqual(args[2], "uuid-abc.metadata")

        mock_sqs.send_message.assert_called_once()
        sqs_args, sqs_kwargs = mock_sqs.send_message.call_args
        self.assertEqual(sqs_kwargs["QueueUrl"],
                         "https://sqs.eu-west-2.amazonaws.com/123456789/testenv-dr2-copy-files-from-dri")
        sent_body = json.loads(sqs_kwargs["MessageBody"])
        self.assertEqual(sent_body["fileId"], "uuid-abc")
        self.assertEqual(sent_body["bucket"], "testenv-dr2-ingest-raw-cache")


if __name__ == '__main__':
    unittest.main()