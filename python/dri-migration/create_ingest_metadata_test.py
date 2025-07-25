import hashlib
import unittest
from unittest.mock import patch, MagicMock, mock_open, call
import json
from migrate import create_ingest_metadata
import os
import tempfile


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
            "fmt/123": {"file_path": "/test/file1"},
            "x-fmt/123": {"file_path": "/test/file2"}
        }

        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("PUID",), ("UUID",), ("FILEID",), ("FILE_PATH",), ("FIXITIES",),
            ("SERIES",), ("DESCRIPTION",), ("TRANSFERINITIATEDDATETIME",),
            ("CONSIGNMENTREFERENCE",), ("DRIBATCHREFERENCE",), ("FILENAME",),
            ("FILEREFERENCE",), ("METADATA",)
        ]

        row_fmt = [
            "fmt/123", "uuid-abc", "fileid-xyz", "/test/file1",
            json.dumps([{"SHA256": "test"}]),
            "series1", "desc", "2021-01-01", "consignment", "batch-ref",
            "filename.txt", "fileref", "meta"
        ]
        row_x_fmt = [
            "x-fmt/123", "uuid-def", "fileid-xyz", "/test/file2",
            json.dumps([{"SHA256": "test"}]),
            "series1", "desc", "2021-01-01", "consignment", "batch-ref",
            "filename.txt", "fileref", "meta"
        ]
        rows = [row_fmt, row_x_fmt]
        mock_cursor.fetchmany.side_effect = [rows, []]
        mock_connect.return_value.cursor.return_value = mock_cursor

        mock_checksum.return_value = "abc123"

        create_ingest_metadata.migrate()

        calls = [
            call("/test/file1", "testenv-dr2-ingest-raw-cache", "uuid-abc"),
            call("/test/file2", "testenv-dr2-ingest-raw-cache", "uuid-def")
        ]

        mock_s3.upload_file.assert_has_calls(calls)
        s3_args = mock_s3.upload_fileobj.call_args_list
        sqs_args = mock_sqs.send_message.call_args_list

        for idx, s3_arg in enumerate(s3_args):
            bytes_request, bucket, object_key = s3_arg[0]
            metadata_bytes = bytes_request.getvalue()
            metadata = json.loads(metadata_bytes.decode("utf-8"))
            metadata_uuid = rows[idx][1]

            self.assertEqual(metadata["UUID"], metadata_uuid)
            self.assertEqual(metadata["Series"], "series1")
            self.assertEqual(metadata["checksum_sha256"], "abc123")
            self.assertEqual(bucket, "testenv-dr2-ingest-raw-cache")
            self.assertEqual(object_key, f"{metadata_uuid}.metadata")

            self.assertEqual(sqs_args[idx][1]["QueueUrl"],
                             "https://sqs.eu-west-2.amazonaws.com/123456789/testenv-dr2-copy-files-from-dri")
            sent_body = json.loads(sqs_args[idx][1]["MessageBody"])
            self.assertEqual(sent_body["fileId"], rows[idx][1])
            self.assertEqual(sent_body["bucket"], "testenv-dr2-ingest-raw-cache")

    def test_skeleton_suite_lookup(self):
        self.test_dir = tempfile.mkdtemp()
        os.environ['DROID_PATH'] = self.test_dir

        self.prefixes = ['fmt', 'x-fmt']
        for prefix in self.prefixes:
            os.mkdir(os.path.join(self.test_dir, prefix))

        self.files = [
            ('fmt', 'fmt-123-signature-id-1.txt'),
            ('fmt', 'fmt-234-signature-id-2.bin'),
            ('fmt', 'other-file.txt'),
            ('x-fmt', 'x-fmt-345-signature-id-5.xml'),
            ('x-fmt', 'not-a-match.doc'),
        ]
        for prefix, filename in self.files:
            with open(os.path.join(self.test_dir, prefix, filename), 'w') as f:
                f.write('test')

        result = create_ingest_metadata.create_skeleton_suite_lookup(self.prefixes)

        expected_keys = [
            'fmt/123',
            'fmt/234',
            'x-fmt/345',
        ]

        for key in expected_keys:
            self.assertIn(key, result)
            file_path = result[key]['file_path']
            self.assertTrue(os.path.exists(file_path))

        self.assertNotIn('fmt/other', result)
        self.assertNotIn('x-fmt/not-a-match', result)

        self.assertTrue(result['fmt/123']['file_path'].endswith('fmt-123-signature-id-1.txt'))
        self.assertTrue(result['fmt/234']['file_path'].endswith('fmt-234-signature-id-2.bin'))
        self.assertTrue(result['x-fmt/345']['file_path'].endswith('x-fmt-345-signature-id-5.xml'))

    def test_calculate_checksum(self):
        self.test_file = tempfile.NamedTemporaryFile(delete=False)
        self.test_file.write(b'Test data for checksum\n')
        self.test_file.close()
        self.file_path = self.test_file.name

        self.expected_md5 = hashlib.md5(b'Test data for checksum\n').hexdigest()
        self.expected_sha256 = hashlib.sha256(b'Test data for checksum\n').hexdigest()

        result = create_ingest_metadata.calculate_checksum(self.file_path, 'md5')
        self.assertEqual(result, self.expected_md5)

        result = create_ingest_metadata.calculate_checksum(self.file_path, 'sha256')
        self.assertEqual(result, self.expected_sha256)

        with self.assertRaises(ValueError) as cm:
            create_ingest_metadata.calculate_checksum(self.file_path, 'notahash')
        self.assertEqual(str(cm.exception), "Unsupported hash algorithm: notahash")


if __name__ == '__main__':
    unittest.main()
