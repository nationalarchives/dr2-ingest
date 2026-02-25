import hashlib
import json
import os
import tempfile
import unittest
from pathlib import PureWindowsPath
from sndhdr import test_au
from unittest.mock import patch, MagicMock, mock_open, call
from parameterized import parameterized
from migrate import create_ingest_metadata


def setup_test(mock_checksum, mock_connect, mock_create_skeleton, rows, test_run):
    os.environ['CLIENT_LOCATION'] = '/test/client'
    os.environ['STORE_PASSWORD'] = 'password'
    os.environ['DROID_PATH'] = '/test/droid'
    os.environ['ACCOUNT_NUMBER'] = '123456789'
    os.environ['ENVIRONMENT'] = 'testenv'
    os.environ['NETWORK_LOCATION'] = '/network-location'

    if test_run:
        os.environ['TEST_RUN'] = test_run
    elif 'TEST_RUN' in os.environ:
        os.environ.pop('TEST_RUN')


    mock_create_skeleton.return_value = {
        "fmt/123": {"file_path": "/test/file1"},
        "x-fmt/123": {"file_path": "/test/file2"}
    }
    mock_cursor = MagicMock()
    mock_cursor.description = [
        ("PUID",), ("UUID",), ("UNITREF",), ("FILEID",), ("FILE_PATH",), ("FIXITIES",),
        ("SERIES",), ("DESC1",), ("DESC2",), ("TRANSFERINITIATEDDATETIME",),
        ("CONSIGNMENTREFERENCE",), ("DRIBATCHREFERENCE",), ("FILENAME",),
        ("FILEREFERENCE",), ("METADATA",), ("MANIFESTATIONRELREF",), ("TYPEREF",), ('SORTORDER',), ('SECURITYTAG',)
    ]

    mock_cursor.fetchmany.side_effect = [rows, []]
    mock_connect.return_value.cursor.return_value = mock_cursor
    mock_checksum.return_value = "abc123"


class TestMigrate(unittest.TestCase):
    @parameterized.expand([
        ('tru','test'),
        ('true','abc123'),
        ('false','test'),
        ('test','test'),
        (None,'abc123'),
    ])
    @patch('oracledb.connect')
    @patch('oracledb.init_oracle_client')
    @patch('builtins.open', new_callable=mock_open, read_data="SELECT * FROM TEST")
    @patch('migrate.create_ingest_metadata.create_skeleton_suite_lookup')
    @patch('migrate.create_ingest_metadata.calculate_checksum')
    @patch('migrate.create_ingest_metadata.get_clients')
    def test_migrate_s3_sqs(
            self, test_run, checksum, get_clients, mock_checksum,
            mock_create_skeleton, _, __, mock_connect
    ):
        row_fmt = [
            "fmt/123", "uuid-abc", "unitref-abc", "fileid-xyz", "/test/file1",
            json.dumps([{"SHA256": "test"}]),
            "series1", "desc1", "desc2", "2021-01-01", "consignment", "batch-ref",
            "filename.txt", "fileref", "meta", "1", "1", 1, "BornDigital"
        ]
        row_x_fmt = [
            "x-fmt/123", "uuid-def", "unitref-def", "fileid-xyz", "/test/file2",
            json.dumps([{"SHA256": "test"}]),
            "series1", "desc1", "desc2", "2021-01-01", "consignment", "batch-ref",
            "filename.txt", "fileref", "meta", "1", "1", 1, "Surrogate"
        ]
        rows = [row_fmt, row_x_fmt]

        setup_test(mock_checksum, mock_connect, mock_create_skeleton, rows, test_run)

        mock_s3 = MagicMock()
        mock_sqs = MagicMock()
        get_clients.return_value = (mock_s3, mock_sqs,)

        create_ingest_metadata.migrate()

        if test_run == "true" or not test_run:
            call_paths = ("/test/file1", "/test/file2")
        else:
            call_paths = (PureWindowsPath("/network-location/test/file1"), PureWindowsPath("/network-location/test/file2"))

        calls = [
            call(call_paths[0], "testenv-dr2-ingest-dri-migration-cache", "uuid-abc/fileid-xyz"),
            call(call_paths[1], "testenv-dr2-ingest-dri-migration-cache", "uuid-def/fileid-xyz")
        ]

        mock_s3.upload_file.assert_has_calls(calls)
        s3_args = mock_s3.upload_fileobj.call_args_list
        sqs_args = mock_sqs.send_message_batch.call_args_list

        sent_ids = {x['Id'] for x in sqs_args[0][1]["Entries"]}
        self.assertEqual(2, len(sent_ids))

        for idx, s3_arg in enumerate(s3_args):
            bytes_request, bucket, object_key = s3_arg[0]
            metadata_bytes = bytes_request.getvalue()
            metadata = json.loads(metadata_bytes.decode("utf-8"))[0]
            metadata_uuid = rows[idx][1]
            unit_ref = rows[idx][2]
            expected_digital_asset_source = "BornDigital" if idx == 0 else "Surrogate"

            self.assertEqual(metadata_uuid, metadata["UUID"])
            self.assertEqual(unit_ref.replace("-", ""), metadata["IAID"])
            self.assertEqual("series1", metadata["Series"])
            self.assertEqual(checksum, metadata["checksum_sha256"])
            self.assertEqual(expected_digital_asset_source, metadata["digitalAssetSource"])
            self.assertEqual(1, metadata["sortOrder"])
            self.assertEqual("testenv-dr2-ingest-dri-migration-cache", bucket)
            self.assertEqual(f"{metadata_uuid}.metadata", object_key)

            self.assertEqual("https://sqs.eu-west-2.amazonaws.com/123456789/testenv-dr2-preingest-dri-importer",
                             sqs_args[0][1]["QueueUrl"])
            sent_entries = [json.loads(x['MessageBody']) for x in sqs_args[0][1]["Entries"]]
            sent_body = sent_entries[idx]
            self.assertEqual(rows[idx][1], sent_body["assetId"])
            self.assertEqual("testenv-dr2-ingest-dri-migration-cache", sent_body["bucket"])

    @patch('oracledb.connect')
    @patch('oracledb.init_oracle_client')
    @patch('builtins.open', new_callable=mock_open, read_data="SELECT * FROM TEST")
    @patch('migrate.create_ingest_metadata.create_skeleton_suite_lookup')
    @patch('migrate.create_ingest_metadata.calculate_checksum')
    @patch('migrate.create_ingest_metadata.get_clients')
    def test_migrate_raises_error_if_consignment_ref_and_batch_ref_are_missing(
            self, get_clients, mock_checksum,
            mock_create_skeleton, ___, ____, mock_connect,
    ):
        row = [
            "fmt/123", "uuid-abc", "unitref-abc", "fileid-xyz", "/test/file1",
            json.dumps([{"SHA256": "test"}]),
            "series1", "desc1", "desc2", "2021-01-01", None, None,
            "filename.txt", "fileref", "meta", "1", "1", 1, "BornDigital"
        ]
        setup_test(mock_checksum, mock_connect, mock_create_skeleton, [row], None)
        mock_s3 = MagicMock()
        mock_sqs = MagicMock()
        get_clients.return_value = mock_s3, mock_sqs

        with self.assertRaises(ValueError) as cm:
            create_ingest_metadata.migrate()

        self.assertEqual("We need either a consignment reference or a dri batch reference", str(cm.exception))

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
        self.assertEqual(self.expected_md5, result)

        result = create_ingest_metadata.calculate_checksum(self.file_path, 'sha256')
        self.assertEqual(self.expected_sha256, result)

        with self.assertRaises(ValueError) as cm:
            create_ingest_metadata.calculate_checksum(self.file_path, 'notahash')
        self.assertEqual("Unsupported hash algorithm: notahash", str(cm.exception))

    def test_redacted_processing(self):
        metadata_redacted_one = {'UUID': '72918742-af2e-4007-b630-0785c94a7526', 'FileReference': 'ABC/Z', 'IAID': '556a5d2bcf4e4383aaeb1897b180a295'}
        metadata_redacted_two = {'UUID': '059a3c12-812d-45ce-afa0-d3adffb9c8b7', 'FileReference': 'ABC/Z', 'IAID': '28618217be0541119a8d493063b57ba8'}
        metadata_standard = {'UUID': '00117826-c0b7-4485-b16f-6c996f0e331c', 'FileReference': 'DEF/Z', 'IAID': 'f8185e53ac554715ae1f1b7bdee0c3b2'}
        assets = [
            {'type_ref': 100, 'rel_ref': 2, 'metadata': metadata_redacted_one},
            {'type_ref': 1, 'rel_ref': 1, 'metadata': metadata_redacted_two},
            {'type_ref': 1, 'rel_ref': 1, 'metadata': metadata_standard}
        ]
        processed_assets = create_ingest_metadata.process_redacted(assets)

        def count(key, value):
            return sum(1 for asset in processed_assets if asset['metadata'][key] == value)

        self.assertEqual(3, len(processed_assets))
        self.assertEqual(1, count('UUID', '72918742-af2e-4007-b630-0785c94a7526'))
        self.assertEqual(1, count('UUID', '059a3c12-812d-45ce-afa0-d3adffb9c8b7'))
        self.assertEqual(1, count('UUID', '00117826-c0b7-4485-b16f-6c996f0e331c'))
        self.assertEqual(1, count('IAID', '556a5d2bcf4e4383aaeb1897b180a295_1'))
        self.assertEqual(1, count('IAID', '28618217be0541119a8d493063b57ba8'))
        self.assertEqual(1, count('IAID', 'f8185e53ac554715ae1f1b7bdee0c3b2'))
        self.assertEqual(1, count('FileReference', 'ABC/Z'))
        self.assertEqual(1, count('FileReference', 'ABC/Z/1'))
        self.assertEqual(1, count('FileReference', 'DEF/Z'))

    @patch('migrate.create_ingest_metadata.sts_client')
    def test_get_clients(self,  mock_sts_client):
        mock_sts_client.assume_role.return_value = {
            'Credentials': {'AccessKeyId': 'TestAccessKey', 'SecretAccessKey': 'TestSecret', 'SessionToken': 'TestSessionToken'},
        }
        def check_client(client):
            self.assertEqual('TestAccessKey', client._get_credentials().access_key)
            self.assertEqual('TestSecret', client._get_credentials().secret_key)
            self.assertEqual('TestSessionToken', client._get_credentials().token)

        (s3_client, sqs_client) = create_ingest_metadata.get_clients(12345, "test")

        sts_args = mock_sts_client.assume_role.call_args_list[0][1]
        self.assertEqual('arn:aws:iam::12345:role/test-dr2-ingest-dri-migration-role', sts_args['RoleArn'])
        self.assertEqual('dri-migration', sts_args['RoleSessionName'])

        check_client(s3_client)
        check_client(sqs_client)


if __name__ == '__main__':
    unittest.main()
