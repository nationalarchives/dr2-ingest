import hashlib
import json
import os
import shutil
import tempfile
from unittest import TestCase
from unittest.mock import MagicMock, patch

import version_check


class Test(TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _write_file(self, name, content):
        path = os.path.join(self.test_dir, name)
        with open(path, "wb") as f:
            f.write(content if isinstance(content, bytes) else content.encode())
        return path

    @staticmethod
    def mock_urlopen(data):
        mock_response = MagicMock()
        mock_response.read.return_value = data if isinstance(data, bytes) else data.encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        return mock_response

    # --- fetch_json ---

    @patch("urllib.request.urlopen")
    def test_fetch_json_should_return_parsed_json(self, mock_urlopen):
        payload = [{"name": "file.py", "type": "file"}]
        mock_urlopen.return_value = self.mock_urlopen(json.dumps(payload))
        result = version_check.fetch_json("https://example.com/api")
        self.assertEqual(payload, result)
        mock_urlopen.assert_called_once()

    @patch("urllib.request.urlopen")
    def test_fetch_bytes_should_return_raw_bytes(self, mock_urlopen):
        expected = b"raw file content"
        mock_urlopen.return_value = self.mock_urlopen(expected)
        result = version_check.fetch_bytes("https://example.com/file.py")
        self.assertEqual(expected, result)


    def test_md5_of_bytes_should_return_correct_hex_digest(self):
        data = b"test"
        expected = hashlib.md5(data).hexdigest()
        self.assertEqual(expected, version_check.md5_of_bytes(data))

    def test_md5_of_bytes_should_return_different_digests_for_different_inputs(self):
        self.assertNotEqual(
            version_check.md5_of_bytes(b"content_a"),
            version_check.md5_of_bytes(b"content_b"),
        )

    @patch("version_check.fetch_json")
    def test_fetch_remote_files_should_return_only_non_test_files(self, mock_fetch_json):
        mock_fetch_json.return_value = [
            {"name": "ad_hoc_ingest.py", "type": "file"},
            {"name": "ad_hoc_ingest_test.py", "type": "file"},
            {"name": "some_dir", "type": "dir"},
            {"name": "version_check.py", "type": "file"},
        ]
        result = version_check.fetch_remote_files()
        names = [e["name"] for e in result]
        self.assertIn("ad_hoc_ingest.py", names)
        self.assertIn("version_check.py", names)
        self.assertNotIn("ad_hoc_ingest_test.py", names)
        self.assertNotIn("some_dir", names)

    @patch("version_check.fetch_json")
    def test_fetch_remote_files_should_return_empty_list_when_no_matching_entries(self, mock_fetch_json):
        mock_fetch_json.return_value = [
            {"name": "dataset_validator_test.py", "type": "file"},
            {"name": "subdir", "type": "dir"},
        ]
        result = version_check.fetch_remote_files()
        self.assertEqual([], result)

    # --- is_latest_version ---

    @patch("version_check.fetch_bytes")
    @patch("version_check.fetch_remote_files")
    def test_is_latest_version_should_return_true_when_all_files_match(self, mock_remote_files, mock_fetch_bytes):
        content = b"print('hello')"
        self._write_file("ad_hoc_ingest.py", content)
        self._write_file("version_check.py", content)

        mock_remote_files.return_value = [
            {"name": "ad_hoc_ingest.py", "download_url": "https://example.com/ad_hoc_ingest.py"},
            {"name": "version_check.py", "download_url": "https://example.com/version_check.py"},
        ]
        mock_fetch_bytes.return_value = content

        with patch("os.walk") as mock_walk, patch("os.listdir") as mock_listdir:
            mock_walk.return_value = [(self.test_dir, [], ["ad_hoc_ingest.py", "version_check.py"])]
            mock_listdir.return_value = ["ad_hoc_ingest.py", "version_check.py"]
            result = version_check.is_latest_version()

        self.assertTrue(result)

    @patch("version_check.fetch_bytes")
    @patch("version_check.fetch_remote_files")
    def test_is_latest_version_should_return_false_when_file_content_differs(self, mock_remote_files, mock_fetch_bytes):
        local_content = b"local version"
        remote_content = b"remote version"
        self._write_file("ad_hoc_ingest.py", local_content)
        self._write_file("version_check.py", local_content)

        mock_remote_files.return_value = [
            {"name": "version_check.py", "download_url": "https://example.com/version_check.py"},
        ]
        mock_fetch_bytes.return_value = remote_content

        with patch("os.walk") as mock_walk, patch("os.listdir") as mock_listdir:
            mock_walk.return_value = [(self.test_dir, [], ["ad_hoc_ingest.py", "version_check.py"])]
            mock_listdir.return_value = ["ad_hoc_ingest.py", "version_check.py"]
            result = version_check.is_latest_version()

        self.assertFalse(result)

    @patch("version_check.fetch_bytes")
    @patch("version_check.fetch_remote_files")
    def test_is_latest_version_should_return_false_when_remote_file_is_missing_locally(self, mock_remote_files, mock_fetch_bytes):
        self._write_file("ad_hoc_ingest.py", b"content")

        mock_remote_files.return_value = [
            {"name": "new_module.py", "download_url": "https://example.com/new_module.py"},
        ]

        with patch("os.walk") as mock_walk, patch("os.listdir") as mock_listdir:
            mock_walk.return_value = [(self.test_dir, [], ["ad_hoc_ingest.py"])]
            mock_listdir.return_value = ["ad_hoc_ingest.py"]
            result = version_check.is_latest_version()

        self.assertFalse(result)
        mock_fetch_bytes.assert_not_called()

    @patch("version_check.fetch_bytes")
    @patch("version_check.fetch_remote_files")
    def test_is_latest_version_should_return_true_when_there_are_no_remote_files(self, mock_remote_files, mock_fetch_bytes):
        self._write_file("ad_hoc_ingest.py", b"content")

        mock_remote_files.return_value = []

        with patch("os.walk") as mock_walk, patch("os.listdir") as mock_listdir:
            mock_walk.return_value = [(self.test_dir, [], ["ad_hoc_ingest.py"])]
            mock_listdir.return_value = ["ad_hoc_ingest.py"]
            result = version_check.is_latest_version()

        self.assertTrue(result)
        mock_fetch_bytes.assert_not_called()
