import unittest
from unittest.mock import MagicMock, call, mock_open, patch

from botocore.exceptions import ClientError

from migrate import copy_metadata_to_live


class TestCopyMetadataToLive(unittest.TestCase):
    def test_iter_s3_keys_skips_directory_keys(self):
        s3_client = MagicMock()
        s3_client.get_paginator.return_value.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "working/ABC/file1.json"},
                    {"Key": "working/ABC/subdirectory/"},
                ]
            },
            {"Contents": [{"Key": "working/ABC/file2.json"}]},
        ]

        result = list(copy_metadata_to_live.iter_s3_keys(s3_client, "bucket", "working/ABC/"))

        self.assertEqual(["working/ABC/file1.json", "working/ABC/file2.json"], result)
        s3_client.get_paginator.assert_called_once_with("list_objects_v2")
        s3_client.get_paginator.return_value.paginate.assert_called_once_with(
            Bucket="bucket",
            Prefix="working/ABC/",
        )

    def test_copy_to_destination_prefix_uses_source_filename(self):
        s3_client = MagicMock()

        copy_metadata_to_live.copy_to_destination_prefix(
            s3_client,
            "bucket",
            "working/ABC/nested/file.json",
            "live/",
        )

        s3_client.copy_object.assert_called_once_with(
            Bucket="bucket",
            CopySource={"Bucket": "bucket", "Key": "working/ABC/nested/file.json"},
            Key="live/file.json",
        )

    @patch("migrate.copy_metadata_to_live.print")
    @patch("builtins.open", new_callable=mock_open)
    @patch("migrate.copy_metadata_to_live.os.getcwd", return_value="/working")
    @patch("migrate.copy_metadata_to_live.as_completed")
    @patch("migrate.copy_metadata_to_live.ThreadPoolExecutor")
    @patch("migrate.copy_metadata_to_live.iter_s3_keys")
    @patch("migrate.copy_metadata_to_live.boto3.client")
    def test_main_counts_successful_and_failed_copies(
        self,
        mock_client,
        mock_iter_s3_keys,
        mock_executor_class,
        mock_as_completed,
        _,
        mock_open_file,
        mock_print,
    ):
        s3_client = MagicMock()
        mock_client.return_value = s3_client
        source_keys = ["working/ABC/file1.json", "working/ABC/file2.json"]
        mock_iter_s3_keys.return_value = iter(source_keys)
        first_future = MagicMock()
        second_future = MagicMock()
        second_future.result.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            "CopyObject",
        )
        executor = mock_executor_class.return_value.__enter__.return_value
        executor.submit.side_effect = [first_future, second_future]
        mock_as_completed.return_value = [first_future, second_future]

        copy_metadata_to_live.main(["ABC"])

        mock_client.assert_called_once_with("s3")
        mock_iter_s3_keys.assert_called_once_with(
            s3_client,
            "prod-dr2-records-metadata",
            "working/ABC/",
        )
        self.assertEqual(
            [
                call(copy_metadata_to_live.copy_worker, "prod-dr2-records-metadata", source_keys[0], "live/"),
                call(copy_metadata_to_live.copy_worker, "prod-dr2-records-metadata", source_keys[1], "live/"),
            ],
            executor.submit.call_args_list,
        )
        first_future.result.assert_called_once_with()
        second_future.result.assert_called_once_with()
        mock_open_file.assert_called_once_with("failed.txt", "w")
        mock_open_file().writelines.assert_called_once_with([source_keys[1]])
        mock_print.assert_any_call("\nDone. Copied 1 objects, 1 failed.")
        mock_print.assert_any_call("Failed keys have been written to /working/failed.txt")

    @patch("migrate.copy_metadata_to_live.ThreadPoolExecutor")
    @patch("migrate.copy_metadata_to_live.iter_s3_keys")
    @patch("migrate.copy_metadata_to_live.boto3.client")
    def test_main_reraises_listing_errors(self, mock_client, mock_iter_s3_keys, _):
        error = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            "ListObjectsV2",
        )
        mock_iter_s3_keys.side_effect = error

        with self.assertRaises(ClientError) as raised:
            copy_metadata_to_live.main(["ABC"])

        self.assertIs(error, raised.exception)
        mock_client.assert_called_once_with("s3")


if __name__ == "__main__":
    unittest.main()
