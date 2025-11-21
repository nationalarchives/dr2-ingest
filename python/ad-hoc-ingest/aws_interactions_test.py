import importlib
import json
from unittest import TestCase
from unittest.mock import patch, MagicMock

import aws_interactions

class Test(TestCase):

    @patch("aws_interactions.session")
    def test_get_account_number(self, mock_session):
        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_session.client.return_value = mock_sts

        account_num = aws_interactions.get_account_number()

        mock_session.client.assert_called_once_with("sts")
        mock_sts.get_caller_identity.assert_called_once()
        self.assertEqual("123456789012", account_num)

    @patch("aws_interactions.session")
    def test_upload_metadata(self, mock_session):
        mock_s3 = MagicMock()
        mock_session.client.return_value = mock_s3

        some_metadata = {"Series": "AB 1", "some_field": "some_value"}
        aws_interactions.upload_metadata("some_asset", "some_bucket", some_metadata)

        mock_session.client.assert_called_once_with("s3")
        self.assertTrue(mock_s3.upload_fileobj.called)
        args, kwargs = mock_s3.upload_fileobj.call_args

        fileobj = args[0]
        fileobj.seek(0)
        uploaded_bytes = fileobj.read()

        expected_bytes = json.dumps([some_metadata]).encode("utf-8")
        self.assertEqual(uploaded_bytes, expected_bytes)
        self.assertEqual("some_bucket", args[1])
        self.assertEqual("some_asset.metadata", args[2])

    @patch("aws_interactions.session")
    def test_upload_file(self, mock_session):
        mock_s3 = MagicMock()
        mock_session.client.return_value = mock_s3

        aws_interactions.upload_file("some_asset", "some_bucket", "file_id", "/home/data/some_file.pdf")

        mock_session.client.assert_called_once_with("s3")
        self.assertTrue(mock_s3.upload_file.called)
        args, kwargs = mock_s3.upload_file.call_args

        self.assertEqual("/home/data/some_file.pdf", args[0])
        self.assertEqual("some_bucket", args[1])
        self.assertEqual("some_asset/file_id", args[2])

    @patch("aws_interactions.get_region")
    @patch("aws_interactions.session")
    def test_send_sqs_message(self, mock_session, mock_region):
        mock_region.return_value = "london-town"

        mock_sqs = MagicMock()
        mock_session.client.return_value = mock_sqs

        aws_interactions.send_sqs_message("some_asset", "some_bucket", "https://some-queue-url")
        expected_message_body = json.dumps({"assetId": "some_asset", "bucket": "some_bucket"})

        mock_session.client.assert_called_once()
        args, kwargs = mock_session.client.call_args
        self.assertEqual("sqs", args[0])
        self.assertEqual("london-town", kwargs["config"].region_name)


        mock_sqs.send_message.assert_called_once()
        _, send_kwargs = mock_sqs.send_message.call_args

        self.assertEqual("https://some-queue-url", send_kwargs["QueueUrl"])
        self.assertEqual(expected_message_body, send_kwargs["MessageBody"])

    @patch("aws_interactions.boto3.Session")
    def test_refresh_session(self, mock_session):
        new_session = MagicMock()
        mock_session.return_value = new_session

        aws_interactions.refresh_session()

        mock_session.assert_called_once_with()

        self.assertEqual(new_session, aws_interactions.session)

    @patch("aws_interactions.boto3.Session")
    def test_get_region(self, mock_session):
        patched_session = MagicMock()
        patched_session.region_name = "london-town"
        mock_session.return_value = patched_session

        importlib.reload(aws_interactions) #reload using patched session

        region = aws_interactions.get_region()

        self.assertEqual("london-town", region)