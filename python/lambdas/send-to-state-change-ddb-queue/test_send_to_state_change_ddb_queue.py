import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

import send_to_state_change_ddb_queue


@patch.dict("os.environ", {"QUEUE_URL": "destination-queue"})
@patch("send_to_state_change_ddb_queue.sqs_client.send_message")
class TestLambdaFunction(unittest.TestCase):
    def test_lambda_handler_sends_message_to_sqs(self, send_message):
        send_to_state_change_ddb_queue.lambda_handler({"Records": [{"body": "body"}]}, None)
        self.assertEqual(1, send_message.call_count)
        self.assertEqual(
            {"QueueUrl": "destination-queue", "MessageBody": '{"Records": [{"body": "body"}]}'},
            send_message.call_args_list[0].kwargs
        )
