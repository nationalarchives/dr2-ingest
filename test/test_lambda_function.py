import json
import os
import unittest
from unittest.mock import Mock

from boto3 import resource, client
from moto import mock_aws
from lambda_function import (get_message_from_json_event, get_item_with_id, add_true_to_ingest_cc_attribute,
                             lambda_handler)

from urllib3.exceptions import ConnectTimeoutError


@mock_aws
class TestCcNotificationHandler(unittest.TestCase):
    table_name = "file-table-name"

    dynamodb_client = client("dynamodb",
                             region_name="eu-west-2",
                             aws_access_key_id="test-access-key",
                             aws_secret_access_key="test-secret-key",
                             aws_session_token="test-session-token2")

    @staticmethod
    def set_aws_credentials():
        """Mocked AWS Credentials for moto."""
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        os.environ['AWS_SECURITY_TOKEN'] = 'testing'
        os.environ['AWS_SESSION_TOKEN'] = 'testing'
        os.environ['AWS_DEFAULT_REGION'] = 'eu-west-2'

    def create_table(self):
        self.set_aws_credentials()
        self.dynamodb_client.create_table(TableName=self.table_name,
                                          KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                                          AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                                          BillingMode="PAY_PER_REQUEST")

        return resource("dynamodb").Table(self.table_name)

    def put_item_in_table(self, item):
        self.dynamodb_client.put_item(
            TableName=self.table_name,
            Item=item
        )

    def get_item_from_table(self, item):
        return self.dynamodb_client.get_item(
            TableName=self.table_name,
            Key=item
        )

    def test_get_message_from_json_event_should_return_identifier_if_present(self):
        event = self.default_event

        message = get_message_from_json_event(event)["tableItemIdentifier"]

        self.assertEqual(message, "identifier")

    def test_get_message_from_json_event_should_raise_error_if_message_not_present(self):
        event = {"Records": [{}]}

        self.assertRaises(KeyError, get_message_from_json_event, event)

    def test_get_item_with_id_should_return_expected_item(self):
        table = self.create_table()
        self.put_item_in_table({"id": {"S": "identifier"}, "batchId": {"S": "batchIdValue"}})

        item_with_id = get_item_with_id(table, "id", "identifier")

        self.assertEqual(item_with_id, {"id": "identifier", "batchId": "batchIdValue"})

    def test_get_item_with_id_should_raise_exception_if_item_not_in_table(self):
        table = self.create_table()

        self.assertRaises(Exception, get_item_with_id, table, "id", "wrongIdentifier")

    def test_add_true_to_ingest_cc_attribute_should_write_true_to_ingest_cc_attr(self):
        table = self.create_table()
        self.put_item_in_table({"id": {"S": "identifier"}, "batchId": {"S": "batchIdValue"}})

        add_true_to_ingest_cc_attribute(table, "id", "batchId", {"id": "identifier", "batchId": "batchIdValue"})

        item_response = self.get_item_from_table({"id": {"S": "identifier"}})
        new_ingested_cc_value = item_response["Item"]["ingested_CC"]

        self.assertEqual(new_ingested_cc_value, {"S": "true"})

    def test_lambda_handler_should_write_true_to_ingest_cc_attr(self):
        os.environ["DYNAMO_TABLE_NAME"] = self.table_name
        self.create_table()
        self.put_item_in_table({"id": {"S": "identifier"}, "batchId": {"S": "batchIdValue"}})

        lambda_handler(self.default_event, None)

        item_response = self.get_item_from_table({"id": {"S": "identifier"}})
        new_ingested_cc_value = item_response["Item"]["ingested_CC"]

        self.assertEqual(new_ingested_cc_value, {"S": "true"})

    default_event = {
        "Records": [
            {
                "Sns": {
                    "SignatureVersion": "1",
                    "Timestamp": "2019-01-02T12:45:07.000Z",
                    "Signature": "tcc6faL2yUC6dgZdmrwh1Y4cGa/ebXEkAi6RibDsvpi+tE/1+82j...65r==",
                    "SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-ac565b8b1a6c5d002d285f9598aa1d9b.pem",
                    "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
                    "Message": "{\"tableItemIdentifier\": \"identifier\"}",
                    "MessageAttributes": {
                        "Test": {
                            "Type": "String",
                            "Value": "TestString"
                        },
                        "TestBinary": {
                            "Type": "Binary",
                            "Value": "TestBinary"
                        }
                    },
                    "Type": "Notification",
                    "UnsubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&amp;SubscriptionArn=arn:aws:sns:us-east-1:123456789012:test-lambda:21be56ed-a058-49f5-8c98-aedd2564c486",
                    "TopicArn": "arn:aws:sns:us-east-1:123456789012:sns-lambda",
                    "Subject": "TestInvoke"
                }
            }
        ]
    }


if __name__ == "__main__":
    unittest.main()
