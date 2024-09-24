import os
import unittest
from unittest.mock import Mock

from boto3 import client
from moto import mock_aws

from cc_notifications import lambda_function


@mock_aws
class TestCcNotificationHandler(unittest.TestCase):
    table_name = "file-table-name"

    def setUp(self):
        self.set_aws_credentials()
        self.dynamodb_client = client("dynamodb",
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
                                          KeySchema=[
                                              {"AttributeName": "id", "KeyType": "HASH"},
                                              {"AttributeName": "batchId", "KeyType": "RANGE"}
                                          ],
                                          AttributeDefinitions=[
                                              {"AttributeName": "batchId", "AttributeType": "S"},
                                              {"AttributeName": "id", "AttributeType": "S"},
                                              {"AttributeName": "parentPath", "AttributeType": "S"}
                                          ],
                                          GlobalSecondaryIndexes=[{
                                              "IndexName": "BatchParentPathIdx",
                                              "KeySchema": [
                                                  {"AttributeName": "batchId", "KeyType": "HASH"},
                                                  {"AttributeName": "parentPath", "KeyType": "RANGE"}
                                              ],
                                              "Projection": {"ProjectionType": "ALL"}
                                          }],
                                          BillingMode="PAY_PER_REQUEST")

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

    def test_get_messages_from_json_event_should_return_all_messages_except_those_that_have_an_empty_identifier_and_are_deleted(
            self):
        event = self.default_event

        messages_with_identifier = lambda_function.get_messages_from_json_event(event)

        self.assertEqual(
            messages_with_identifier,
            [
                {"tableItemIdentifier": "identifier", "status": "Created"},
                {"tableItemIdentifier": "differentIdentifier", "status": "Updated"},
            ]
        )

    def test_get_messages_from_json_event_should_raise_error_if_message_not_present(self):
        event = {"Records": [{}]}

        self.assertRaises(KeyError, lambda_function.get_messages_from_json_event, event)

    def test_get_items_with_id_should_return_expected_item(self):
        self.create_table()
        self.put_item_in_table({"id": {"S": "identifier"}, "batchId": {"S": "batchIdValue"}})

        items_with_id = lambda_function.get_items_with_id(self.dynamodb_client, self.table_name, "id", "identifier")

        self.assertEqual(items_with_id, [{'id': {'S': 'identifier'}, 'batchId': {'S': 'batchIdValue'}}])

    def test_get_items_with_id_should_return_an_empty_list__if_item_not_in_table(self):
        self.create_table()

        items_with_id = lambda_function.get_items_with_id(self.dynamodb_client, self.table_name, "id",
                                                          "wrongIdentifier")

        self.assertEqual(items_with_id, [])

    def test_add_true_to_ingest_cc_attribute_should_add_ingest_cc_attr_with_true_value_if_it_does_not_exist(self):
        self.create_table()
        item = {"id": {"S": "identifier"}, "batchId": {"S": "batchIdValue"}}
        self.put_item_in_table(item)

        lambda_function.add_true_to_ingest_cc_attribute(self.dynamodb_client, self.table_name, "id", "batchId",
                                                        [item])

        item_response = self.get_item_from_table({"id": {"S": "identifier"}, "batchId": {"S": "batchIdValue"}})
        new_ingested_cc_value = item_response["Item"]["ingested_CC"]

        self.assertEqual(new_ingested_cc_value, {"S": "true"})

    def test_add_true_to_ingest_cc_attribute_should_not_add_ingest_cc_attr_with_true_value_if_it_already_exists(
            self):
        self.create_table()
        item = {"id": {"S": "identifier"}, "batchId": {"S": "batchIdValue"}, "ingested_CC": {"S": "true"}}
        self.put_item_in_table(item)

        table = Mock()
        table.update_item = Mock()

        lambda_function.add_true_to_ingest_cc_attribute(self.dynamodb_client, self.table_name, "id", "batchId",
                                                        [item])

        table.update_item.assert_not_called()

    def test_add_true_to_ingest_cc_attribute_should_add_ingest_cc_attr_with_true_value_if_current_value_is_not_true(
            self):
        self.create_table()
        identifier1 = {"id": {"S": "identifier1"}, "batchId": {"S": "batchIdValue"}, "ingested_CC": {"S": "false"}}
        identifier2 = {"id": {"S": "identifier2"}, "batchId": {"S": "batchIdValue"}, "ingested_CC": {"S": "tru"}}
        identifier3 = {"id": {"S": "identifier3"}, "batchId": {"S": "batchIdValue"}, "ingested_CC": {"S": "True"}}

        self.put_item_in_table(identifier1)
        self.put_item_in_table(identifier2)
        self.put_item_in_table(identifier3)

        lambda_function.add_true_to_ingest_cc_attribute(
            self.dynamodb_client, self.table_name, "id", "batchId", [identifier1, identifier2, identifier3]
        )

        item_response1 = self.get_item_from_table({"id": {"S": "identifier1"}, "batchId": {"S": "batchIdValue"}})
        item_response2 = self.get_item_from_table({"id": {"S": "identifier2"}, "batchId": {"S": "batchIdValue"}})
        item_response3 = self.get_item_from_table({"id": {"S": "identifier3"}, "batchId": {"S": "batchIdValue"}})

        responses = [item_response1, item_response2, item_response3]

        for response in responses:
            attribute_value = response["Item"]["ingested_CC"]
            self.assertEqual(attribute_value, {"S": "true"})

    def test_lambda_handler_should_write_true_to_ingest_cc_attr(self):
        os.environ["DYNAMO_TABLE_NAME"] = self.table_name
        self.create_table()
        self.put_item_in_table({"id": {"S": "identifier"}, "batchId": {"S": "batchIdValue"}})

        lambda_function.lambda_handler(self.default_event, None)
        item_response = self.get_item_from_table({"id": {"S": "identifier"}, "batchId": {"S": "batchIdValue"}})
        new_ingested_cc_value = item_response["Item"]["ingested_CC"]

        self.assertEqual(new_ingested_cc_value, {"S": "true"})

    def test_lambda_handler_should_raise_error_if_table_identifier_empty(self):
        os.environ["DYNAMO_TABLE_NAME"] = self.table_name
        self.create_table()
        self.put_item_in_table({"id": {"S": "identifier"}, "batchId": {"S": "batchIdValue"}})

        with self.assertRaises(ValueError) as context:
            lambda_function.lambda_handler(self.empty_table_identifier_event, None)

        expected_error = "Table identifier is missing for message {'tableItemIdentifier': '', 'status': 'Updated'}"
        self.assertEqual(expected_error, str(context.exception))

    empty_table_identifier_event = {
        "Records": [
            {
                "messageId": "dfef2ac4-7b37-437e-bf65-56e687784975",
                "receiptHandle": "AQEBzWwaftRI0KuVm4tP+/7q1rGgNqicHq...",
                "body": "{\"tableItemIdentifier\": \"\", \"status\":\"Updated\"}"
            }
        ]
    }

    default_event = {
        "Records": [
            {
                "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                "body": "{\"tableItemIdentifier\": \"identifier\", \"status\":\"Created\"}"
            },
            {
                "messageId": "2e1424d4-f796-459a-8184-9c92662be6da",
                "receiptHandle": "AQEBzWwaftRI0KuVm4tP+/7q1rGgNqicHq...",
                "body": "{\"tableItemIdentifier\": \"identifier\", \"status\":\"Deleted\"}"
            },
            {
                "messageId": "d17dbe4d-8336-4db8-b2df-b7f3000dd7ed",
                "receiptHandle": "AQEBzWwaftRI0KuVm4tP+/7q1rGgNqicHq...",
                "body": "{\"tableItemIdentifier\": \"\", \"status\":\"Deleted\"}"
            },
            {
                "messageId": "2e1424d4-f796-459a-8184-9c92662be6da",
                "receiptHandle": "AQEBzWwaftRI0KuVm4tP+/7q1rGgNqicHq...",
                "body": "{\"tableItemIdentifier\": \"differentIdentifier\", \"status\":\"Updated\"}"
            }
        ]
    }

if __name__ == "__main__":
    unittest.main()
