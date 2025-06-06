# Post-Ingest state change handler

This lambda is invoked via a Dynamo DB stream whenever an entry in Dynamo is changed ("MODIFY"), removed or added ("INSERT").
We're only interested in MODIFY and INSERT events at the moment so REMOVE is ignored.

## Lambda input

The input is provided by DynamoDB, a list of either:
```json
{
  "Records": [
    {
      "eventID": "d54bf46da49d9044706b8a8682fef203",
      "eventName": "INSERT",
      "eventVersion": "1.1",
      "eventSource": "aws:dynamodb",
      "awsRegion": "eu-west-2",
      "dynamodb": {
        "ApproximateCreationDateTime": 1720773442,
        "Keys": {
          "assetId": {
            "S": "assetId"
          },
          "batchId": {
            "S": "batchId"
          }
        },
        "OldImage": {
           "assetId": {
              "S": "assetId"
           },
           "batchId": {
              "S": "batchId"
           },
           "input": {
              "S": "input"
           },
           "correlationId": {
              "S": "id"
           }
        }, 
        "NewImage": {
          "assetId": {
            "S": "assetId"
          },
          "batchId": {
            "S": "batchId"
          },
          "input": {
            "S": "input"
          },
          "correlationId": {
             "S": "id"
          },
          "queue": {
             "S": "queue1"
          },
          "firstQueued": {
             "S": "2038-01-19T15:14:07.000Z"
          },
          "lastQueued": {
             "S": "2038-01-19T15:14:07.000Z"
          },
          "result_CC": {
             "S": "<result>"
          }
        },
        "SequenceNumber": "6200000000010677449965",
        "SizeBytes": 47,
        "StreamViewType": "NEW_IMAGE"
      },
      "eventSourceARN": "arn:aws:dynamodb:..."
    }
  ]
}
```
or

```json
{
  "Records": [
    {
      "eventID": "d54bf46da49d9044706b8a8682fef203",
      "eventName": "MODIFY",
      "eventVersion": "1.1",
      "eventSource": "aws:dynamodb",
      "awsRegion": "eu-west-2",
      "dynamodb": {
        "ApproximateCreationDateTime": 1720773442,
        "Keys": {
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "A"
          }
        },
        "NewImage": {
           "assetId": {
              "S": "assetId"
           },
           "batchId": {
              "S": "batchId"
           },
           "input": {
              "S": "input"
           },
           "correlationId": {
              "S": "id"
           },
           "queue": {
              "S": "queue1"
           },
           "firstQueued": {
              "S": "2038-01-19T15:14:07.000Z"
           },
           "lastQueued": {
              "S": "2038-01-19T15:14:07.000Z"
           },
           "result_CC": {
              "S": "<result>"
           }
        },
        "SequenceNumber": "6200000000010677449965",
        "SizeBytes": 47,
        "StreamViewType": "NEW_IMAGE"
      },
      "eventSourceARN": "arn:aws:dynamodb:..."
    }
  ]
}
```

## Lambda logic
The Lambda:
1. Gets the queue information from the config, this queue comprises:
   1. the queue alias - the name of the queue
   2. queue order - (starting from 1) the queue order denotes the order in which to call the queues
   3. queue url - the url to send the message to
   4. result attribute name - the name of the attribute (of the item) that has the queue result (state); this is defined in the DynamoFormatters library
   5. result attribute name case class alias - the name of the property in the `PostIngestStatusTableItem` case class that 
      will hold the result (state) of the queue;
      1. when DDB reads the item out of the table, it is saved to a case class and each attribute name is given a camel cased
         equivalent/alternative
      2. this property name will (and should) end with the queue alias and therefore can be searched for by using the alias
2. Checks for "INSERT" events; if one exists, it will:
   1. get the first queue (using the queueOrder)
   2. update the item in the DDB table with:
      1. the queue it will go to next
      2. when it was first queued
      3. when it was last queued
   3. send an SQS message with the:
      1. assetId
      2. batchId
      3. resultAttrName - the name that the next queue should use when updating the table
      4. payload - the "input" attribute on the item 
   4. send an SNS message with the:
      1. Properties:
         1. executionId - batchId
         2. messageId - randomly generated UUID
         3. parentMessageId - correlationId from the item
         4. timestamp - the datetime now
         5. messageType - will be "IngestUpdate"
      2. Parameters:
         1. assetId
         2. status - will be "IngestedPreservation"
3. Checks for "MODIFY" events; if one exists, it will:
   1. check if old item and new item exist, if not then it will throw an error
   2. convert the old and new item (image) objects to `Map`s
   3. for each queue in the list of queues:
      1. find the result attribute name case class alias in each of the (Map) items
      2. check if the value is different, if yes, then return the queue, if not, then continue to the next queue
   4. if at the end (of going through each queue):
      1. a queue has been found i.e. the attribute value of the queue of the new image is different from the old image, then
         1. if the number of items is the same as the queueOrder number, then
            1. delete the item from the table since there are no more queues left
            2. send an SNS message with the:
               1. Properties:
                  1. executionId - batchId
                  2. messageId - randomly generated UUID
                  3. parentMessageId - correlationId from the item
                  4. timestamp - the datetime now
                  5. messageType - will be IngestComplete
               2. Parameters:
                  1. assetId
                  2. status - will be IngestedCCDisk since that is the only queue at the moment
         2. if the number of items is less than the queueOrder number, then repeat the steps of the "INSERT" process (above), starting from steps 2
      2. a queue has not been found, throw an error because a modify event has been triggered but no change can be found
   5. send an SQS message with the:
      1. assetId
      2. batchId
      3. resultAttrName - the name that the next queue should use when updating the table
      4. payload - the "input" attribute on the item
   6. send an SNS message with the:
      1. Properties:
         1. executionId - batchId
         2. messageId - randomly generated UUID
         3. parentMessageId - correlationId from the item
         4. timestamp - the datetime now
         5. messageType - either IngestUpdate or IngestComplete
      2. Parameters:
         1. assetId
         2. status - either IngestedPreservation or IngestedCCDisk

Note: The queue configuration is defined in the `post_ingest.tf` terraform environments [file](https://github.com/nationalarchives/dr2-terraform-environments/blob/main/post_ingest/post_ingest.tf);
in order to add/remove a queue, change the alias name, add another property, modify this file. If the queue configuration
is modified, update the Decoder in the state change handler to account for the change(s).

There is no output from this Lambda. It sends a message to an SNS topic depending on its input and the state of other
entries in Dynamo.

## Environment Variables

| Name                                       | Description                                                               |
|--------------------------------------------|---------------------------------------------------------------------------|
| POST_INGEST_STATE_DDB_TABLE                | The name of the post ingest state dynamo table                            |
| POST_INGEST_DDB_TABLE_BATCHPARENT_GSI_NAME | The global secondary index name. Used to search by batchId and parentPath |
| OUTPUT_TOPIC_ARN                           | The output SNS topic arn                                                  |
| POST_INGEST_QUEUES                         | The config for the queues                                                 |


In the future as we add more locations to store the files, we will update the queues as well as the attributes of the DDB
item.
