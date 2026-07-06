# DR2 Send to Post-Ingest State Change DDB Queue

## Input

The lambda is triggered via a DynamoDB stream whenever an entry in DynamoDB is changed ("MODIFY"),
removed or added ("INSERT").

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

## Output

The lambda doesn't return anything, but it passes the event on to an SQS `QUEUE_URL`


## Steps

1. Send the `event` recevied to the `QUEUE_URL`

[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments)

## Environment Variables

| Name               | Description                                          |
|--------------------|------------------------------------------------------|
| QUEUE_URL   | The SQS queue to send the DynamoDB stream message to |