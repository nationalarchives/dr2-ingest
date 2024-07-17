# Ingest files change handler

This lambda is invoked via a Dynamo DB stream whenever an entry in Dynamo is changed, removed or added.
We're only interested in changed entries at the moment so all others are ignored.

## Lambda input

The input is provided by Dynamo DB.

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
          "ingested_PS": {
            "BOOL": true
          },
          "id": {
            "S": "1"
          },
          "batchId": {
            "S": "A"
          },
          "type": {
            "S": "Asset"
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

There is no output from this Lambda. It sends a message to an SNS topic depending on its input and the state of other
entries in Dynamo.

[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments)

## Environment Variables

| Name              | Description                                                               |
|-------------------|---------------------------------------------------------------------------|
| DYNAMO_TABLE_NAME | The name of the ingest files dynamo table                                 |
| DYNAMO_GSI_NAME   | The global secondary index name. Used to search by batchId and parentPath |
| TOPIC_ARN         | The output SNS topic arn                                                  |

## Lambda logic
The logic which determines whether to send a message and which message is described with this pseudocode

```text
if item.type == "File":
  if item.ingested_CC:
    Fetch parent Asset item
    Run logic below (for the Asset)
    
if item.type == "Asset":
  if item.ingested_PS and item.ingested_CC:
    Query for child items
    if ingested_CC==true for all children:
      Send ingest complete message
      Delete Asset And children
      Query for other Assets with the same id
      for each item with same id (j):
        if j.ingested_PS and j.skipIngest:
          Send ingest complete message
          Delete Asset j
    return
  if item.ingested_PS and item.skipIngest:
    Send ingest update message
    Query for items with the same id
    if no items with same id:
      Send ingest complete message
      Delete Asset And children
    else
      Delete children (optional)
    return
  if item.ingested_PS:
    Send ingest update message
    return
```

## Test cases

[These are the test cases](./TestCases.md) which are covered in the [LambdaTest](./src/test/scala/uk/gov/nationalarchives/ingestfileschangehandler/LambdaTest.scala)

