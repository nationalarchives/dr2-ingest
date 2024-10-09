# DR2 Ingest failure notifications

## Lambda input

The lambda takes the following input from an EventBridge event:

```json
{
  "detail": {
    "input": "{\"groupId\":\"TDR_c0ce1c2e-03fc-4cef-bc22-f95547ad3448\",\"batchId\":\"TDR_c0ce1c2e-03fc-4cef-bc22-f95547ad3448_0\"}"
  }
}

```

## Lambda output

There is no output

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

## Environment Variables

| Name                        | Description                                |
|-----------------------------|--------------------------------------------|
| DDB_LOCK_TABLE              | The name of the Dynamo lock table          |
| LOCK_DDB_TABLE_GROUP_ID_IDX | The name of the lock table secondary index |
| TOPIC_ARN                   | The arn of the notifications topic         |

