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

## Environment Variables

| Name                            | Description                                 |
|---------------------------------|---------------------------------------------|
| LOCK_DDB_TABLE                  | The name of the Dynamo lock table.          |
| LOCK_DDB_TABLE_GROUPID_GSI_NAME | The name of the lock table secondary index. |
| OUTPUT_TOPIC_ARN                | The arn of the notifications topic.         |
