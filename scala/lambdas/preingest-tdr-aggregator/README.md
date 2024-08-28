# DR2 Preingest TDR aggregator

A Lambda triggered by an aggregator SQS queue. It will run either when the number of messages reaches `MAX_BATCH_SIZE` or `MAX_SECONDARY_BATCHING_WINDOW` seconds have passed

There is a method to create a new group.
* Generate a new group ID 
* Generate an expiry time of the lambda timout plus the batching window.
* Generate a batch ID which is "${groupId}_0"
* Generate a delay time which is the expiry time - the current time
* Start a step function with these parameters.

The lambda works out whether to create a new group with these steps.

* Check the cache for an existing group id with the event source arn as the key.
* If there is no group in the cache then create a new group
* If there is a group in the cache then:
  * If the lambda timeout time is later than the current group expiry, then create a new group.
  * If the item count is more than the batch size, create a new group
  * Otherwise, increment the itemCount in the group cache

## Lambda input

The lambda takes an SQS event as the input. The body of the event is:

```json
{
  "id": "709e506e-e9d2-416f-9c72-c11ddeead51e",
  "location": "s3://bucket/key"
}
```

## Lambda output

The lambda does not output anything directly. If a new group is created, it will start the ingest step function with
this input

```json
{
  "groupId": "TST_2b1d1ab5-e4b6-4c18-bf45-0abe1f248dd6",
  "batchId": "TST_2b1d1ab5-e4b6-4c18-bf45-0abe1f248dd6_0",
  "waitFor": 100,
  "retryCount": 0
}
```

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

## Environment Variables

| Name                          | Description                                                                             |
|-------------------------------|-----------------------------------------------------------------------------------------|
| DDB_LOCK_TABLE                | The lock table name in Dynamo                                                           |
| MAX_SECONDARY_BATCHING_WINDOW | The maximum time before the lambda will trigger with items from the input queue         |
| MAX_BATCH_SIZE                | If the number of items in the input queue is greater than this, the lambda will trigger |
| SOURCE_SYSTEM                 | Used to generate the group ID and batch ID                                              |
| PREINGEST_SFN_ARN             | The step function arn to run when a new group is created.                               |