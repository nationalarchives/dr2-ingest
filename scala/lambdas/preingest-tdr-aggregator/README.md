# DR2 Preingest TDR aggregator

The aggregation uses Lambda's [built-in batching](https://aws.amazon.com/about-aws/whats-new/2020/11/aws-lambda-now-supports-batch-windows-of-up-to-5-minutes-for-functions/) functionality and in-memory caching to group events into groups of a specified size. 
DynamoDB is used to prevent the same message being included in multiple groups due to SQS's at-least-once delivery guarantee. 
The application is designed to handle a failure of the aggregation-lambda, ensuring that events do not become stuck within the DynamoDB table and preventing re-processing.

There is a method to create a new group.
* Generate a new group ID 
* Generate an expiry time of the lambda timeout plus the batching window.
* Generate a batch ID which is "${groupId}_0"
* Generate a delay time which is the expiry time - the current time
* Start a step function with these parameters.

The lambda works out whether to create a new group with these steps.

* Check the cache for an existing group id with the event source arn as the key.
* If there is no group in the cache then create a new group
* If there is a group in the cache then:
  * If the current group expiry occurs before the lambda timeout, then create a new group.
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