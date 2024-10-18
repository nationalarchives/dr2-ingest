# DR2 Preingest TDR aggregator

The aggregation uses Lambda's [built-in batching](https://aws.amazon.com/about-aws/whats-new/2020/11/aws-lambda-now-supports-batch-windows-of-up-to-5-minutes-for-functions/) functionality and in-memory caching to group events into groups of a specified size. 
DynamoDB is used to prevent the same message being included in multiple groups due to SQS's at-least-once delivery guarantee. 
The application is designed to handle a failure of the aggregation-lambda, ensuring that events do not become stuck within the DynamoDB table and preventing re-processing.

The lambda is configured to receive messages from our input queue. The Event Source Mapping for this is configured with the highest possible BatchSize and MaximumBatchingWindowInSeconds; the Lambda won't be invoked until the payload size is 6MB or 300 seconds has elapsed since the first message was received.

On the surface, the processing this Lambda completes is relatively simple. It creates a new UUID to be used as the batchId, sends this UUID to our batch-queue with a delay, attempts to write the message to our lock-table, and returns a partial batch response so that only messages that were successfully written our lock-table are deleted from the input-queue. We send the batchId first to ensure that the batch is processed even if the Lambda times out before it finishes processing. 
And use a ConditionExpression when writing to DynamoDB to prevent the same message from being processed in multiple batches simultaneously.

However, due to Lambda's 6MB invocation payload limit and the large amount of non-optional SQS metadata included in each received message, we need to make this function smarter if it is to handle batches of more than a few thousand items. 
We do this by caching the current batchId outside of the handler function. To encourage Lambda to handle messages within the same function containers, we specify a MaximumConcurrency on the Event Source Mapping which prevents Lambda from scaling up with additional SQS pollers. As we now want multiple invocations of the Lambda function to complete before we begin processing the batch, we implement a MAX_SECONDARY_BATCHING_WINDOW variable into our code to specify how long we should wait before starting to process a batch.

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
| LOCK_DDB_TABLE                | The lock table name in Dynamo                                                           |
| MAX_BATCH_SIZE                | If the number of items in the input queue is greater than this, the lambda will trigger |
| MAX_SECONDARY_BATCHING_WINDOW | The maximum time before the lambda will trigger with items from the input queue         |
| PREINGEST_SFN_ARN             | The step function arn to run when a new group is created.                               |
| SOURCE_SYSTEM                 | Used to generate the group ID and batch ID                                              |