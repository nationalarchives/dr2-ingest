# DR2 Preingest TDR aggregator

This code is used for the TDR, Court Document and DRI migration preingest.

The Aggregator SQS queue sends messages to this lambda. In order to reduce the number of invocations, this lambda uses
Lambda's [built-in batching](https://aws.amazon.com/about-aws/whats-new/2020/11/aws-lambda-now-supports-batch-windows-of-up-to-5-minutes-for-functions/)
functionality and in-memory caching to group these messages into groups (batches) of a specified size. The Event Source Mapping,
a resource that connects an event source (the Aggregator SQS queue in this case) to a Lambda function, for this is configured
with the highest possible `BatchSize` and `MaximumBatchingWindowInSeconds`, meaning that this lambda won't be invoked
until the payload size (messages gathered) reaches 6MB or 10,000 messages, or 300 seconds has elapsed since the first message
was received. These values are subject to change.

However, due to Lambda's 6MB invocation payload limit and the large amount of non-optional SQS metadata included in each
received message, we need to make this function smarter if it is to handle batches of more than a few thousand items.
We do this by caching the current batchId outside the handler function. To encourage Lambda to handle messages within
the same function containers, we specify a MaximumConcurrency on the Event Source Mapping which prevents Lambda from
scaling up with additional SQS pollers.

DynamoDB (DDB) is used to prevent the same message being included in multiple groups due to SQS's at-least-once delivery guarantee.

## The Process

Once the lambda is invoked, each message is processed in parallel via a fibre. The process (for each message) is as follows:

1. Decode the message body into an `Input` (the structure of which is highlighted in the "Lambda input section")
2. Generate the group expiry time beforehand, in case a new group needs to be created in one of the next steps
   - This would be the lambdaTimeoutTime + maxSecondaryBatchingWindow
      - As we now want multiple invocations of the Lambda function to complete before we
        begin processing the batch, we use a maxSecondaryBatchingWindow, which is the maximum amount of time for a lambda to wait to process a batch
3. Check the group cache for an existing group, using the `sourceId` (event source arn) as the key
   1. If there is no group in the cache then create a new group
   2. If there is a group in the cache then:
      1. If the current group expiry occurs before the lambda timeout, then create a new group
         - Since the expiry comprises the lambdaTimeoutTime + maxSecondaryBatchingWindow, what determines whether to
           create a new group is how much time is left on the maxSecondaryBatchingWindow; if there is 0 time left, then
           the group is ready to be processed and should not have anything added to it
      2. If the item count is more than the batch size, create a new group
      3. Otherwise, increment the itemCount of the group (for that sourceId) and save it back to the cache
4. If a group is to be created then it will:
   1. Generate a new group ID
   2. Pass in the group expiry time
   3. Generate a batch ID which is "${groupId}_$retryCount" - retryCount is set to 0
   4. Generate a delay time which is the expiry time - the current time
      - This delay tells the next step function to wait until that delay is up, to start processing the group
   5. Write the `assetId` (from the message input), the `groupId`, message input and created at date to the DDB Lock Table
      - This is done before the step function call to ensure that the batch is processed even if the Lambda times out before it finishes processing.
      - The query contains a `ConditionExpression` on `assetId` in order to prevent the same message from being processed in multiple batches simultaneously.
   6. Start the preingest step function passing in the parameters mentioned in substeps 1-4
   7. Return the newly generated group
5. Return the messageId
6. Join up all fibres and count all that were successes and all that were failures (for whatever reason)
7. Log the successes and failures
8. Returns a list of `BatchItemFailure`s with the messageId as the identifier
   - This is done so that only messages that were successfully written to our lock-table are deleted from the input-queue later in the ingest process

The application is designed to handle a failure of this aggregation-lambda, ensuring that events do not become stuck within the DDB table and preventing re-processing.

## Lambda input

The lambda takes an SQS event as the input. The body of the event is:

```json
{
  "id": "709e506e-e9d2-416f-9c72-c11ddeead51e",
  "location": "s3://bucket/key",
  "messageId": "optionalMessageId"
}
```

## Lambda output

If a new group is created, the lambda will start the preingest step function with
this input (an example)
```json
{
  "groupId": "TST_2b1d1ab5-e4b6-4c18-bf45-0abe1f248dd6",
  "batchId": "TST_2b1d1ab5-e4b6-4c18-bf45-0abe1f248dd6_0",
  "waitFor": 100,
  "retryCount": 0
}
```

The lambda also returns a batch item response.
If no incoming messages failed to process:
```json
{ 
  "batchItemFailures": []
}
```

If there are failures:
```json
{
"batchItemFailures": [{ "itemIdentifier": "sqsMessageId" }]
}
```

If we report the failures, it prevents AWS from deleting the message which failed so it can be redelivered at another time.

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

## Environment Variables

| Name                          | Description                                                                             |
|-------------------------------|-----------------------------------------------------------------------------------------|
| LOCK_DDB_TABLE                | The lock table name in Dynamo                                                           |
| MAX_BATCH_SIZE                | If the number of items in the input queue is greater than this, the lambda will trigger |
| MAX_SECONDARY_BATCHING_WINDOW | The maximum time before a lambda will trigger with items from the input queue           |
| PREINGEST_SFN_ARN             | The step function arn to run when a new group is created.                               |
| SOURCE_SYSTEM                 | Used to generate the group ID and batch ID                                              |