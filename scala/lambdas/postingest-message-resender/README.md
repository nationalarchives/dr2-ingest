# Post-Ingest Message Resender

This lambda is invoked by a scheduled event to resend messages that have not been successfully processed by the Post-Ingest Lambda.

## Lambda Logic
This lambda is triggered periodically by a scheduled event. On invocation, it performs the following steps.
1. It retrieves the configuration for the queues from environment variables.
2. It iterates over the configured queues.
3. For each queue,
   * It retrieves the message retention period for that queue using the `getQueueAttributes` method.
   * It retrieves all items in the Post Ingest State DynamoDB table that match the queue's alias and they are older than the message retention period.
   * For each such item, it constructs a message and sends it to the corresponding SQS queue using the `sendMessage` method.
   * It updates the Post Ingest State DynamoDB table to mark the message as resent by updating the `lastQueued` timestamp.
4. The lambda continues to process items for subsequent queues from the configuration.
5. Once all queues have been processed, the lambda invocation terminates.
 

## Environment Variables

| Name                                       | Description                                                               |
|--------------------------------------------|---------------------------------------------------------------------------|
| POST_INGEST_STATE_DDB_TABLE                | The name of the post ingest state dynamo table                            |
| POST_INGEST_DDB_TABLE_BATCHPARENT_GSI_NAME | The global secondary index name. Used to search by batchId and parentPath |
| POST_INGEST_QUEUES                         | The config for the queues                                                 |

 