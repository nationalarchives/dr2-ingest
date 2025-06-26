# Postingest Message Resender

This lambda is invoked by a scheduled event to resend messages that have not been processed by the downstream systems for a period beyond the SQS queue's message retention period.

## Lambda Logic
This lambda is triggered periodically by a scheduled event. On invocation, it performs the following steps:
1. It retrieves the configuration for the queues from environment variables.
2. It traverses the configured queues.
3. For each queue,
   * It retrieves the message retention period for that queue using the `getQueueAttributes` method.
   * It retrieves all items from the dr2-postingest-state table that match the queue's alias and are older than the message retention period.
   * For each such item, it constructs a message and sends it to the corresponding SQS queue using the `sendMessage` method.
   * It updates the dr2-postingest-state table and sets the `lastQueued` timestamp to be current datetime.
4. Once all queues have been traversed, the lambda terminates.
 

## Environment Variables

| Name                                       | Description                                                               |
|--------------------------------------------|---------------------------------------------------------------------------|
| POST_INGEST_STATE_DDB_TABLE                | The name of the Postingest state dynamo table <br/>(dr2-postingest-state)                     |
| POST_INGEST_DDB_TABLE_BATCHPARENT_GSI_NAME | The global secondary index name. Used to search by batchId and parentPath |
| POST_INGEST_QUEUES                         | The config for the queues                                                 |