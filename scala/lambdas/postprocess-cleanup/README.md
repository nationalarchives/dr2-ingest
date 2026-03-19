# Post process cleanup
This lambda is invoked when the ingesting is completed. There is an SQS queue that subscribes to the external SNS 
notifications. When it receives a message for ingest completion, it invokes this lambda.

## Lambda input
Input to this lambda is provided by SQS, which is subscribed to the SNS topic that receives notifications for ingest completion. 
The SQSEvent that comes as an input has "Records" with a "body" field which contains the message sent to the SNS topic. A sample input looks like below:

```json
{
  "body": {
    "properties": {
      "executionId": "TDR_d719d44b-85d3-4549-a7b9-ba19474cf09d_0", 
      "messageType": "preserve.digital.asset.ingest.complete"
    },
    "parameters": {
      "assetId": "3e2a21cc-a0b0-45f5-a3fc-22e26eb07212",
      "status": "Asset has been written to custodial copy disk."
    }
  }
}
```

## Lambda logic
The lambda performs the following steps:
1 It reads the message from the SQS event and extracts the assetId and executionId from the message body.
2 It updates the time-to-live attribute of the item in the DynamoDB table for the given assetId and executionId. The time-to-live is set to 1 day from the current time. This means that the item will be automatically deleted from the DynamoDB table after 1 day.
3 It finds all children of the Asset in the DynamoDB table using the parentPath attribute
4 For each child item, it updates the time-to-live attribute to 1 day from the current time, similar to step 2.
5 For each child item, it finds the location attribute and adds a "TO_BE_DELETED" tag to object in S3 for that key.
6 It finds all the ancestors of the Asset in the DynamoDB table using the parentPath attribute and updates the time-to-live attribute to 1 day from the current time, similar to step 2.

## Environment Variables
| Name                                 | Description                                                               |
|--------------------------------------|---------------------------------------------------------------------------|
| FILES_DDB_TABLE                      | The name of the DynamoDB table that contains the files metadata           |
| FILES_DDB_TABLE_BATCHPARENT_GSI_NAME | The global secondary index name. Used to search by batchId and parentPath |
| RAW_CACHE_BUCKET_NAME                | The name of the S3 bucket that contains the raw cache files.              |
