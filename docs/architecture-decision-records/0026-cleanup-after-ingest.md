# 26. Cleanup After Ingest (Successful Ingest)

**Date:** 2026-02-12

## Context
As part of the ingest process, we save some information within our AWS infrastructure. The main places where such 
information is stored are S3 buckets and DynamoDB tables. Some of this information is useful while ingest is in 
progress. Once the ingest operation has finished—that is, once the various final storage locations for those records, 
such as the Preservation System, Custodial Copy, Tape Drive, etc., are reached—we do not want the data left behind 
within the S3 buckets or any processing information in the DynamoDB tables.

As of now, a notification from the Custodial Copy of a successful ingest is the logical endpoint of the ingest process.
However, with time, there may be more downstream processes added to the ingest process, and the logical endpoint will 
move. We have SNS notifications going out to any external or internal subscribers for completion of ingest. This 
notification is utilised as the starting point for the cleanup process.

We make use of AWS-supported options to achieve this rather than deleting the items ourselves. To that effect, we have 
a few components added to the process, as follows:

![Existing Process and New Lambda](/docs/images/adr/0026/current-with-added-lambda.png)

## Changes to DR2
The modifications are as follows:
* There is a new queue that listens for the SNS notifications we send out to external systems. This queue triggers the cleanup Lambda
* There is a new lambda function, triggered by the queue, for the cleanup process. This lambda function is responsible for soft delete 
  * The lambda function updates the item in the `Files` table to set ttl to +1 day from the time of execution
  * The lambda function tags the objects in `raw-cache` and `ingest-state` S3 buckets with `delete=true` 
* The soft delete mentioned above turns into a hard delete through AWS deletions. This is achieved by the following:
  * For DynamoDB tables, the value in a specific attribute (`ttl` in `Files` table) is set to be the driver for ttl. The `Files` table already has TTL attribute, called `ttl` configured, we will utilise the same 
  * For S3, we have a lifecycle rule to expire items after 1 day, where `delete = true`. The rule can be configured as shown below

    ```json
    {
      "Rules": [
        {
          "ID": "DeleteMarkedObjectsAfterOneDay",
          "Status": "Enabled",
          "Filter": {
            "Tag": {
              "Key": "delete",
              "Value": "true"
            }
          },
          "Expiration": {
            "Days": 1
          }
        }
      ]
    }
    ```
## Ingest complete message
The message looks as shown below. The lambda makes use of `assetId` and `executionId` from the message.
```json
{
  "body": {
    "parameters": {
      "assetId": "f69663f5-f980-4deb-8d02-e6ec27180edd",
      "status": "Asset has been written to custodial copy disk."
    },
    "properties": {
      "executionId": "COURTDOC_TST-2025-C4PD_0",
      "messageType": "preserve.digital.asset.ingest.complete"
    }
  }
}
```
## Deletions Based on Message
* From the assetId, the lambda traverses children and all parents of the children and deletes based on the `location` field
* From the executionId, the lambda deletes all objects from `raw-cache` having path `<executionId>/metadata.json`
* From the executionId, the lambda deletes all objects from `ingest-state` having path `<executionId>/folders.json` and `<executionId>/assets.json`   

## Other Cleanups 
* The importer lambda, for the input sources managed by DR2 (only `ADHOC` and `DRI` at the time of writing), deletes objects from upload buckets once they are copied to `raw-cache`

## Legacy Cleanup 
* Once this process is live, we would want to cleanup any leftovers from the time before this process. To achieve that, we will do a one-off reconciliation using checksums from raw-cache bucket and checksums from the preservation system and empty the `raw-cache` bucket based on the findings

