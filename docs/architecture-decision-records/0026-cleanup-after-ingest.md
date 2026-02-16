# 26. Cleanup After Ingest

**Date:** 2026-02-12

## Context
As part of the ingest process, we save some information within our AWS infrastructure. The main places where such information is stored are S3 buckets 
and DynamoDB tables. Some of this information is useful while the "transaction" is in progress. Once the transaction has completed, i.e. the various final
storage places of those records, such as Preservation System, Custodial Copy, Tape Drive etc. are achieved, we do not want the data left behind within the S3
buckets or any processing information in the DynamoDB tables.

As of now, notification from custodial copy of a successful ingest is the logical end point of ingest, however, with time, there may be more downstream processes 
added to the ingest process and the logical end point will move.

We make use of the AWS supported options to achieve this rather than deleting the items ourselves. To that effect we have one main components added to the process as below

![Existing Process and New Lambda](/docs/images/adr/0026/current-with-added-lambda.png)

The modifications are as follows: 
* There is a new queue which listens to the SNS notifications, this queue triggers the cleanup lambda 
* The lambda updates the item in the files table to set ttl to +1 day from that time
* Tag the objects in S3 with `delete=true` 

The soft delete mentioned above turns into a hard delete through AWS deletions. This is achieved by

* For DynamoDB tables, the value in a specific attribute (`ttl` in files table) is set to be the driver for ttl
```json
ttl {
    attribute_name = "ttl"
    enabled        = true
}
```
* For S3, we have a lifecycle rule to expire items after 90 days where `delete = true`
```json
{
  "Rules": [
    {
      "ID": "DeleteMarkedObjectsAfterNinetyDays",
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

The main driving input for the cleanup lambda is `assetId` abd `executionId`, which can be taken from the message:
```json
{
    "body": {
        "properties": {
            "executionId": "COURTDOC_b6f8299d-2324-4172-ac8f-65effb6808d8_0",
            "messageId": "1b8fb969-3220-454a-969e-e19f093f518a",
            "parentMessageId": null,
            "timestamp": "2026-02-13T10:50:45.757547134Z",
            "messageType": "preserve.digital.asset.ingest.update"
        },
        "parameters": {
            "assetId": "1c69a793-66dc-4794-a841-c21e3841abd2",
            "status": "Asset has been ingested to the Preservation System."
        }
    },
    "timestamp": "1770979846211",
    "topicArn": "arn:aws:sns:eu-west-2:132135060795:prod-dr2-notifications"
}

```

* From the assetId, the lambda traverses children and all parents of the children and deletes based on the `location` field
* From the executionId, the lambda deletes all files from `raw-cache` having path `<executionId>/metadata.json`

* The importer lambda, for the input sources where applicable (ADHOC and DRI), deletes files from upload buckets 

* Cleanup before this process goes live: We will do a one off reconciliation using checksums from raw-cache bucket and checksums from preservica and empty the `raw-cache` bucket based on the findings.

