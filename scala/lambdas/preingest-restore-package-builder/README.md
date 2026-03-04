# DR2 CC Restore Preingest package builder

This code is used for the Custodial Copy restore preingest.

Because of aggregation, assets are grouped and therefore each have a group ID in the Ingest lock table.

1. The lambda passes in the group ID it gets from the input and lock table name in a call to DDB
2. It retrieves only the items from the ingest lock table that match the group ID
3. Each lock table item has a `message` attribute which is a JSON string with an id and a metadata file location like:
    ```json
    {
      "id": "0c76c5d0-bb8e-4ebb-98c7-5900dc26a1a0",
      "location": "s3://raw-cache-bucket/0c76c5d0-bb8e-4ebb-98c7-5900dc26a1a0.metadata"
    }
    ```
4. For each item:
   1. get the url from the `location` field and use it to download the metadata file (from S3). The metadata is XML from Custodial Copy.
   2. convert the byte array returned into a string
   3. decode this string into a scala XML element
   4. use element to:
      1. generate:
         1. a `FileMetadataObject` with information on the file
         2. an `AssetMetadataObject` (its parent)
         3. a `ContentFolderObject` (the Asset's parent). This is always "Restored records"
   5. If the metadata list is not empty, upload the JSON file at this location `{batchId}/metadata.json` in S3.
       1. Return an `Output` of `batchId`, `groupId`, location of metadata.json file (`packageMetadata`) and `retryCount`
       2. This output will serve as the input to our generic ingest process.

## Lambda input

The lambda receives its input from a step in the preingest step function; it looks like this:

```json
{
  "groupId": "TDR_a21ac6c6-c692-4977-96dd-3b8cc82c7850",
  "batchId": "TDR_a21ac6c6-c692-4977-96dd-3b8cc82c7850_0",
  "waitFor": 352,
  "retryCount": 0
}
```

## Lambda output

The lambda outputs a JSON like this

```json
{
  "batchId": "TST_2b1d1ab5-e4b6-4c18-bf45-0abe1f248dd6_0",
  "groupId": "TST_2b1d1ab5-e4b6-4c18-bf45-0abe1f248dd6",
  "packageMetadata": "s3://cache-bucket/TST_2b1d1ab5-e4b6-4c18-bf45-0abe1f248dd6_0/metadata.json",
  "retryCount": 0
}
```

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-ingest/tree/main/terraform)

## Environment Variables

| Name                            | Description                                                                                                                             |
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| LOCK_DDB_TABLE                  | The lock table name in Dynamo                                                                                                           |
| LOCK_DDB_TABLE_GROUPID_GSI_NAME | The lock table global secondary index name in Dynamo                                                                                    |
| OUTPUT_BUCKET_NAME              | The bucket the TDR file and metadata file are stored in                                                                                 |
