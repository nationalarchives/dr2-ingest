# DR2 Preingest court document package builder

Because of aggregation, assets are grouped and therefore each have group ID in the Ingest lock table.

1. The lambda passes in the group ID it gets from the input and lock table name in a call to DDB
2. It retrieves only the items from the ingest lock table that match the group ID
3. Each lock table item has a `message` attribute which is a JSON string with an id, a file location which points to the metadata from TRE and a file id which is the S3 key for the judgment:
    ```json
    {
      "id": "0c76c5d0-bb8e-4ebb-98c7-5900dc26a1a0",
      "location": "s3://raw-cache-bucket/0c76c5d0-bb8e-4ebb-98c7-5900dc26a1a0",
      "fileId": "acc379db-2dc4-4688-bcb0-7e611e51a150",
      "messageId": "optionalMessageId"
    }
    ```
4. For each lock table message:   
   1. Download from the location in the lock table message and deserialise to a TRE metadata file.  
   2. Retrieve a URI from this file (with this format https://example.com/id/{court}/{year}/{cite}/{optionalDoctype}), extract the court and trim anything after the cite.
   3. Map the `court` (that was extracted) to a series and department reference using a static lookup table.
   4. Generate a single [metadataPackage object](/docs/metadataPackage.md) describing the ingest package.
5. Upload an array containing the JSON objects to this location `{batchId}/metadata.json` in S3.
6. Return an `Output` of `batchId`, `groupId`, location of metadata.json file (`packageMetadata`) and `retryCount`. This will serve as the input to our generic ingest process.

## Lambda input

The lambda receives its input from a step in the preingest step function; it looks like this:

```json
{
  "groupId": "COURTDOC_a21ac6c6-c692-4977-96dd-3b8cc82c7850",
  "batchId": "COURTDOC_a21ac6c6-c692-4977-96dd-3b8cc82c7850_0",
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

[Link to the infrastructure code](/terraform/preingest.tf)

## Environment Variables

| Name                            | Description                                                                                                                             |
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| LOCK_DDB_TABLE                  | The lock table name in Dynamo                                                                                                           |
| LOCK_DDB_TABLE_GROUPID_GSI_NAME | The lock table global secondary index name in Dynamo                                                                                    |
| OUTPUT_BUCKET_NAME              | The bucket the TDR file and metadata file are stored in                                                                                 |
