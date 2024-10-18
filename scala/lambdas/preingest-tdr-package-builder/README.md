# DR2 Preingest TDR package builder

The lambda reads rows from the ingest lock table for the group ID provided as input.

Each lock table row has a message with an id and a file location

```json
{
  "id": "0c76c5d0-bb8e-4ebb-98c7-5900dc26a1a0",
  "location": "s3://raw-cache-bucket/0c76c5d0-bb8e-4ebb-98c7-5900dc26a1a0",
  "messageId": "optionalMessageId"
}
```

Each file from TDR has an associated metadata file at `<uuid>.metadata`
We download this file, parse the metadata and use this to construct our metadata.json file which is the input to our
generic ingest process.

## Lambda input

The lambda receives its input from a step function step in the preingest step function

```json
{
  "groupId": "TDR_a21ac6c6-c692-4977-96dd-3b8cc82c7850",
  "batchId": "TDR_a21ac6c6-c692-4977-96dd-3b8cc82c7850_0",
  "waitFor": 352,
  "retryCount": 0
}
```

## Lambda output

The lambda outputs this json.

```json
{
  "groupId": "TST_2b1d1ab5-e4b6-4c18-bf45-0abe1f248dd6",
  "batchId": "TST_2b1d1ab5-e4b6-4c18-bf45-0abe1f248dd6_0",
  "packageMetadata": "s3://cache-bucket/TST_2b1d1ab5-e4b6-4c18-bf45-0abe1f248dd6_0/metadata.json",
  "retryCount": 0
}
```

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

## Environment Variables

| Name                        | Description                                                                                                                             |
|-----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| DDB_LOCK_TABLE              | The lock table name in Dynamo                                                                                                           |
| LOCK_DDB_TABLE_GROUP_ID_IDX | The lock table global secondary index name in Dynamo                                                                                    |
| RAW_CACHE_BUCKET            | The bucket the TDR file and metadata file are stored in                                                                                 |
| CONCURRENCY                 | An optional parameter which configures how many items the lambda processes in parallel. Used for performance tweaking. Defaults to 1000 |
