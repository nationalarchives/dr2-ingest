# DR2 Preingest package builder

This code is used for both the TDR and DRI migration preingest.

Because of aggregation, assets are grouped and therefore each have group ID in the Ingest lock table.

1. The lambda passes in the group ID it gets from the input and lock table name in a call to DDB
2. It retrieves only the items from the ingest lock table that match the group ID
3. Each lock table item has a `message` attribute which is a JSON string with an id and a file location like:
    ```json
    {
      "id": "0c76c5d0-bb8e-4ebb-98c7-5900dc26a1a0",
      "location": "s3://raw-cache-bucket/0c76c5d0-bb8e-4ebb-98c7-5900dc26a1a0",
      "messageId": "optionalMessageId"
    }
    ```
4. Since each asset has an associated metadata file in the same location as the file (with this format `<uuid>.metadata`),
   for each asset (in parallel):
   1. get the url from the `location` field and use it to download the metadata file (from S3)
      *  an example of some of the fields a `<uuid>.metadata` file might contain
           ```json
           {
              "Series":"TEST 123",
              "UUID":"0000b033-a309-4c42-8397-ba7854e345e2",
              "description":null,
              "TransferringBody":"TestBody",
              "TransferInitiatedDatetime":"2024-10-07 09:54:48",
              "ConsignmentReference":"REF-2024-XJKD",
              "Filename":"0000b033-a309-4c42-8397-ba7854e345e2.txt",
              "SHA256ServerSideChecksum":"33e806a1d38ec24fec78fd41f5ea3f54a300f9e1652e6d0b0b1eeca8e25d27ed",
              "FileReference":"Z1BA8C",
              "ClientSideOriginalFilepath":"/path/to/file"
           }
           ```
   2. convert the byte array returned into a string
   3. decode this string into a list of `PackageMetadata` objects
   4. use the `PackageMetadata` to (in parallel):
      1. generate:
         1. a `FileMetadataObject` with information on the file
            * file size and S3 key is obtained from the file in S3
         2. an `AssetMetadataObject` (its parent)
         3. a `ContentFolderObject` (the Asset's parent), if the ContentFolder isn't already in the ContentFolder cache
            and then update the cache with the newly created object
      2. generate a `FileMetadataObject` with information on the `<uuid>.metadata` metadata file (in S3)
   5. combine and return these as a list of `MetadataObjects`
   6. (if the list of objects is empty, throw an error, otherwise) convert the list of `MetadataObjects` to a JSON file
   7. upload the JSON file at this location `{batchId}/metadata.json` in S3.
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

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

## Environment Variables

| Name                            | Description                                                                                                                             |
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| LOCK_DDB_TABLE                  | The lock table name in Dynamo                                                                                                           |
| LOCK_DDB_TABLE_GROUPID_GSI_NAME | The lock table global secondary index name in Dynamo                                                                                    |
| OUTPUT_BUCKET_NAME              | The bucket the TDR file and metadata file are stored in                                                                                 |
| CONCURRENCY                     | An optional parameter which configures how many items the lambda processes in parallel. Used for performance tweaking. Defaults to 1000 |
| SOURCE_SYSTEM                   | TDR or DRI                                                                                                                              |
