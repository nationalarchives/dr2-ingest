# DR2 Ingest - Mapper

This Lambda reads a JSON file passed in as the input to the Lambda, parses file contents and writes this to a DynamoDB
table.

The lambda:

* Reads the input from the step function step with this format:

```json
{
  "batchId": "batch",
  "metadataPackage": "s3://metadata-bucket/metadata.json",
  "executionName": "executionName"
}
```

* Downloads the metadata file from the `metadataPackage` location and parses it.
* Gets a list of series names from the metadata json. Extracts the department reference from the series reference by
  doing `series.split(" ").head.
* For each unique series and department pair, gets the title and description from Discovery. This is run through the
  XSLT in `src/main/resources/transform.xsl` to replace the EAD tags with newlines. If discovery is unavailable, or the
  series doesn't yet exist, the title and description are not added to the table.
* If the series is `Unknown`, it will create a hierarchy of `Unknown/`.
* Creates a ujson Obj with the department and series output and the metadata json. We use a generic `Obj` because we
  will eventually have to handle fields we don't know about in advance.
* Generates the `parentPath` of each entity, replacing the `parentId`.
* Calculates the `childCount` of each entity.
* Updates dynamo with the values.
* A ttl is set on the rows. It is set to ${TTL_DAYS} days in the future.
* Write the UUIDs for the folders (Content and Archive) and Assets, to separate json files, in an S3 bucket with the
  paths `<executionName>/folders.json`, `<executionName>/assets.json`, respectively.
* Writes the state data (including information on the json files with the ids) for the next step function step with this
  format:

```json
{
  "batchId": "TDR-2023-ABC",
  "metadataPackage": "s3://metadata-bucket/metadata.json",
  "assets": {
    "bucket": "stateBucketName",
    "key": "<executionName>/assets.json"
  },
  "folders": {
    "bucket": "stateBucketName",
    "key": "<executionName>/folders.json"
  },
  "archiveHierarchyFolders": [
    "f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176",
    "e88e433a-1f3e-48c5-b15f-234c0e663c27",
    "93f5a200-9ee7-423d-827c-aad823182ad2"
  ],
  "contentFolders": [],
  "contentAssets": [
    "a8163bde-7daa-43a7-9363-644f93fe2f2b"
  ]
}
```

## Environment Variables

| Name               | Description                                                  |
|--------------------|--------------------------------------------------------------|
| FILES_DDB_TABLE    | The table to write the values to                             |
| OUTPUT_BUCKET_NAME | The bucket to write the JSON files (containing the UUIDs) to |
| TTL_DAYS           | The number of days in the future to set the TTL on the rows  |
