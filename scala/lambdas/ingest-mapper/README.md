# DR2 Ingest Mapper

This lambda reads a bagit package based on the input to the lambda, parses file metadata and writes this to a DynamoDB table.

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
* Gets a list of series names from the metadata json. Do `series.split(" ").head` to get the department.
* For each series and department pair, get the title and description for department and series from Discovery. This is run through the XSLT in `src/main/resources/transform.xsl` to replace the EAD tags with newlines.
* If the series is `Unknown`, it will create a hierarchy of `Unknown/Court Documents (court unknown)` or `Unknown/Court Documents (court not matched)` depending on the title of the top level folder.
* Creates a ujson Obj with the department and series output and the metadata json. We use a generic `Obj` because we will eventually have to handle fields we don't know about in advance.
* Updates dynamo with the values
* Write the UUIDs for the folders (Content and Archive) and Assets, to separate json files, in an S3 bucket with the paths `<executionName>/folders.json`, `<executionName>/assets.json`, respectively.
* Writes the state data (including information on the json files with the ids) for the next step function step with this format:
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



[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments/blob/main/ingest_mapper.tf)

## Environment Variables

| Name                | Description                                                  |
|---------------------|--------------------------------------------------------------|
| DYNAMO_TABLE_NAME   | The table to write the values to                             |
| INGEST_STATE_BUCKET | The bucket to write the JSON files (containing the UUIDs) to |
