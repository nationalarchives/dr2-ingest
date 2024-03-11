# DR2 Ingest Mapper

This lambda reads a bagit package based on the input to the lambda, parses file metadata and writes this to a DynamoDB table.

The lambda:
* Reads the input from the step function step with this format:
```json
{
  "batchId": "batch",
  "s3Bucket": "bucket",
  "s3Prefix": "prefix/",
  "department": "department",
  "series": "series"
}
```
* Gets the title and description for department and series from Discovery. This is run through the XSLT in `src/main/resources/transform.xsl` to replace the EAD tags with newlines.
* Parses the metadata json file
* Parses the manifest file
* Converts these into ujson Obj classes. This is because we will eventually have to handle fields we don't know about in advance.
* Updates dynamo with the values
* Writes the state data for the next step function step with this format:
```json
{
  "batchId": "TDR-2023-ABC",
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

| Name              | Description                      |
|-------------------|----------------------------------|
| DYNAMO_TABLE_NAME | The table to write the values to |
