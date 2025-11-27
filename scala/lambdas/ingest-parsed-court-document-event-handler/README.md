# DR2 Court Document Preingest - Parsed Court Document Event Handler

The Lambda does the following.

- Receives an input message from TRE via an SQS message.
- Downloads the package from TRE.
- Unzips it.
- Untars it.
- Uploads all files from the package to S3 with a UUID; even though this includes files we don't care about, it's easier than
  trying to parse JSON on the fly.
- Parses the TRE metadata JSON file ("TRE-{batchRef}-metadata.json") from the package.
- It retrieves a URI from this file (with this format https://example.com/id/{court}/{year}/{cite}/{optionalDoctype}) and extracts the court and trims of anything after the cite.
- Maps the `court` (that was extracted) to a series and department reference using a static lookup table.
- Generates a single [metadataPackage JSON file](/docs/metadataPackage.md) describing the ingest package.
- Deletes the files from TRE that were copied, that are not the file info and metadata file info ones (the "files we don't care about"), from the S3 bucket.
- Writes the `assetId`, `groupId` and `message`, containing a json String with the format {"messageId":"{randomId}"} to the lock table in DynamoDB only if it doesn't already exist.
- Starts a Step Function execution for the ingest package.

The department and series lookup is very judgment-specific but this can be changed if we start taking in other
transfers.

## Metadata Mapping

This example shows how we map the metadata from TRE to our metadataPackage JSON file.  
Each field in a row is tried, if it's not null, it's used, otherwise the next field is tried.

The input to the Lambda is an SQS event, with a body like

```json
{
  "properties": {
    "messageId": "26f9dc44-b607-4e25-b750-1db67de8046a"
  },
  "parameters": {
    "status": "status",
    "reference": "TEST-REFERENCE",
    "s3Bucket": "inputBucket",
    "s3Key": "test.tar.gz"
  }
}
```

We can also replay the message with the `skipSeriesLookup` parameter

```json
{
  "properties": {
    "messageId": "26f9dc44-b607-4e25-b750-1db67de8046a"
  },
  "parameters": {
    "status": "status",
    "reference": "TEST-REFERENCE",
    "skipSeriesLookup": true,
    "s3Bucket": "inputBucket",
    "s3Key": "test.tar.gz"
  }
}
```

The lambda doesn't return anything, it writes objects to S3 and starts a Step Function execution.

#### metadataPackage JSON file

```json
[
  {
    "series": "ABC 123",
    "id_Code": "cite",
    "id_Cite": "cite",
    "id_URI": "https://example.com/id/court/2023/",
    "id": "4e6bac50-d80a-4c68-bd92-772ac9701f14",
    "parentId": null,
    "title": "test",
    "type": "ArchiveFolder",
    "name": "https://example.com/id/court/2023/"
  },
  {
    "originalMetadataFiles": ["61ac0166-ccdf-48c4-800f-29e5fba2efda"],
    "description": "test",
    "id_ConsignmentReference": "test-identifier",
    "id_UpstreamSystemReference": "TEST-REFERENCE",
    "transferringBody": "test-organisation",
    "transferCompleteDatetime": "2023-10-31T13:40:54Z",
    "upstreamSystem": "TRE: FCL Parser workflow",
    "digitalAssetSource": "Born Digital",
    "digitalAssetSubtype": "FCL",
    "id_URI": "https://example.com/id/court/2023/",
    "id_NeutralCitation": "cite",
    "id": "c2e7866e-5e94-4b4e-a49f-043ad937c18a",
    "parentId": "4e6bac50-d80a-4c68-bd92-772ac9701f14",
    "title": "Test.docx",
    "type": "Asset",
    "name": "Test.docx",
    "correlationId": "26f9dc44-b607-4e25-b750-1db67de8046a"
  },
  {
    "id": "c7e6b27f-5778-4da8-9b83-1b64bbccbd03",
    "parentId": "c2e7866e-5e94-4b4e-a49f-043ad937c18a",
    "title": "Test",
    "type": "File",
    "name": "Test.docx",
    "sortOrder": 1,
    "location": "s3://raw-cache-bucket/53e7e334-a0bb-4dd2-ac26-0e428db56982",
    "fileSize": 15684
  },
  {
    "id": "61ac0166-ccdf-48c4-800f-29e5fba2efda",
    "parentId": "c2e7866e-5e94-4b4e-a49f-043ad937c18a",
    "title": "",
    "type": "File",
    "name": "TRE-TEST-REFERENCE-metadata.json",
    "location": "s3://raw-cache-bucket/96a07aa3-c4c5-40b2-b546-c51d2f24dce3",
    "sortOrder": 2,
    "fileSize": 215
  }
]
```

If `skipSeriesLookup` is sent and the series can't be found, the value of `series` in the top level `ArchiveFolder` will be `Unknown`

## Environment Variables

| Name               | Description                                                                    |
| ------------------ | ------------------------------------------------------------------------------ |
| INGEST_SFN_ARN     | The arn of the step function this lambda will trigger.                         |
| LOCK_DDB_TABLE     |                                                                                |
| OUTPUT_BUCKET_NAME | The raw cache bucket for storing the files and metadata created by this lambda |
