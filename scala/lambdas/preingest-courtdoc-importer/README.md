# DR2 Court Document Preingest Importer

## Input
The input to the Lambda is an SQS event, with a body like

```json
{
  "properties": {
    "messageType": "uk.gov.nationalarchives.tre.messages.courtdocument.parse.uk.gov.nationalarchives.da.messages.courtdocumentpackage.available.CourtDocumentPackageAvailable",
    "function": "tre-tf-module-parse-judgment",
    "producer": "TRE",
    "executionId": "2403e328-f6af-4246-b4ef-970e8fb32f5c",
    "parentExecutionId": "1f8dc4dc-a39f-4a37-afa1-86f0a216699a",
    "timestamp": "2026-07-16T15:27:37.750Z"
  },
  "parameters": {
    "reference": "TDR-2026-DLD5",
    "s3FolderName": "TDR-2026-DLD5/15eff63e-26ac-41c9-97a0-dd0e88b490ee/TDR-2026-DLD5",
    "originator": "TDR",
    "s3Bucket": "prod-tre-common-data",
    "status": "COURT_DOCUMENT_PARSE_NO_ERRORS"
  }
}
```
For the purpose of import, we are only interested in the `s3FolderName` and `s3Bucket` parameters, which are used to download the package from TRE.

## Output

The lambda doesn't return anything, but it sends a message to `OUTPUT_QUEUE_URL`

```json
{
  "id": "cc3c10fb-f34c-4438-be42-198fc31ab213",
  "fileId": "0744de10-6a5f-4eb8-9213-1bf7f581e45d",
  "location": "s3://DESTINATION_BUCKET/<assetId>"
}
```

## Steps

The Lambda does the following.

- Receives an input message from TRE via an SQS message.
- Reads the `s3FolderName` and `s3Bucket` parameters from the message.
- Reads the metadata file in the `s3Bucket` at `s3FolderName/out/TRE-<bathId>-metadata.json` and reads the `fileName` from payload section.
- Generates a random UUID to be used as fileName at the destination bucket.
- Copies the file identified by `fileName` from `s3Bucket` at `s3FolderName/out/data<fileName>` to the `OUTPUT_BUCKET_NAME` at `<generated uuid>`.
- Generates a random UUID to be used as a name for the metadata file at the destination bucket.
- Copies the metadata file from `s3Bucket` at `s3FolderName/out/TRE-<bathId>-metadata.json` to the `OUTPUT_BUCKET_NAME` at `<generated uuid>.metadata`.
- Sends a message to `OUTPUT_QUEUE_URL` as mentioned in the Output section above.


[Link to the infrastructure code](https://github.com/nationalarchives/dr2-ingest/tree/main/terraform)

## Environment Variables

| Name               | Description                                                                    |
|--------------------|--------------------------------------------------------------------------------|
| OUTPUT_QUEUE_URL   | The queue to send the SQS message to                                           |
| OUTPUT_BUCKET_NAME | The raw cache bucket for storing the files and metadata created by this lambda |

