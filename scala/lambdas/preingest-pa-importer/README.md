# DR2 Preingest Import files from Parliament

## Input

The lambda is triggered from an SQS message with this body:

```json
{
  "metadataLocation": "s3://tdr-export-intg/60d69a88-0859-4a8e-b981-c2cc2b3f9ddc"
}
```

## Output

The lambda doesn't return anything, but it sends a message to `DESTINATION_QUEUE`

```json
{
  "location": "s3://DESTINATION_BUCKET/<assetId>.metadata"
}
```

## Steps

1. Download the metadata from the location in the input message. This is an array of json objects with each object
   representing a file within a record.
2. For each json object
    1. Validate the metadata against an external schema
    2. Copy the file named `<fileId>` from the `FILES_BUCKET` The lambda assumes the `ROLE_TO_ASSUME` role to do this.
    3. Modify the series and file reference by prefixing it with `Y` and truncating any series which starts with four
       letters to three. e.g. ABCD/12 becomes YABC/12
    4. Upload the modified metadata
    5. Send the id and new location of the file to `DESTINATION_QUEUE`

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-ingest/tree/main/terraform)

## Environment Variables

| Name               | Description                                          |
|--------------------|------------------------------------------------------|
| OUTPUT_BUCKET_NAME | The DR2 bucket to copy the files to                  |
| OUTPUT_QUEUE_URL   | The SQS queue to send the metadata file locations to |
| FILES_BUCKET       | The bucket to copy the files from                    |
| ROLE_TO_ASSUME     | The role to assume to allow the files to be copied   |
