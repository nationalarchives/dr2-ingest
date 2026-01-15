# DR2 Court Document Preingest Importer

## Input
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

## Output

The lambda doesn't return anything, but it sends a message to `OUTPUT_QUEUE_URL`

```json
{
  "id": "cc3c10fb-f34c-4438-be42-198fc31ab213",
  "fileId": "0744de10-6a5f-4eb8-9213-1bf7f581e45d",
  "location": "s3://DESTINATION_BUCKET/<assetId>.metadata"
}
```

## Steps

The Lambda does the following.

- Receives an input message from TRE via an SQS message.
- Downloads the package from TRE.
- Unzips it.
- Untars it.
- Uploads all files from the package to S3 with a UUID; even though this includes files we don't care about, it's easier
  than
  trying to parse JSON on the fly.
- Deletes the files from TRE that were copied, that are not the file info and metadata file info ones (the "files we
  don't care about"), from the S3 bucket.

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-ingest/tree/main/terraform)

## Environment Variables

| Name               | Description                                                                    |
|--------------------|--------------------------------------------------------------------------------|
| OUTPUT_QUEUE_URL   | The queue to send the SQS message to                                           |
| OUTPUT_BUCKET_NAME | The raw cache bucket for storing the files and metadata created by this lambda |

