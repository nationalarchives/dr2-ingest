# DR2 Preingest Import files from TDR

## Input

The lambda is triggered from an SQS message.

```json
{
  "bucket": "tdr-export-judgment-intg",
  "fileId": "60d69a88-0859-4a8e-b981-c2cc2b3f9ddc"
}
```

## Output

The lambda doesn't return anything, but it sends a message to `OUTPUT_QUEUE_URL`

```json
{
  "location": "s3://OUTPUT_BUCKET_NAME/metadata_file_id"
}
```

## Steps

1. Read the `assetId` (if it's there, else `fileId`) from the message body and save it as `assetId`
1. Read the `bucket` and from the message body
1. Call `head_object` on the `s3://bucket/assetId` key (which contains all objects related to the asset), then:
   1. Check if a `"{asset_id}.metadata"` file exists, if not, throw an exception
   1. Check if there are any file objects, if there aren't any, throw an exception
   1. Return the files
1. Call `get_object` to retrieve the metadata file at the `s3://bucket/{asset_id}.metadata` key, then
   1. Convert metadata to a JSON
   1. Confirm that mandatory fields exist
   1. Confirm that UUID is in the correct format
   1. Confirm that Series exists
   1. Confirm that Series is in the correct format
1. Copy files from the `bucket` to the `OUTPUT_BUCKET_NAME`
1. Send the `assetId`, location of the metadata file and `messageId` (if there is one) to `OUTPUT_QUEUE_URL`

[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments)

## Environment Variables

| Name                  | Description                                              |
|-----------------------|----------------------------------------------------------|
| OUTPUT_BUCKET_NAME    | The DR2 bucket to copy the files to                      |
| OUTPUT_QUEUE_URL      | The SQS queue to send the non-metadata file locations to |
