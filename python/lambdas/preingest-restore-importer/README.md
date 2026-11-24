# DR2 Preingest Import files from restore package

This lambda imports files from a package [restored from Custodial Copy](../../generate-restore-package/README.md)

## Input

The lambda is triggered from an SQS message.

```json
{
   "assetId": "<asset_id>",
   "bucket": "<env>-dr2-ingest-cc-restore-cache",
   "metadataLocation": "s3://<env>-dr2-ingest-cc-restore-cache/<asset_id>.metadata"
}
```

## Output

The lambda doesn't return anything, but it sends a message to `OUTPUT_QUEUE_URL`

```json
{
  "id": "<asset_id>",
  "location": "s3://OUTPUT_BUCKET_NAME/metadata_file_id",
  "filesPrefix": "<asset_id>"
}
```

## Steps

1. Read the `assetId` from the message body
2. Read the `bucket` and from the message body
3. Call `get_object` to retrieve the metadata file using the url from `metadataLocation` in the message body and load the XML.
4. Generate a list of keys by getting the text values from the elements at path `/{*}Representation/{*}ContentObjects/{*}ContentObject`, prefixed with `<asset_id>/`
5. Copy files from the `bucket` to the `OUTPUT_BUCKET_NAME`
6. Delete all files and metadata json from the source bucket.
7. Send the `assetId`, location of the metadata file, and the `assetId` as the files prefix to `OUTPUT_QUEUE_URL`

[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments)

## Environment Variables

| Name                    | Description                                                                                                                   |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| OUTPUT_BUCKET_NAME      | The DR2 bucket to copy the files to                                                                                           |
| OUTPUT_QUEUE_URL        | The SQS queue to send the non-metadata file locations to                                                                      |
