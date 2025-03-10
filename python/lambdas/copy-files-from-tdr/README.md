# DR2 Preingest Copy files from TDR to our bucket

## Input

The lambda is triggered from an SQS message.

```json
{
  "bucket": "tdr-export-judgment-intg",
  "fileId": "60d69a88-0859-4a8e-b981-c2cc2b3f9ddc"
}
```

## Output

The lambda doesn't return anything, but it sends a message to `DESTINATION_QUEUE`

```json
{
  "location": "s3://DESTINATION_BUCKET/fileId"
}
```

## Steps

1. Read the `fileId` and `bucket` from the message
2. Call `head_object` on the `s3://bucket/fileId` object and get the file size
3. If the file size is greater than 5GB, copy the file into `DESTINATION_BUCKET` with the same key using a multipart
   copy.
4. If the file size is less than 5GB, copy the file into `DESTINATION_BUCKET` with the same key using a standard copy.
5. Send the new location of the file to `DESTINATION_QUEUE`
6. Repeat steps 2-4 for the `fileId.metadata` file. We don't need to send a message for the metadata file as its
   location can be inferred downstream.

[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments)

## Environment Variables

| Name                  | Description                                              |
|-----------------------|----------------------------------------------------------|
| OUTPUT_BUCKET_NAME    | The DR2 bucket to copy the files to                      |
| OUTPUT_QUEUE_URL      | The SQS queue to send the non-metadata file locations to |
