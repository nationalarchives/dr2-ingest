# DR2 Ingest - Folder Opex Creator

This Lambda creates OPEX XML files for folders within the ingest package.

The Lambda is run within a Map state in our `dr2-ingest` Step Function, with the event containing the `id` of a folder to be created.

## Input
```json
{
  "batchId": "TDR-2023-RMW",
  "id": "6016a2ce-6581-4e3b-8abc-177a8d008879",
  "executionName": "test-execution"
}
```

There is no output from this Lambda.

## Environment Variables

| Name                                 | Description                                                 |
|--------------------------------------|-------------------------------------------------------------|
| FILES_DDB_TABLE                      | The dynamo table to read the folders and children from      |
| FILES_DDB_TABLE_BATCHPARENT_GSI_NAME | The name of the global secondary index used to query Dynamo |
| OUTPUT_BUCKET_NAME                   | The name of the bucket to write the opex files to.          |
