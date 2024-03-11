# Ingest Folder Opex Creator

This lambda will create opex files for folders within the opex package

## Input json
The input will come from the step function. 

```json
{
  "batchId": "TDR-2023-RMW",
  "id": "6016a2ce-6581-4e3b-8abc-177a8d008879",
  "executionName": "test-execution"
}

```

There is no output

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

## Environment Variables

| Name              | Description                                                 |
|-------------------|-------------------------------------------------------------|
| DYNAMO_TABLE_NAME | The dynamo table to read the folders and children from      |
| BUCKET_NAME       | The name of the bucket to write the opex files to           |
| DYNAMO_GSI_NAME   | The name of the global secondary index used to query Dynamo |
