# DR2 Ingest Folder Opex Creator

This lambda will be responsible for creating Preservica Asset Exchange (PAX) packages and accompanying OPEX for assets within our ingest package. 
The Lambda will be run in a Map state within our dr2-ingest Step Function, with the event containing the UUID of an asset to be created.

The lambda will:
* Fetch the asset from Dynamo using the id passed as input.
* Get the child files from Dynamo where the child's parent path equals the asset path.
* Create a XIP xml file and uploads it to S3
* Create an OPEX xml file and uploads it to S3.

## Example
Given the following input:
```json
{
  "id": "68b1c80b-36b8-4f0f-94d6-92589002d87e",
  "batchId": "test-batch-id",
  "executionName": "test-execution-name",
  "sourceBucket": "source-bucket"
}
```

Given an asset:
```json
{
  "id":"68b1c80b-36b8-4f0f-94d6-92589002d87e",
  "parentPath": "6016a2ce-6581-4e3b-8abc-177a8d008879/63864ef3-1ab7-4556-a4f1-0a62849e05a7/66cd14be-4e19-4d9c-bd3e-a735508ee935"
}
```
And two children:
```json
[
  {
    "id":"feedd76d-e368-45c8-96e3-c37671476793",
    "parentPath": "6016a2ce-6581-4e3b-8abc-177a8d008879/63864ef3-1ab7-4556-a4f1-0a62849e05a7/66cd14be-4e19-4d9c-bd3e-a735508ee935/68b1c80b-36b8-4f0f-94d6-92589002d87e",
    "ext":"json"
  },
  {
    "id":"c2919517-2e47-472e-967b-e6a8bd0807cd",
    "parentPath": "6016a2ce-6581-4e3b-8abc-177a8d008879/63864ef3-1ab7-4556-a4f1-0a62849e05a7/66cd14be-4e19-4d9c-bd3e-a735508ee935/68b1c80b-36b8-4f0f-94d6-92589002d87e",
    "ext":"docx"
  }
]
```

This will produce the following structure in S3.
```text
opex
└── test-execution
    └── 66cd14be-4e19-4d9c-bd3e-a735508ee935
        └── 63864ef3-1ab7-4556-a4f1-0a62849e05a7
            └── 6016a2ce-6581-4e3b-8abc-177a8d008879
                ├── 68b1c80b-36b8-4f0f-94d6-92589002d87e.pax
                │   ├── 68b1c80b-36b8-4f0f-94d6-92589002d87e.xip
                │   └── Representation_Preservation
                │       ├── c2919517-2e47-472e-967b-e6a8bd0807cd
                │       │   └── Generation_1
                │       │       └── c2919517-2e47-472e-967b-e6a8bd0807cd.docx
                │       └── feedd76d-e368-45c8-96e3-c37671476793
                │           └── Generation_1
                │               └── feedd76d-e368-45c8-96e3-c37671476793.json
                └── 68b1c80b-36b8-4f0f-94d6-92589002d87e.pax.opex

```

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

## Environment Variables

| Name               | Description                                                                         |
|--------------------|-------------------------------------------------------------------------------------|
| DYNAMO_TABLE_NAME  | The name of the table to read assets and their children from                        |
| SOURCE_BUCKET      | The raw bucket to copy the files from                                               |
| DESTINATION_BUCKET | The staging bucket to copy the files to                                             |
| DYNAMO_GSI_NAME    | The name of the global secondary index. This is used for querying fields in the GSI |
