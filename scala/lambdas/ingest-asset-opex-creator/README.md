# DR2 Ingest - Asset Opex Creator

This Lambda is responsible for creating Preservica Asset Exchange (PAX) packages and accompanying OPEX for assets within our ingest package. 

The Lambda is run within a Map state in our `dr2-ingest` Step Function, with the event containing the UUID of an asset to be created.

The Lambda
* Fetches the asset from DynamoDB using the `id` passed as input.
* Queries DynamoDB for child files of the asset - where the child's `parentPath` is equal to this asset's `parentPath+id`.
* Verifies the GSI was fully populated by comparing `childCount` with the number of files found, erroring if not.
* Creates the required OPEX components to represent this asset within the batch.
  * Creates a XIP XML file and uploads it to S3
  * Create an OPEX XML file and uploads it to S3.
  * Copies and renames the digital file objects from source to `$OUTPUT_BUCKET_NAME`.

## Example
Given the input

```json
{
    "batchId": "AAA_95b8f7b6-0b70-45ee-a76e-532dc36318c2_0",
    "executionName": "AnExecution",
    "id": "68b1c80b-36b8-4f0f-94d6-92589002d87e"
}
```
it will fetch the asset from DyamoDB

```json
{
  "id":"68b1c80b-36b8-4f0f-94d6-92589002d87e",
  "batchId": "AAA_95b8f7b6-0b70-45ee-a76e-532dc36318c2_0",
  "parentPath": "6016a2ce-6581-4e3b-8abc-177a8d008879/63864ef3-1ab7-4556-a4f1-0a62849e05a7/66cd14be-4e19-4d9c-bd3e-a735508ee935",
  "childCount": 2,
  ...
}
```
and query for the two children.
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

It will produce the following structure in the destination S3 bucket.
```text
opex
└── test-execution
    └── 6016a2ce-6581-4e3b-8abc-177a8d008879
        └── 63864ef3-1ab7-4556-a4f1-0a62849e05a7
            └──  66cd14be-4e19-4d9c-bd3e-a735508ee935
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

## Environment Variables

| Name                                    | Description                                                                         |
|-----------------------------------------|-------------------------------------------------------------------------------------|
| FILES_DDB_TABLE                         | The name of the table to read assets and their children from                        |
| FILES_DDB_TABLE_BATCHPARENT_GSI_NAME    | The name of the global secondary index. This is used for querying fields in the GSI |
| OUTPUT_BUCKET_NAME                      | The staging bucket to copy the files to                                             |
