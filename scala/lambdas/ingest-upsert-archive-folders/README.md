# DR2 Ingest Upsert Archive Folders

A Lambda that retrieves folders from a DB and creates/updates Entities based on that information

## Lambda input
The input to this lambda is provided by the step function.

```json
{
  "batchId": "TDR-2023-ABC",
  "archiveHierarchyFolders": [
      "f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176",
      "e88e433a-1f3e-48c5-b15f-234c0e663c27",
      "93f5a200-9ee7-423d-827c-aad823182ad2"
  ],
  "contentFolders": [],
  "contentAssets": [
      "a8163bde-7daa-43a7-9363-644f93fe2f2b"
  ]
}
```

## Lambda steps
The lambda carries out the following steps.

* Get the folder rows for a batch ID and sort by parent path with the shortest first.
* Get existing Preservica entities with a SourceID matching any row name and create a `Map[String, Entity]`
* Use `foldLeft` with a `Map[UUID, UUID]` on the folder rows. For each row:
    * Check the SourceId to entity map to see if the entity already exists.
    * If it does, check the entity type, parent ref and security tag and raise errors if they are invalid and add folderId -> entityRef to the `Map[UUID, UUID]`
    * If it doesn't, get the parent ref from the `Map[UUID, UUID]` This will be there because by sorting the parent paths shortest first, we guarantee to create entities higher in the tree first.
    * Use this to create the entity in Preservica
    * Add folderId -> entityRef to the `Map[UUID, UUID]`
* Calculate any updated or added entities and send the Slack message out if any are found.
There is no output from this Lambda.

[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments)

## Environment Variables

| Name                      | Description                                    |
|---------------------------|------------------------------------------------|
| PRESERVICA_API_URL        | The Preservica API  url                        |
| PRESERVICA_SECRET_NAME    | The secret used to call the Preservica API     |
| ARCHIVE_FOLDER_TABLE_NAME | The name of the table to get folders from      |

