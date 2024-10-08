# DR2 Ingest - Upsert Archive Folders

A Lambda that retrieves folder items from DynamoDB and creates/updates entities in the Preservation System based on that information.

## Lambda input

The input to this lambda is provided by the Step Function.

```json
{
	"batchId": "TDR-2023-ABC",
	"archiveHierarchyFolders": [
		"f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176",
		"e88e433a-1f3e-48c5-b15f-234c0e663c27",
		"93f5a200-9ee7-423d-827c-aad823182ad2"
	],
	"contentFolders": [],
	"contentAssets": ["a8163bde-7daa-43a7-9363-644f93fe2f2b"]
}
```

## Lambda steps

The Lambda

- Retrieves each item in the `archiveHierarchyFolders` list from DynamoDB.
- Sorts the list of retrieved items by `parentPath`, with the shortest path first.
- Gets existing Preservation System entities where the `SourceID` identifier matches the `id` from DynamoDB and creates a `Map[String, Entity]`.
- Uses `foldLeft` with a `Map[UUID, UUID]` on the folder items. For each item:
  - Check the SourceId to entity map to see if the entity already exists.
  - If it does, check the entity type, parent ref and security tag and raise errors if they are invalid. Updates the descriptive metadata if required, setting the title from `title` or `id`. And adds folderId -> entityRef to the `Map[UUID, UUID]`.
  - If it doesn't, get the parent ref from the `Map[UUID, UUID]` This will be there because by sorting the parent paths shortest first, we guarantee to create entities higher in the tree first.
  - Use this to create the entity in the Preservation System.
  - Add folderId -> entityRef to the `Map[UUID, UUID]`
- Calculates any updated entities and sends a message to Slack if any are found.

There is no output from this Lambda.

## Environment Variables

| Name                      | Description                                |
| ------------------------- | ------------------------------------------ |
| PRESERVICA_API_URL        | The Preservica API url                     |
| PRESERVICA_SECRET_NAME    | The secret used to call the Preservica API |
| ARCHIVE_FOLDER_TABLE_NAME | The name of the table to get folders from  |
