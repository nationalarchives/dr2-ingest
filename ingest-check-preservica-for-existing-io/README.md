# DR2 Check Preservica For Existing Information Object

A Lambda triggered by our dr2-ingest Step Function that:

1. Takes an Asset ID as an input
2. Queries Dynamo for the row belong to the ID
3. Gets the Asset name from row
4. Queries Preservica in order to check if an Information Object with the same SourceID already exists
5. If Information Object Exists:
   * Send an update to the Dynamo row to add a 'skipIngest' attribute
6. Returns a `StateOutput` with a key of `assetExists` that has a Boolean value

## Lambda input
The lambda takes the following input:

```json
{
  "batchId": "test-batch-id",
  "assetId": "test-asset-id"
}
```

## Lambda output
The lambda outputs the Ingest assetExists for the purpose of causing/preventing and ingest of the asset
```json
{
  "assetExists": "[Boolean]"
}
```

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

## Environment Variables

| Name                   | Description                                                  |
|------------------------|--------------------------------------------------------------|
| PRESERVICA_API_URL     | The Preservica API  url                                      |
| PRESERVICA_SECRET_NAME | The secret used to call the Preservica API                   |
| DYNAMO_TABLE_NAME      | The name of the table to read assets and their children from |

