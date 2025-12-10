# DR2 Ingest - Find Existing Asset

A Lambda triggered by our `dr2-ingest` Step Function that:

1. Takes an input with a list of `InputItems`, each containing an asset `id` and `batchId`.
1. For Each `InputItem`:
   1. Use the `id` and `batchId` to fetch the asset item from DynamoDB.
   1. An error is thrown if:
      1. Item isn't in DynamoDB (which it should by this point)
      2. Its type is not an Asset
      3. Return assets
1. With all the assets:
   1. Query the `SourceID`s of the Preservation System using the assets' `id`s
   1. For assets that have the entity type of `InformationObject`, update the Dynamo item to add a 'skipIngest' attribute
1. Returns an array of JSON objects (representing each asset), each object comprising:
   1. A key of `id`
   1. A key of `batchId`
   1. A key of `assetExists` that has a Boolean value

## Lambda input

The Lambda takes the following input:

```json
{
	"batchId": "test-batch-id",
	"id": "test-asset-id"
}
```

## Lambda output

The Lambda outputs a JSON object

```json
{
    "id": "test-asset-id",
    "batchId": "test-batch-id",
    "assetExists": "[Boolean]"
}
```

## Environment Variables

| Name                   | Description                                                  |
| ---------------------- | ------------------------------------------------------------ |
| FILES_DDB_TABLE        | The name of the table to read assets and their children from |
| PRESERVICA_API_URL     | The Preservica API url                                       |
| PRESERVICA_SECRET_NAME | The secret used to call the Preservica API                   |
