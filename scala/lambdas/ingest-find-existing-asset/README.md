# DR2 Ingest - Find Existing Asset

A Lambda triggered by our `dr2-ingest` Step Function that:

1. Takes an input with a list of `InputItems`, each containing an asset `id` and `batchId`.
2. For Each `InputItem`:
   1. Use the `id` and `batchId` to fetch the asset item from DynamoDB.
   2. An error is thrown if:
      1. Item isn't in DynamoDB (which it should by this point)
      2. Its type is not an Asset
      3. Return assets
3. With all the assets:
   1. Query the `SourceID`s of the Preservation System using the assets' `id`s
   2. For assets that have the entity type of `InformationObject`, update the Dynamo item to add a 'skipIngest' attribute
4. If there are no errors, call `sendTaskSuccess` once for each input item, with the `taskToken` from the input and an object, with the object comprising:
   1. A key of `id`
   2. A key of `batchId`
   3. A key of `assetExists` that has a Boolean value
5. If there are any errors, call `sendTaskFailure` with the error message as the cause.

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
