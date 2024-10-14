# DR2 Ingest - Find Existing Asset

A Lambda triggered by our `dr2-ingest` Step Function that:

1. Takes an asset `id` and `batchId` as an input.
1. Fetches the asset item from DynamoDB.
1. Queries the Preservation System using the asset's `id`
1. If Information Object is returned, updates the Dynamo item to add a 'skipIngest' attribute
1. Returns a JSON object with a key of `assetExists` that has a Boolean value

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
	"assetExists": "[Boolean]"
}
```

## Environment Variables

| Name                   | Description                                                  |
| ---------------------- | ------------------------------------------------------------ |
| DYNAMO_TABLE_NAME      | The name of the table to read assets and their children from |
| PRESERVICA_API_URL     | The Preservica API url                                       |
| PRESERVICA_SECRET_NAME | The secret used to call the Preservica API                   |
