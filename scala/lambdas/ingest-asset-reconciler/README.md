# DR2 Ingest - Asset Reconciler

A Lambda function that queries the Preservation System for the assets and compares these to the ingest source in DynamoDB.

The Lambda:

- Reads the input from the step function step with this format:

```json
{
	"batchId": "batch",
	"executionId": "id",
	"assetId": "asset"
}
```

- Fetches the Asset from our dr2-ingest-files DynamoDB table, using the `assetId` and `batchId` passed as input.
- Queries the Preservation System to find the asset.
- Use the urls to obtain the Content Objects (COs) belonging to each `representationType` from the API
- Get the asset's children from DynamoDB where the child's `parentPath` equals the asset's `parentPath` + `/` + asset `id`.
- Get the bitstream info of the COs from the API using the CO `ref`
- Iterate through each child, using its checksum to find the CO that has the same checksum (fixity) and file title (reconciliation)
- If any file couldn't be reconciled, return a `StateOutput` with:
  - a `wasReconciled` value of `false`
  - a `reason` with info on the file/files that could not be reconciled
  - the `assetId`
  - A `None` value for the `ReconciliationSnsMessage`
- If all files could be reconciled, get the item that corresponds to the `assetName` from the lock table
- Get the attribute `message` from the item; this is a JSON object string with the keys :
  - `messageId`
  - `parentMessageId` (optional)
  - `executionId` (optional)
- Return a `StateOutput` object that contains:
  - a `wasReconciled` value of `true`
  - an empty string for the `reason`
  - the `assetId`
  - A `Some(ReconciliationSnsMessage)`, containing information such as:
    - `reconciliationUpdate`
    - `assetId`
    - `properties`, containing the values:
      - `messageId` (a newly generated `messageId`)
      - `parentMessageId` (the old `messageId`)
      - `executionId`
- Writes the `StateOutput` data for the next step function step as JSON with this format:

```json
{
	"wasReconciled": false,
	"reason": ":alert-noflash-slow: Reconciliation Failure - Out of the 2 files expected to be ingested for assetId 'a8163bde-7daa-43a7-9363-644f93fe2f2b' with `representationType` Preservation, a checksum and title could not be matched with a file on Preservica for:\n1. b285c02d-44e3-4939-a856-66252fd7919a\n2. 974081e5-3123-42ea-923d-3999cc160718"
}
```

## Environment Variables

| Name                                 | Description                                                                         |
| ------------------------------------ | ----------------------------------------------------------------------------------- |
| FILES_DDB_TABLE                      | The name of the table to read assets and their children from                        |
| FILES_DDB_TABLE_BATCHPARENT_GSI_NAME | The name of the global secondary index. This is used for querying fields in the GSI |
| LOCK_DDB_TABLE                       | The name of the lock table to retrieve the message from                             |
| PRESERVICA_API_URL                   | The Preservica API url                                                              |
| PRESERVICA_SECRET_NAME               | The secret used to call the Preservica API                                          |
