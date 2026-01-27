# DR2 Ingest - Asset Reconciler

A Lambda function that queries the Preservation System for the assets and compares these to the ingest source in DynamoDB.

The Lambda:

- Reads the input from the step function step with this format:

```json
{
	"batchId": "872c839d-eb4c-4b96-ab73-54de2f81bbef",
	"executionId": "id",
	"assetId": "97c9fb03-e65d-488a-8af4-cb102c96cff1"
}
```

1. Fetches the Asset from our dr2-ingest-files DynamoDB table, using the `assetId` and `batchId` passed as input.
    1. If no asset with the that `assetId` was found then throw an error
    2. If asset type is not `Asset` then throw an error
2. Queries the Preservation System to find the asset.
   1. If more than one entity was returned the throw an error
3. If no entity was returned from the Preservation System:
   1. return a `StateOutput` with:
      1. a `wasReconciled` value of `false`
      2. a List of `Failures` with the `NoEntityFoundWithSourceId` given as the reason
      3. the `assetId`
      4. a `None` value for the IO's ref (since no IO was found)
      5. otherwise, continue with the steps below
4. Get the asset's children from DynamoDB where the child's `parentPath` equals the asset's `parentPath` + `/` + asset `id`.
   1. If the number of children return doesn't match the child count attribute on the Asset, throw an error
   2. If no children were found then throw an error
5. Group the children by their representation type (Preservation or Access).
6. Get the urls of each Content Object (CO) belonging to each `representationType` from the API.
7. Use the urls to obtain the COs:
   1. If no COs were returned, return a `StateOutput` with:
      1. a `wasReconciled` value of `false`
      2. a List of `Failures` with the `NoContentObjects` given as the reason
      3. the `assetId`
      4. the IO's ref
   2. otherwise, continue with the steps below
8. Get the bitstream info of the COs from the API using the CO `ref`.
9. Iterate through each child, using its checksum to find the CO in the DynamoDB table that has the same checksum (fixity)
   and file title (AKA reconciliation).
10. If all files could be reconciled, return a `StateOutput` with:
    1. a `wasReconciled` value of `true`
    2. an empty List of `Failures`
    3. the `assetId`
    4. the IO's ref
11. If any file couldn't be reconciled, return a `StateOutput` with:
    1. a `wasReconciled` value of `false`
    2. a List of `Failures` with the `TitleChecksumMismatch` given as the reason and the children that could not be reconciled
    3. the `assetId`
    4. the IO's ref
12. Writes the `StateOutput` data for the next step function step as JSON with this format:

    ```json
    {
        "wasReconciled": false,
        "failures": [
          {
            "failureReason": "TitleChecksumMismatch",
            "childIds": ["c1ff317e-e83e-4163-9bbb-0101ed959e68", "790eb3bc-b4af-4913-93c0-503328b92b4f"]
          }
        ],
        "assetId": "97c9fb03-e65d-488a-8af4-cb102c96cff1",
        "ioRef": "f5933c12-05f7-43a0-bbb2-03b1a97b3735"
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
