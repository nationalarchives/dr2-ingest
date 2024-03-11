# DR2 Ingest Asset Reconciler Lambda

A Lambda that queries Preservica for the assets that have been reported as successfully ingested.

The lambda:
* Reads the input from the step function step with this format:
```json
{
  "batchId": "batch",
  "executionId": "id",
  "assetId": "asset"
}
```
* Fetch the asset from Dynamo using the id passed as input.
* Query Preservica for an Entity that has a SourceID value that is the same as the Asset's name
* Get the Entity's ref from the returned Entity object and use it to get the urls to the Entity's representations
* Use the urls to obtain the Content Objects belonging to each representation from the API
* Get the child files from Dynamo where the child's parent path equals the asset path.
* Get the bitstreams of the Content Objects from the API
* Iterate through each child, using its checksum to find a Content Object that has the same checksum (fixity) and file name
* Generate a StateOutput object that contains information on whether all files were reconciled (Boolean) and if they weren't, the reason why
* Writes the StateOutput data for the next step function step with this format:
```json
{
  "wasReconciled": false,
  "reason": "Out of the 2 files expected to be ingested for assetId 'a8163bde-7daa-43a7-9363-644f93fe2f2b', a checksum could not be found for: b285c02d-44e3-4939-a856-66252fd7919a, 974081e5-3123-42ea-923d-3999cc160718"
}
```

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

## Environment Variables

| Name                   | Description                                                                         |
|------------------------|-------------------------------------------------------------------------------------|
| PRESERVICA_API_URL     | The Preservica API  url                                                             |
| PRESERVICA_SECRET_NAME | The secret used to call the Preservica API                                          |
| DYNAMO_TABLE_NAME      | The name of the table to read assets and their children from                        |
| DYNAMO_GSI_NAME        | The name of the global secondary index. This is used for querying fields in the GSI |