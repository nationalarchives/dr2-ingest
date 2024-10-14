# DR2 Ingest - Parent Folder Opex Creator

This Lambda lists the contents of our staging cache below the `opex/<executionName>/` prefix and creates an OPEX file with a folder entry for the next level down.

## Lambda input
The input is a json object with the execution id
```json
{
  "executionId": "step-function-execution-id"
}
```

There is no output from this lambda

## Environment Variables

| Name                 | Description                                                                                 |
|----------------------|---------------------------------------------------------------------------------------------|
| STAGING_CACHE_BUCKET | The bucket from which to get the common prefixes from and where to upload the .opex file to |

