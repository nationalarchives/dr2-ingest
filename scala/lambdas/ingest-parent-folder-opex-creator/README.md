# DR2 Ingest Opex Parent Folder Creator

This lambda will list the contents of our staging cache below the `opex/<executionName>/` prefix and create a folder entry
in our .opex file (Containing manifests) for the next level down.

## Lambda input
The input is a json object with the execution id
```json
{
  "executionId": "step-function-execution-id"
}
```
There is no output from this lambda

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

## Environment Variables

| Name                 | Description                                                                                 |
|----------------------|---------------------------------------------------------------------------------------------|
| STAGING_CACHE_BUCKET | The bucket from which to get the common prefixes from and where to upload the .opex file to |

