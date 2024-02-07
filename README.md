# DR2 Get Ingest Monitor

A Lambda triggered by our dr2-ingest Step Function to get an ingest monitor

## Lambda input
The lambda takes the following input:

```json
{"executionId": "step-function-execution-id"}
```

## Lambda output
The lambda outputs the Ingest status and mappedId (for the purpose of calling other endpoints) in a json object
```json
{
  "status": "Succeeded",
  "mappedId": "cce3f18b96293cbaac1fab97f7dfae2f"
}
```

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

## Environment Variables

| Name                   | Description                                |
|------------------------|--------------------------------------------|
| PRESERVICA_API_URL     | The Preservica API  url                    |
| PRESERVICA_SECRET_NAME | The secret used to call the Preservica API |