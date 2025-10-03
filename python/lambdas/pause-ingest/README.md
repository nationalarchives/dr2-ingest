# DR2 Pause Ingest

## Input

The lambda can be triggered either directly from a call to invoke-lambda in GitHub actions or can be triggered by an
Eventbridge scheduled event.

The invoke-lambda input will be either

```json
{
  "pause": true
}
```

or

```json
{
  "pause": false
}
```

depending on whether we want to pause and resume the ingest.

With the scheduled event input, we're only looking at the `source` field

```json
{
  "source": "aws.events"
}
```

## Output

The lambda doesn't return anything

## Steps

If `pause` is True

* Send a message to Slack to say that the ingest is being paused
* Disable the aggregator lambda trigger.
* Disable the court document handler trigger.
* Set `maxConcurrency` in the flow control config to 0 and store the original value in a field
  called `previousMaxConcurrency`

If `pause` is False

* Send a message to Slack to say that the ingest has been resumed
* Enabled the aggregator lambda trigger.
* Enabled the court document handler trigger.
* Set `maxConcurrency` in the flow control config to the value of `previousMaxConcurrency` and
  remove `previousMaxConcurrency` from the json.

If `source` is `aws.events`

If any of the lambda triggers are still disabled or if `maxConcurrency` in the flow control config is still 0
then send a message to Slack to say that the ingest is still paused. Otherwise do nothing. 

[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments)

## Environment Variables

| Name                 | Description                              |
|----------------------|------------------------------------------|
| TRIGGER_ARNS         | A json array containing the trigger arns |
| ENVIRONMENT          | The environment                          |
