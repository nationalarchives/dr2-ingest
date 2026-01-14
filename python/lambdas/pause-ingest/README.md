# DR2 Pause and Resume Ingest

## Input

The lambda can be triggered either directly from a call to invoke-lambda in [GitHub Actions](https://github.com/nationalarchives/dr2-runbooks/actions/workflows/pause_and_resume_ingest.yml) or can be triggered by an
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

* Disable the aggregator lambda trigger
* Disable the court document handler trigger
* Set `enabled` in the flow control config to false
* Send a message to Slack to say that the ingest is being paused

If `pause` is False

* Enable the aggregator lambda trigger.
* Enable the court document handler trigger.
* Set `enabled` in the flow control config to true
* Send a message to Slack to say that the ingest has been resumed

If `source` is `aws.events`

This event comes from the EventBridge Schedule which runs this lambda periodically. It's there to remind us if the ingest is still paused.

If any of the lambda triggers are still disabled or if `enabled` in the flow control config is still false
then send a message to Slack to say that the ingest is still paused, otherwise do nothing.

[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments)

## Environment Variables

| Name                 | Description                              |
|----------------------|------------------------------------------|
| TRIGGER_ARNS         | A json array containing the trigger arns |
| ENVIRONMENT          | The environment                          |
